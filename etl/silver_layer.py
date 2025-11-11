import pandas as pd
from deltalake import DeltaTable, write_deltalake
from pathlib import Path
import os
import pyarrow as pa


BRONZE_PATH = str(Path("../datalake/bronze/realestateapi/").resolve())
SILVER_PATH = str(Path("../datalake/silver/realestateapi/").resolve())


def get_schema():
    return pa.schema([
        pa.field("id", pa.int64()),
        pa.field("title", pa.string()),
        pa.field("description", pa.string()),
        pa.field("property_type", pa.string()),
        pa.field("location_city", pa.string()),
        pa.field("location_state", pa.string()),
        pa.field("location_country", pa.string()),
        pa.field("location_address", pa.string()),
        pa.field("location_neighborhood", pa.string()),
        pa.field("location_zip_code", pa.string()),
        pa.field("location_coordinates_latitude", pa.float64()),
        pa.field("location_coordinates_longitude", pa.float64()),
        pa.field("pricing_price", pa.float64()),
        pa.field("pricing_currency", pa.string()),
        pa.field("pricing_price_per_sqm", pa.float64()),
        pa.field("features_bedrooms", pa.int64()),
        pa.field("features_bathrooms", pa.int64()),
        pa.field("features_half_bathrooms", pa.int64()),
        pa.field("features_total_area_sqm", pa.float64()),
        pa.field("features_covered_area_sqm", pa.float64()),
        pa.field("features_uncovered_area_sqm", pa.float64()),
        pa.field("features_lot_area_sqm", pa.float64()),
        pa.field("features_construction_year", pa.int64()),
        pa.field("features_floors", pa.int64()),
        pa.field("features_floor_number", pa.int64()),
        pa.field("features_parking_spaces", pa.int64()),
        pa.field("status_property_status", pa.string()),
        pa.field("status_is_furnished", pa.bool_()),
        pa.field("status_is_new_construction", pa.bool_()),
        pa.field("status_immediate_availability", pa.bool_()),
        pa.field("agent_name", pa.string()),
        pa.field("agent_email", pa.string()),
        pa.field("agent_phone", pa.string()),
        pa.field("agent_company", pa.string()),
        pa.field("published_at", pa.timestamp('us')),
        pa.field("updated_at", pa.timestamp('us')),
        pa.field("expires_at", pa.timestamp('us')),
        pa.field("published_date", pa.date32())
    ])


def transform_bronze_to_silver(df):
    df_transformed = df.copy()

    df_transformed = df_transformed.rename(columns={
        'location.city': 'location_city',
        'location.state': 'location_state',
        'location.country': 'location_country',
        'location.address': 'location_address',
        'location.neighborhood': 'location_neighborhood',
        'location.zip_code': 'location_zip_code',
        'location.coordinates.latitude': 'location_coordinates_latitude',
        'location.coordinates.longitude': 'location_coordinates_longitude',
        'pricing.price': 'pricing_price',
        'pricing.currency': 'pricing_currency',
        'pricing.price_per_sqm': 'pricing_price_per_sqm',
        'features.bedrooms': 'features_bedrooms',
        'features.bathrooms': 'features_bathrooms',
        'features.half_bathrooms': 'features_half_bathrooms',
        'features.total_area_sqm': 'features_total_area_sqm',
        'features.covered_area_sqm': 'features_covered_area_sqm',
        'features.uncovered_area_sqm': 'features_uncovered_area_sqm',
        'features.lot_area_sqm': 'features_lot_area_sqm',
        'features.construction_year': 'features_construction_year',
        'features.floors': 'features_floors',
        'features.floor_number': 'features_floor_number',
        'features.parking_spaces': 'features_parking_spaces',
        'status.property_status': 'status_property_status',
        'status.is_furnished': 'status_is_furnished',
        'status.is_new_construction': 'status_is_new_construction',
        'status.immediate_availability': 'status_immediate_availability',
        'agent.name': 'agent_name',
        'agent.email': 'agent_email',
        'agent.phone': 'agent_phone',
        'agent.company': 'agent_company',
        'dates.published_at': 'published_at',
        'dates.updated_at': 'updated_at',
        'dates.expires_at': 'expires_at'
    })

    df_transformed['published_at'] = pd.to_datetime(df_transformed['published_at'], format='mixed')
    df_transformed['updated_at'] = pd.to_datetime(df_transformed['updated_at'], format='mixed')
    df_transformed['expires_at'] = pd.to_datetime(df_transformed['expires_at'], format='mixed')

    return df_transformed


def load_to_silver():
    print("Reading data from bronze layer")

    try:
        dt_bronze = DeltaTable(BRONZE_PATH)
    except Exception as e:
        print(f"Error reading bronze layer: {e}")
        return

    Path(SILVER_PATH).parent.mkdir(parents=True, exist_ok=True)

    try:
        dt_silver = DeltaTable(SILVER_PATH)
        silver_exists = True
    except Exception:
        silver_exists = False

    if silver_exists:
        df_silver_existing = dt_silver.to_pandas()
        if not df_silver_existing.empty:
            max_published_at = df_silver_existing['published_at'].max()
            print(f"Silver layer exists. Max published_at: {max_published_at}")

            df_bronze = dt_bronze.to_pandas()
            df_bronze['dates.published_at'] = pd.to_datetime(df_bronze['dates.published_at'], format='mixed')
            df_bronze = df_bronze[df_bronze['dates.published_at'] > max_published_at]

            if df_bronze.empty:
                print("No new records to process")
                return

            print(f"Found {len(df_bronze)} new records from bronze")
        else:
            df_bronze = dt_bronze.to_pandas()
            print(f"Loaded {len(df_bronze)} records from bronze")
    else:
        df_bronze = dt_bronze.to_pandas()
        print(f"Loaded {len(df_bronze)} records from bronze")

    if df_bronze.empty:
        print("No data in bronze layer")
        return

    df_silver = transform_bronze_to_silver(df_bronze)

    if not silver_exists:
        print("Creating silver table")
        write_deltalake(
            SILVER_PATH,
            df_silver,
            mode="overwrite",
            schema=get_schema(),
            partition_by=["published_date"]
        )
        print(f"Successfully created silver table with {len(df_silver)} records")
    else:
        print("Merging data into silver table")

        dt_silver = DeltaTable(SILVER_PATH)

        source_table = pa.Table.from_pandas(df_silver, schema=get_schema())

        (
            dt_silver.merge(
                source=source_table,
                predicate="target.id = source.id AND target.published_at = source.published_at",
                source_alias="source",
                target_alias="target"
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

        print(f"Successfully merged {len(df_silver)} records into silver table")


if __name__ == "__main__":
    load_to_silver()
