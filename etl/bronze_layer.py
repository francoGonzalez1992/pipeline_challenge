import requests
import pandas as pd
from deltalake import DeltaTable, write_deltalake
from datetime import datetime, timedelta
import os
import json
from pathlib import Path


API_BASE_URL = "http://localhost:8000"
BRONZE_PATH = str(Path("../datalake/bronze/realestateapi/").resolve())


def extract_properties_from_api(from_date: str, to_date: str):
    url = f"{API_BASE_URL}/houses/{from_date}/{to_date}"

    response = requests.get(url)
    response.raise_for_status()

    data = response.json()
    return data.get('properties', [])


def get_max_published_date_from_bronze():
    try:
        dt = DeltaTable(BRONZE_PATH)
        df = dt.to_pandas()

        if df.empty:
            return None

        max_date_str = df['dates.published_at'].max()
        max_date = datetime.fromisoformat(max_date_str) + timedelta(seconds=1)
        return max_date

    except Exception:
        return None


def flatten_property_data(properties):
    flattened_data = []

    for prop in properties:
        flat_prop = {
            'id': prop.get('id'),
            'title': prop.get('title'),
            'description': prop.get('description'),
            'property_type': prop.get('property_type'),
            'location.city': prop.get('location', {}).get('city'),
            'location.state': prop.get('location', {}).get('state'),
            'location.country': prop.get('location', {}).get('country'),
            'location.address': prop.get('location', {}).get('address'),
            'location.neighborhood': prop.get('location', {}).get('neighborhood'),
            'location.zip_code': prop.get('location', {}).get('zip_code'),
            'location.coordinates.latitude': prop.get('location', {}).get('coordinates', {}).get('latitude'),
            'location.coordinates.longitude': prop.get('location', {}).get('coordinates', {}).get('longitude'),
            'pricing.price': prop.get('pricing', {}).get('price'),
            'pricing.currency': prop.get('pricing', {}).get('currency'),
            'pricing.price_per_sqm': prop.get('pricing', {}).get('price_per_sqm'),
            'features.bedrooms': prop.get('features', {}).get('bedrooms'),
            'features.bathrooms': prop.get('features', {}).get('bathrooms'),
            'features.half_bathrooms': prop.get('features', {}).get('half_bathrooms'),
            'features.total_area_sqm': prop.get('features', {}).get('total_area_sqm'),
            'features.covered_area_sqm': prop.get('features', {}).get('covered_area_sqm'),
            'features.uncovered_area_sqm': prop.get('features', {}).get('uncovered_area_sqm'),
            'features.lot_area_sqm': prop.get('features', {}).get('lot_area_sqm'),
            'features.construction_year': prop.get('features', {}).get('construction_year'),
            'features.floors': prop.get('features', {}).get('floors'),
            'features.floor_number': prop.get('features', {}).get('floor_number'),
            'features.parking_spaces': prop.get('features', {}).get('parking_spaces'),
            'status.property_status': prop.get('status', {}).get('property_status'),
            'status.is_furnished': prop.get('status', {}).get('is_furnished'),
            'status.is_new_construction': prop.get('status', {}).get('is_new_construction'),
            'status.immediate_availability': prop.get('status', {}).get('immediate_availability'),
            'agent.name': prop.get('agent', {}).get('name') if prop.get('agent') else None,
            'agent.email': prop.get('agent', {}).get('email') if prop.get('agent') else None,
            'agent.phone': prop.get('agent', {}).get('phone') if prop.get('agent') else None,
            'agent.company': prop.get('agent', {}).get('company') if prop.get('agent') else None,
            'dates.published_at': prop.get('dates', {}).get('published_at'),
            'dates.updated_at': prop.get('dates', {}).get('updated_at'),
            'dates.expires_at': prop.get('dates', {}).get('expires_at')
        }
        flattened_data.append(flat_prop)

    return flattened_data


def create_partition_column(df):
    df['published_date'] = pd.to_datetime(df['dates.published_at'], format='mixed').dt.date
    return df


def ensure_schema_consistency(df):
    numeric_columns = [
        'features.floor_number',
        'location.coordinates.latitude',
        'location.coordinates.longitude',
        'pricing.price',
        'pricing.price_per_sqm',
        'features.total_area_sqm',
        'features.covered_area_sqm',
        'features.uncovered_area_sqm',
        'features.lot_area_sqm'
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def load_to_bronze():
    Path(BRONZE_PATH).parent.mkdir(parents=True, exist_ok=True)

    bronze_exists = os.path.exists(BRONZE_PATH) and os.path.isdir(BRONZE_PATH)

    try:
        dt = DeltaTable(BRONZE_PATH)
        bronze_exists = True
    except Exception:
        bronze_exists = False

    if bronze_exists:
        max_date = get_max_published_date_from_bronze()

        if max_date:
            from_date = (max_date).strftime("%Y-%m-%dT%H:%M:%S")
        else:
            from_date = "1990-01-01"
    else:
        from_date = "1990-01-01"

    to_date = (datetime.now().replace(hour=23, minute=59, second=59) ).strftime("%Y-%m-%dT%H:%M:%S")

    print(f"Extracting properties from {from_date} to {to_date}")

    properties = extract_properties_from_api(from_date, to_date)

    if not properties:
        print("No new properties to load")
        return False

    print(f"Extracted {len(properties)} properties")

    flattened_properties = flatten_property_data(properties)
    df = pd.DataFrame(flattened_properties)

    df = ensure_schema_consistency(df)
    df = create_partition_column(df)

    write_deltalake(
        BRONZE_PATH,
        df,
        mode="append",
        partition_by=["published_date"]
    )

    print(f"Successfully loaded {len(df)} properties to bronze layer")
    return True

if __name__ == "__main__":
    load_to_bronze()
