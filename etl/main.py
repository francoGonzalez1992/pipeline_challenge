from bronze_layer import load_to_bronze
from silver_layer import load_to_silver


def run_etl_pipeline():
    print("=" * 50)
    print("Starting ETL Pipeline")
    print("=" * 50)

    print("\n[1/2] Running Bronze Layer")
    print("-" * 50)
    try:
        has_new_data = load_to_bronze()
        print("Bronze layer completed successfully")
    except Exception as e:
        print(f"Error in bronze layer: {e}")
        return

    if not has_new_data:
        print("\nSkipping Silver Layer - No new data detected in Bronze")
        print("=" * 50)
        return

    print("\n[2/2] Running Silver Layer")
    print("-" * 50)
    try:
        load_to_silver()
        print("Silver layer completed successfully")
    except Exception as e:
        print(f"Error in silver layer: {e}")
        return

    print("\n" + "=" * 50)
    print("ETL Pipeline completed successfully")
    print("=" * 50)


if __name__ == "__main__":
    run_etl_pipeline()
