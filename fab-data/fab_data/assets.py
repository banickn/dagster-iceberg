import json
import os
from datetime import datetime, timezone
from typing import List

from dagster import (
    asset, FreshnessPolicy, MetadataValue, Output,
    AssetExecutionContext
)
from azure.storage.blob import BlobServiceClient
import pandas as pd
import pyarrow as pa
from .resources import IcebergCatalogResource
from .schemas import get_pyarrow_schema, ICEBERG_SCHEMA
from pathlib import Path


class AzureConfig:
    def __init__(self):
        self.storage_options = {
            "connection_string": os.getenv("AZURE_CONNECTION_STRING"),
            "account_name": os.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
            "account_key": os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        }
        self.bronze_container = os.getenv("AZURE_BRONZE_CONTAINER_NAME")
        self.silver_container = os.getenv("AZURE_SILVER_CONTAINER_NAME")

    @property
    def warehouse_path(self):
        return f"abfs://{self.silver_container}@{self.storage_options['account_name']}.dfs.core.windows.net/"


@asset(
    description="Setup Iceberg catalog and create necessary tables",
    group_name="infrastructure",
    compute_kind="python"
)
def setup_iceberg_tables(
    context: AssetExecutionContext,
    iceberg_catalog: IcebergCatalogResource
):
    """
    Initialize Iceberg catalog and create required tables.
    This asset must run before any table operations.
    """
    # Get the catalog from the resource
    catalog = iceberg_catalog.get_catalog()

    try:
        # Create namespace if it doesn't exist
        if "silver" not in [ns[0] for ns in catalog.list_namespaces()]:
            catalog.create_namespace("silver")
            context.log.info("Created 'silver' namespace")

        # Create table if it doesn't exist
        table_identifier = ("silver", "fab_data")
        if not catalog.table_exists(table_identifier):
            catalog.create_table(
                identifier=table_identifier,
                schema=ICEBERG_SCHEMA,
                properties={
                    "write.format.default": "parquet",
                    "write.metadata.compression-codec": "gzip",
                    "write.metadata.metrics.default": "full",
                    "write.metadata.metrics.column.counts": "true",
                    "format-version": "2"
                }
            )
            context.log.info("Created 'silver.fab_data' table")

    except Exception as e:
        context.log.error(f"Failed to setup Iceberg table: {str(e)}")
        raise


@asset(
    description="Processes incoming JSON files and loads them into the bronze data lake layer",
    freshness_policy=FreshnessPolicy(
        maximum_lag_minutes=60,
        cron_schedule="0 * * * *"
    ),
    group_name="ingestion",
    compute_kind="python"
)
def ingest_raw_fab_data(context: AssetExecutionContext) -> Output[List[str]]:
    """
    Ingest raw fab data from source to bronze layer.
    Returns list of processed file paths for downstream processing.
    """
    config = AzureConfig()
    blob_service_client = BlobServiceClient.from_connection_string(
        config.storage_options["connection_string"]
    )

    # # Get list of processed files from metadata (for idempotency)
    # processed_files = context.instance.get_latest_materialization_events(
    #     asset_key=context.asset_key
    # )
    # processed_filenames = set(
    #     event.materialization.metadata.get("processed_files", [])
    #     for event in processed_files
    # )

    processed_in_this_run = []
    failed_files = []
    filename = "semi_data-2024-11-22T09:28:27.685750.json"
    file_path = Path("../raw_data") / filename
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    with open(file_path) as f:
        data = json.load(f)
        blob_service_client = BlobServiceClient.from_connection_string(config.storage_options["connection_string"])
        blob_client = blob_service_client.get_blob_client(
            container=config.bronze_container, blob=filename)

        # Serialize JSON data to a string
        json_content = json.dumps(data)

        # Upload the JSON content to the blob
        try:
            blob_client.upload_blob(json_content, overwrite=True)
            processed_in_this_run.append(filename)
        except Exception as e:
            print(f"Failed to process {filename}: {str(e)}")
            failed_files.append(filename)
        print(MetadataValue.json(processed_in_this_run))
        return Output(
            processed_in_this_run,
            metadata={
                "processed_file_count": len(processed_in_this_run),
                "failed_file_count": len(failed_files),
                "processed_files": MetadataValue.json(processed_in_this_run),
                "failed_files": MetadataValue.json(failed_files)
            }
        )


@asset(
    description="Transform bronze data to Iceberg format and ingest into silver layer",
    freshness_policy=FreshnessPolicy(
        maximum_lag_minutes=60,
        cron_schedule="0 * * * *"
    ),
    group_name="ingestion",
    compute_kind="python",
    # required_resource_keys={"iceberg_catalog"}  # Explicitly declare resource dependency
)
def write_iceberg_table(
    iceberg_catalog: IcebergCatalogResource,
    ingest_raw_fab_data: List[str]
) -> Output[None]:
    """
    Transform and load data from bronze to silver layer using Iceberg format.

    Args:
        context: The asset execution context
        iceberg_catalog: The Iceberg catalog resource
        ingest_raw_fab_data: List of processed file paths from the ingestion step
    """
    # Get the catalog from the resource
    catalog = iceberg_catalog.get_catalog()
    config = AzureConfig()
    table = catalog.load_table("silver.fab_data")

    # Initialize Azure client for reading from bronze
    blob_service_client = BlobServiceClient.from_connection_string(
        config.storage_options["connection_string"]
    )
    container_client = blob_service_client.get_container_client(config.bronze_container)

    # Process files
    processed_count = 0
    failed_files = []
    print(f"ingest data: {ingest_raw_fab_data}")
    for blob_name in ingest_raw_fab_data:
        try:
            # Check if file was already processed (idempotency check)
            if table.scan().filter(
                f"source_file = '{blob_name}'"
            ).to_arrow().num_rows > 0:
                print(f"File {blob_name} already processed, skipping")
                continue

            # Process the file
            blob_client = container_client.get_blob_client(blob_name)
            json_data = blob_client.download_blob().readall()

            # Transform data
            data = json.loads(json_data)
            df = pd.DataFrame(data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            # Add processing metadata
            df['source_file'] = blob_name
            df['processed_at'] = datetime.now(timezone.utc)

            # Convert to PyArrow and write
            table_arrow = pa.Table.from_pandas(
                df,
                schema=get_pyarrow_schema()
            )

            table.append(table_arrow)
            processed_count += 1

            print(f"Successfully processed {blob_name}")
        except Exception as e:
            print(f"Error processing {blob_name}: {str(e)}")
            failed_files.append(blob_name)

    # Log processing results
    print(
        f"Processing complete. Processed: {processed_count}, Failed: {len(failed_files)}"
    )
    # Return output with metadata
    return Output(
        None,
        metadata={
            "processed_file_count": processed_count,
            "failed_file_count": len(failed_files),
            "failed_files": MetadataValue.json(failed_files)
        }
    )