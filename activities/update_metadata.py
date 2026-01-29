"""
Update Metadata Activity - Updates ingestion metadata after processing.
"""
import azure.durable_functions as df
import logging
import os
from datetime import datetime, timezone

from azure.data.tables import TableServiceClient, TableClient
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError

logger = logging.getLogger(__name__)

bp_update_metadata = df.Blueprint()

METADATA_TABLE = "IngestionMetadata"


def _get_table_client() -> TableClient:
    """Get Azure Table client for metadata."""
    connection = os.getenv("PARQUET_CONFIG_STORAGE_CONNECTION")
    
    if connection:
        if connection == "UseDevelopmentStorage=true":
            service = TableServiceClient.from_connection_string(connection)
        else:
            service = TableServiceClient.from_connection_string(connection)
    else:
        account = os.getenv("PARQUET_CONFIG_STORAGE_ACCOUNT")
        service = TableServiceClient(
            endpoint=f"https://{account}.table.core.windows.net",
            credential=DefaultAzureCredential()
        )
    
    # Ensure table exists
    try:
        service.create_table(METADATA_TABLE)
    except Exception:
        pass  # Table already exists
    
    return service.get_table_client(METADATA_TABLE)


@bp_update_metadata.activity_trigger(input_name="input")
def update_metadata(input: dict) -> bool:
    """
    Update ingestion metadata after processing.
    
    Input:
        {
            "source_id": str,
            "last_fetch": str,           # ISO timestamp
            "records_fetched": int,
            "parquet_written": str | None
        }
    
    Output:
        True if successful
    """
    source_id = input.get("source_id")
    last_fetch = input.get("last_fetch")
    records_fetched = input.get("records_fetched", 0)
    parquet_written = input.get("parquet_written")
    
    if not source_id:
        return False
    
    logger.info(f"Updating metadata for: {source_id}")
    
    try:
        table = _get_table_client()
        partition_key = source_id.replace("/", "_")
        row_key = "metadata"
        
        # Get existing metadata
        try:
            existing = table.get_entity(partition_key=partition_key, row_key=row_key)
            total_records = existing.get("total_records_fetched", 0) + records_fetched
            total_parquets = existing.get("total_parquets_written", 0)
            if parquet_written:
                total_parquets += 1
        except ResourceNotFoundError:
            total_records = records_fetched
            total_parquets = 1 if parquet_written else 0
        
        # Update metadata
        entity = {
            "PartitionKey": partition_key,
            "RowKey": row_key,
            "source_id": source_id,
            "last_fetch_timestamp": last_fetch,
            "last_records_fetched": records_fetched,
            "last_parquet_written": parquet_written,
            "last_update": datetime.now(timezone.utc).isoformat(),
            "total_records_fetched": total_records,
            "total_parquets_written": total_parquets
        }
        
        table.upsert_entity(entity)
        
        logger.info(f"Metadata updated for {source_id}: {records_fetched} records, parquet: {parquet_written}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to update metadata for {source_id}: {e}")
        return False


@bp_update_metadata.activity_trigger(input_name="input")
def get_metadata(input: dict) -> dict | None:
    """
    Get ingestion metadata for a source.
    
    Input:
        {"source_id": str}
    
    Output:
        Metadata dictionary or None
    """
    source_id = input.get("source_id")
    
    if not source_id:
        return None
    
    try:
        table = _get_table_client()
        partition_key = source_id.replace("/", "_")
        row_key = "metadata"
        
        try:
            entity = table.get_entity(partition_key=partition_key, row_key=row_key)
            return {
                "source_id": entity.get("source_id"),
                "last_fetch_timestamp": entity.get("last_fetch_timestamp"),
                "last_records_fetched": entity.get("last_records_fetched"),
                "last_parquet_written": entity.get("last_parquet_written"),
                "last_update": entity.get("last_update"),
                "total_records_fetched": entity.get("total_records_fetched"),
                "total_parquets_written": entity.get("total_parquets_written")
            }
        except ResourceNotFoundError:
            return None
    
    except Exception as e:
        logger.error(f"Failed to get metadata for {source_id}: {e}")
        return None
