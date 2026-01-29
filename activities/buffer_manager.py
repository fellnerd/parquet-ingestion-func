"""
Buffer Manager Activity - Manages buffer state and operations.

Buffer storage:
- Table Storage: BufferState table for metadata
- Blob Storage: _buffer/{source_id}/pending.jsonl for data
"""
import azure.durable_functions as df
import json
import logging
import os
from datetime import datetime, timezone

from azure.storage.blob import BlobServiceClient
from azure.data.tables import TableServiceClient, TableClient
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError

logger = logging.getLogger(__name__)

bp_buffer_manager = df.Blueprint()

# Table and container names
BUFFER_TABLE = "BufferState"
BUFFER_CONTAINER = "buffer"


def _get_table_client() -> TableClient:
    """Get Azure Table client for buffer state."""
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
        service.create_table(BUFFER_TABLE)
    except Exception:
        pass  # Table already exists
    
    return service.get_table_client(BUFFER_TABLE)


def _get_blob_service() -> BlobServiceClient:
    """Get Azure Blob client for buffer data."""
    connection = os.getenv("PARQUET_CONFIG_STORAGE_CONNECTION")
    
    if connection:
        if connection == "UseDevelopmentStorage=true":
            return BlobServiceClient.from_connection_string(connection)
        return BlobServiceClient.from_connection_string(connection)
    
    account = os.getenv("PARQUET_CONFIG_STORAGE_ACCOUNT")
    return BlobServiceClient(
        account_url=f"https://{account}.blob.core.windows.net",
        credential=DefaultAzureCredential()
    )


@bp_buffer_manager.activity_trigger(input_name="input")
def check_buffer(input: dict) -> dict:
    """
    Check current buffer state for a source.
    
    Input:
        {"source_id": str}
    
    Output:
        {
            "source_id": str,
            "row_count": int,
            "first_record_timestamp": str | None,
            "last_append_timestamp": str | None,
            "last_flush_timestamp": str | None,
            "buffer_file_path": str | None
        }
    """
    source_id = input.get("source_id")
    
    if not source_id:
        return {"source_id": None, "row_count": 0}
    
    logger.info(f"Checking buffer state for: {source_id}")
    
    try:
        table = _get_table_client()
        
        # Use source_id as both partition and row key
        partition_key = source_id.replace("/", "_")
        row_key = "state"
        
        try:
            entity = table.get_entity(partition_key=partition_key, row_key=row_key)
            
            return {
                "source_id": source_id,
                "row_count": entity.get("row_count", 0),
                "first_record_timestamp": entity.get("first_record_timestamp"),
                "last_append_timestamp": entity.get("last_append_timestamp"),
                "last_flush_timestamp": entity.get("last_flush_timestamp"),
                "buffer_file_path": entity.get("buffer_file_path")
            }
        
        except ResourceNotFoundError:
            # No existing state
            return {
                "source_id": source_id,
                "row_count": 0,
                "first_record_timestamp": None,
                "last_append_timestamp": None,
                "last_flush_timestamp": None,
                "buffer_file_path": None
            }
    
    except Exception as e:
        logger.error(f"Failed to check buffer for {source_id}: {e}")
        return {"source_id": source_id, "row_count": 0}


@bp_buffer_manager.activity_trigger(input_name="input")
def append_buffer(input: dict) -> dict:
    """
    Append records to buffer.
    
    Input:
        {
            "source_id": str,
            "records": list[dict],
            "current_state": dict
        }
    
    Output:
        Updated buffer state
    """
    source_id = input.get("source_id")
    records = input.get("records", [])
    current_state = input.get("current_state", {})
    
    if not source_id or not records:
        return current_state
    
    logger.info(f"Appending {len(records)} records to buffer: {source_id}")
    
    try:
        # Prepare buffer file path
        partition_key = source_id.replace("/", "_")
        buffer_path = f"_buffer/{partition_key}/pending.jsonl"
        
        # Append records as JSONL
        blob_service = _get_blob_service()
        
        # Ensure container exists
        container_client = blob_service.get_container_client(BUFFER_CONTAINER)
        try:
            container_client.create_container()
        except Exception:
            pass  # Container exists
        
        blob_client = container_client.get_blob_client(buffer_path)
        
        # Convert records to JSONL
        jsonl_content = "\n".join(json.dumps(r) for r in records) + "\n"
        
        # Append to existing blob or create new
        try:
            # Try to append
            existing = blob_client.download_blob().readall().decode("utf-8")
            new_content = existing + jsonl_content
            blob_client.upload_blob(new_content, overwrite=True)
        except ResourceNotFoundError:
            # Create new blob
            blob_client.upload_blob(jsonl_content)
        
        # Update state
        now = datetime.now(timezone.utc).isoformat()
        new_row_count = current_state.get("row_count", 0) + len(records)
        
        new_state = {
            "source_id": source_id,
            "row_count": new_row_count,
            "first_record_timestamp": current_state.get("first_record_timestamp") or now,
            "last_append_timestamp": now,
            "last_flush_timestamp": current_state.get("last_flush_timestamp"),
            "buffer_file_path": buffer_path
        }
        
        # Save state to table
        table = _get_table_client()
        entity = {
            "PartitionKey": partition_key,
            "RowKey": "state",
            **new_state
        }
        table.upsert_entity(entity)
        
        logger.info(f"Buffer now has {new_row_count} rows")
        return new_state
    
    except Exception as e:
        logger.error(f"Failed to append to buffer for {source_id}: {e}")
        return current_state


@bp_buffer_manager.activity_trigger(input_name="input")
def clear_buffer(input: dict) -> bool:
    """
    Clear buffer after successful Parquet write.
    
    Input:
        {"source_id": str}
    
    Output:
        True if successful
    """
    source_id = input.get("source_id")
    
    if not source_id:
        return False
    
    logger.info(f"Clearing buffer for: {source_id}")
    
    try:
        partition_key = source_id.replace("/", "_")
        buffer_path = f"_buffer/{partition_key}/pending.jsonl"
        
        # Delete blob
        blob_service = _get_blob_service()
        container_client = blob_service.get_container_client(BUFFER_CONTAINER)
        blob_client = container_client.get_blob_client(buffer_path)
        
        try:
            blob_client.delete_blob()
        except ResourceNotFoundError:
            pass  # Already deleted
        
        # Update state
        now = datetime.now(timezone.utc).isoformat()
        table = _get_table_client()
        
        entity = {
            "PartitionKey": partition_key,
            "RowKey": "state",
            "source_id": source_id,
            "row_count": 0,
            "first_record_timestamp": None,
            "last_append_timestamp": None,
            "last_flush_timestamp": now,
            "buffer_file_path": None
        }
        table.upsert_entity(entity)
        
        logger.info(f"Buffer cleared for {source_id}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to clear buffer for {source_id}: {e}")
        return False


@bp_buffer_manager.activity_trigger(input_name="input")
def get_buffer_records(input: dict) -> list[dict]:
    """
    Get all records from buffer (used before writing Parquet).
    
    Input:
        {"source_id": str}
    
    Output:
        List of buffered records
    """
    source_id = input.get("source_id")
    
    if not source_id:
        return []
    
    try:
        partition_key = source_id.replace("/", "_")
        buffer_path = f"_buffer/{partition_key}/pending.jsonl"
        
        blob_service = _get_blob_service()
        container_client = blob_service.get_container_client(BUFFER_CONTAINER)
        blob_client = container_client.get_blob_client(buffer_path)
        
        try:
            content = blob_client.download_blob().readall().decode("utf-8")
            records = [json.loads(line) for line in content.strip().split("\n") if line]
            logger.info(f"Retrieved {len(records)} records from buffer")
            return records
        except ResourceNotFoundError:
            return []
    
    except Exception as e:
        logger.error(f"Failed to get buffer records for {source_id}: {e}")
        return []
