"""
Write Parquet Activity - Writes buffered data to Parquet files.
"""
import azure.durable_functions as df
import json
import logging
import os

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError

from core.parquet_utils import ParquetWriter

logger = logging.getLogger(__name__)

bp_write_parquet = df.Blueprint()

BUFFER_CONTAINER = "buffer"


@bp_write_parquet.activity_trigger(input_name="input")
def write_parquet(input: dict) -> dict:
    """
    Write buffered records to Parquet file in ADLS.
    
    Input:
        {
            "source_id": str,
            "config": dict,
            "buffer_state": dict
        }
    
    Output:
        {
            "success": bool,
            "file_path": str | None,
            "row_count": int,
            "file_size_bytes": int,
            "error": str | None
        }
    """
    source_id = input.get("source_id")
    config = input.get("config", {})
    buffer_state = input.get("buffer_state", {})
    
    if not source_id:
        return {"success": False, "error": "source_id required"}
    
    buffer_path = buffer_state.get("buffer_file_path")
    if not buffer_path:
        return {"success": False, "error": "No buffer file path"}
    
    logger.info(f"Writing Parquet for source: {source_id}")
    
    try:
        # Read records from buffer
        records = _read_buffer(buffer_path)
        
        if not records:
            return {
                "success": False,
                "error": "Buffer is empty",
                "file_path": None,
                "row_count": 0,
                "file_size_bytes": 0
            }
        
        # Get output config
        concept = config.get("concept", "unknown")
        source = config.get("source", "unknown")
        entity = config.get("entity", "unknown")
        output_config = config.get("output", {})
        
        # Write Parquet
        writer = ParquetWriter(
            connection_string=os.getenv("PARQUET_OUTPUT_CONNECTION") or 
                            os.getenv("PARQUET_CONFIG_STORAGE_CONNECTION")
        )
        
        result = writer.write(
            records=records,
            concept=concept,
            source=source,
            entity=entity,
            compression=output_config.get("compression", "snappy"),
            row_group_size=output_config.get("row_group_size", 100000),
            inject_metadata=True  # Already injected in transform, but add file name
        )
        
        logger.info(f"Successfully wrote {result['row_count']} rows to {result['file_path']}")
        
        return {
            "success": True,
            "file_path": result["file_path"],
            "row_count": result["row_count"],
            "file_size_bytes": result["file_size_bytes"],
            "error": None
        }
    
    except Exception as e:
        error_msg = f"Failed to write Parquet: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "file_path": None,
            "row_count": 0,
            "file_size_bytes": 0,
            "error": error_msg
        }


def _read_buffer(buffer_path: str) -> list[dict]:
    """Read records from buffer blob."""
    connection = os.getenv("PARQUET_CONFIG_STORAGE_CONNECTION")
    
    if connection:
        if connection == "UseDevelopmentStorage=true":
            blob_service = BlobServiceClient.from_connection_string(connection)
        else:
            blob_service = BlobServiceClient.from_connection_string(connection)
    else:
        account = os.getenv("PARQUET_CONFIG_STORAGE_ACCOUNT")
        blob_service = BlobServiceClient(
            account_url=f"https://{account}.blob.core.windows.net",
            credential=DefaultAzureCredential()
        )
    
    try:
        container_client = blob_service.get_container_client(BUFFER_CONTAINER)
        blob_client = container_client.get_blob_client(buffer_path)
        
        content = blob_client.download_blob().readall().decode("utf-8")
        records = [json.loads(line) for line in content.strip().split("\n") if line]
        
        return records
    
    except ResourceNotFoundError:
        logger.warning(f"Buffer blob not found: {buffer_path}")
        return []
    
    except Exception as e:
        logger.error(f"Failed to read buffer {buffer_path}: {e}")
        return []
