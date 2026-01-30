"""
Transform Data Activity - Applies schema mapping and metadata injection.
"""
import azure.durable_functions as df
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

from core.jsonpath_parser import apply_mapping, flatten_record

logger = logging.getLogger(__name__)

bp_transform_data = df.Blueprint()


@bp_transform_data.activity_trigger(input_name="input")
def transform_data(input: dict) -> list[dict]:
    """
    Transform records according to schema mapping.
    
    Input:
        {
            "records": list[dict],     # Raw records to transform
            "config": dict             # Source configuration
        }
    
    Output:
        List of transformed record dictionaries
    
    Transformation:
        1. Apply field mappings from schema
        2. Flatten nested structures (optional)
        3. Inject metadata columns (dss_*)
    """
    records = input.get("records", [])
    config = input.get("config", {})
    
    logger.info(f"transform_data called with {len(records) if records else 0} records")
    
    if not records:
        logger.warning("transform_data: no records provided")
        return []
    
    logger.info(f"Transforming {len(records)} records")
    
    # Get schema reference
    schema_ref = config.get("response", {}).get("schema_ref")
    schema = None
    
    if schema_ref:
        schema = _load_schema(schema_ref)
    
    # Get source info for metadata
    concept = config.get("concept", "unknown")
    source = config.get("source", "unknown")
    
    transformed = []
    errors = 0
    
    for record in records:
        try:
            # Apply schema mapping if available
            if schema and "source_mapping" in schema:
                mappings = schema["source_mapping"].get("mappings", [])
                result = apply_mapping(record, mappings)
            else:
                # No schema: flatten nested structures
                result = flatten_record(record)
            
            # Inject metadata columns
            result["dss_load_date"] = datetime.now(timezone.utc).isoformat()
            result["dss_record_source"] = f"{concept}/{source}"
            
            transformed.append(result)
        
        except Exception as e:
            errors += 1
            if errors <= 5:
                logger.warning(f"Failed to transform record: {e}")
    
    if errors > 0:
        logger.warning(f"Failed to transform {errors} out of {len(records)} records")
    
    logger.info(f"Successfully transformed {len(transformed)} records")
    return transformed


def _load_schema(schema_ref: str) -> dict | None:
    """
    Load schema from storage or local path.
    
    Args:
        schema_ref: Path to schema file (e.g., "schemas/jira-issue.json")
    
    Returns:
        Schema dictionary or None
    """
    try:
        # Try to load from blob storage
        storage_connection = os.getenv("PARQUET_CONFIG_STORAGE_CONNECTION")
        config_container = os.getenv("PARQUET_CONFIG_CONTAINER", "config")
        
        if storage_connection:
            if storage_connection == "UseDevelopmentStorage=true":
                blob_service = BlobServiceClient.from_connection_string(storage_connection)
            else:
                blob_service = BlobServiceClient.from_connection_string(storage_connection)
            
            blob_client = blob_service.get_blob_client(
                container=config_container,
                blob=schema_ref
            )
            
            try:
                content = blob_client.download_blob().readall().decode("utf-8")
                return json.loads(content)
            except Exception as e:
                logger.debug(f"Schema not found in storage: {e}")
        
        # Try local file (for development)
        local_path = os.path.join(os.path.dirname(__file__), "..", "config", schema_ref)
        if os.path.exists(local_path):
            with open(local_path, "r") as f:
                return json.load(f)
        
        logger.warning(f"Schema {schema_ref} not found")
        return None
    
    except Exception as e:
        logger.error(f"Failed to load schema {schema_ref}: {e}")
        return None
