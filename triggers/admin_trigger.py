"""
Admin Trigger - Administrative endpoints for management.
"""
import azure.durable_functions as df
import azure.functions as func
import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import unquote

from core.config_loader import ConfigLoader

logger = logging.getLogger(__name__)

bp_admin_trigger = df.Blueprint()


@bp_admin_trigger.route(route="mgmt/sources", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
async def list_sources(req: func.HttpRequest) -> func.HttpResponse:
    """
    List all configured sources.
    
    GET /api/admin/sources
    
    Returns:
        List of source configurations (sensitive data redacted)
    """
    logger.info("Listing all sources")
    
    try:
        loader = ConfigLoader()
        source_def = loader.load_all_sources()
        
        result = []
        for s in source_def.sources:
            result.append({
                "source_id": s.source_id,
                "concept": s.concept,
                "source": s.source,
                "entity": s.entity,
                "fetch_type": s.fetch.type,
                "fetch_endpoint": s.fetch.endpoint[:50] + "..." if s.fetch.endpoint and len(s.fetch.endpoint) > 50 else s.fetch.endpoint,
                "schedule_enabled": s.schedule.enabled if s.schedule else False,
                "buffer_max_rows": s.buffer.max_rows if s.buffer else None,
                "buffer_max_age_minutes": s.buffer.max_age_minutes if s.buffer else None
            })
        
        return func.HttpResponse(
            json.dumps({"sources": result, "count": len(result)}),
            mimetype="application/json"
        )
    
    except Exception as e:
        logger.error(f"Failed to list sources: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )


@bp_admin_trigger.route(route="mgmt/sources/{source_id}", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
async def get_source(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get detailed configuration for a specific source.
    
    GET /api/admin/sources/{source_id}
    """
    source_id = req.route_params.get("source_id")
    
    if not source_id:
        return func.HttpResponse(
            json.dumps({"error": "source_id is required"}),
            mimetype="application/json",
            status_code=400
        )
    
    # URL-decode the source_id (e.g., werkportal%2Fapi%2Finvoice -> werkportal/api/invoice)
    # Note: New format uses double underscores (werkportal__api__invoice) to avoid URL encoding issues
    source_id = unquote(source_id)
    
    try:
        loader = ConfigLoader()
        source = loader.get_source(source_id)
        
        if not source:
            return func.HttpResponse(
                json.dumps({"error": f"Source {source_id} not found"}),
                mimetype="application/json",
                status_code=404
            )
        
        # Redact sensitive information
        config_dict = source.model_dump()
        if config_dict.get("fetch", {}).get("auth"):
            auth = config_dict["fetch"]["auth"]
            if auth.get("password"):
                auth["password"] = "***"
            if auth.get("token"):
                auth["token"] = auth["token"][:10] + "***" if len(auth.get("token", "")) > 10 else "***"
            if auth.get("api_key"):
                auth["api_key"] = auth["api_key"][:10] + "***" if len(auth.get("api_key", "")) > 10 else "***"
        
        return func.HttpResponse(
            json.dumps({"source": config_dict}),
            mimetype="application/json"
        )
    
    except Exception as e:
        logger.error(f"Failed to get source {source_id}: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )


@bp_admin_trigger.route(route="mgmt/sources/{source_id}", methods=["PUT", "PATCH"], auth_level=func.AuthLevel.FUNCTION)
async def update_source(req: func.HttpRequest) -> func.HttpResponse:
    """
    Update configuration for a specific source.
    
    PUT/PATCH /api/mgmt/sources/{source_id}
    
    Body (partial update supported):
    {
        "buffer": {
            "min_rows": 50,
            "max_rows": 100,
            "max_age_minutes": 30
        },
        "schedule": {
            "enabled": true,
            "cron": "0 */10 * * * *"
        },
        "output": {
            "compression": "snappy"
        }
    }
    """
    from azure.storage.blob import BlobServiceClient
    
    source_id = req.route_params.get("source_id")
    
    if not source_id:
        return func.HttpResponse(
            json.dumps({"error": "source_id is required"}),
            mimetype="application/json",
            status_code=400
        )
    
    # URL-decode the source_id
    source_id = unquote(source_id)
    
    # Parse request body
    try:
        body = req.get_body()
        if not body:
            return func.HttpResponse(
                json.dumps({"error": "Request body is required"}),
                mimetype="application/json",
                status_code=400
            )
        updates = json.loads(body.decode("utf-8"))
    except (json.JSONDecodeError, ValueError) as e:
        return func.HttpResponse(
            json.dumps({"error": f"Invalid JSON: {str(e)}"}),
            mimetype="application/json",
            status_code=400
        )
    
    # Validate allowed update fields (don't allow updating sensitive fields via API)
    allowed_fields = {"buffer", "schedule", "output", "description", "enabled"}
    invalid_fields = set(updates.keys()) - allowed_fields
    if invalid_fields:
        return func.HttpResponse(
            json.dumps({
                "error": f"Cannot update fields: {', '.join(invalid_fields)}",
                "allowed_fields": list(allowed_fields)
            }),
            mimetype="application/json",
            status_code=400
        )
    
    try:
        # Load current config from blob storage
        connection = os.getenv("PARQUET_CONFIG_STORAGE_CONNECTION")
        config_container = os.getenv("PARQUET_CONFIG_CONTAINER", "config")
        config_blob = "sources.json"
        
        if connection == "UseDevelopmentStorage=true":
            blob_service = BlobServiceClient.from_connection_string(connection)
        else:
            blob_service = BlobServiceClient.from_connection_string(connection)
        
        blob_client = blob_service.get_blob_client(container=config_container, blob=config_blob)
        
        # Download current config
        download = blob_client.download_blob()
        config_content = json.loads(download.readall().decode("utf-8"))
        
        # Find and update the source
        source_found = False
        for source in config_content.get("sources", []):
            if source.get("id") == source_id:
                source_found = True
                
                # Apply updates (deep merge for nested objects)
                for key, value in updates.items():
                    if key in source and isinstance(source[key], dict) and isinstance(value, dict):
                        # Merge nested dict
                        source[key].update(value)
                    else:
                        # Replace value
                        source[key] = value
                
                break
        
        if not source_found:
            return func.HttpResponse(
                json.dumps({"error": f"Source {source_id} not found"}),
                mimetype="application/json",
                status_code=404
            )
        
        # Upload updated config
        blob_client.upload_blob(
            json.dumps(config_content, indent=2),
            overwrite=True
        )
        
        # Invalidate cache
        loader = ConfigLoader()
        loader.invalidate_cache()
        
        # Reload and return updated source
        updated_source = loader.get_source(source_id, force_reload=True)
        
        if updated_source:
            config_dict = updated_source.model_dump()
            # Redact sensitive info
            if config_dict.get("fetch", {}).get("auth"):
                auth = config_dict["fetch"]["auth"]
                if auth.get("password"):
                    auth["password"] = "***"
                if auth.get("token"):
                    auth["token"] = "***"
                if auth.get("api_key"):
                    auth["api_key"] = "***"
        
            return func.HttpResponse(
                json.dumps({
                    "message": f"Source {source_id} updated successfully",
                    "source": config_dict
                }),
                mimetype="application/json"
            )
        else:
            return func.HttpResponse(
                json.dumps({"message": f"Source {source_id} updated, but could not reload"}),
                mimetype="application/json"
            )
    
    except Exception as e:
        logger.error(f"Failed to update source {source_id}: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )


@bp_admin_trigger.route(route="mgmt/reload-config", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
async def reload_config(req: func.HttpRequest) -> func.HttpResponse:
    """
    Force reload configuration from storage.
    
    POST /api/admin/reload-config
    """
    logger.info("Reloading configuration")
    
    try:
        loader = ConfigLoader()
        sources = loader.get_all_sources(force_reload=True)
        
        return func.HttpResponse(
            json.dumps({
                "message": "Configuration reloaded",
                "sources_count": len(sources),
                "reload_time": datetime.now(timezone.utc).isoformat()
            }),
            mimetype="application/json"
        )
    
    except Exception as e:
        logger.error(f"Failed to reload config: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )


@bp_admin_trigger.route(route="mgmt/health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
async def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """
    Health check endpoint.
    
    GET /api/admin/health
    """
    return func.HttpResponse(
        json.dumps({
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "version": os.getenv("PARQUET_FUNC_VERSION", "1.0.0")
        }),
        mimetype="application/json"
    )


@bp_admin_trigger.route(route="mgmt/buffer/{source_id}", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
async def get_buffer_status(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get buffer status for a source.
    
    GET /api/admin/buffer/{source_id}
    """
    from azure.data.tables import TableServiceClient
    from azure.core.exceptions import ResourceNotFoundError
    
    source_id = req.route_params.get("source_id")
    
    if not source_id:
        return func.HttpResponse(
            json.dumps({"error": "source_id is required"}),
            mimetype="application/json",
            status_code=400
        )
    
    # URL-decode the source_id
    source_id = unquote(source_id)
    
    try:
        connection = os.getenv("PARQUET_CONFIG_STORAGE_CONNECTION")
        service = TableServiceClient.from_connection_string(connection)
        table = service.get_table_client("BufferState")
        
        partition_key = source_id.replace("/", "_")
        
        try:
            entity = table.get_entity(partition_key=partition_key, row_key="state")
            return func.HttpResponse(
                json.dumps({
                    "source_id": source_id,
                    "row_count": entity.get("row_count", 0),
                    "first_record_timestamp": entity.get("first_record_timestamp"),
                    "last_append_timestamp": entity.get("last_append_timestamp"),
                    "last_flush_timestamp": entity.get("last_flush_timestamp"),
                    "buffer_file_path": entity.get("buffer_file_path")
                }),
                mimetype="application/json"
            )
        
        except ResourceNotFoundError:
            return func.HttpResponse(
                json.dumps({
                    "source_id": source_id,
                    "row_count": 0,
                    "message": "No buffer state found"
                }),
                mimetype="application/json"
            )
    
    except Exception as e:
        logger.error(f"Failed to get buffer status for {source_id}: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )


@bp_admin_trigger.route(route="mgmt/metadata/{source_id}", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
async def get_ingest_metadata(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get ingestion metadata for a source.
    
    GET /api/admin/metadata/{source_id}
    """
    from azure.data.tables import TableServiceClient
    from azure.core.exceptions import ResourceNotFoundError
    
    source_id = req.route_params.get("source_id")
    
    if not source_id:
        return func.HttpResponse(
            json.dumps({"error": "source_id is required"}),
            mimetype="application/json",
            status_code=400
        )
    
    # URL-decode the source_id
    source_id = unquote(source_id)
    
    try:
        connection = os.getenv("PARQUET_CONFIG_STORAGE_CONNECTION")
        service = TableServiceClient.from_connection_string(connection)
        table = service.get_table_client("IngestionMetadata")
        
        partition_key = source_id.replace("/", "_")
        
        try:
            entity = table.get_entity(partition_key=partition_key, row_key="metadata")
            return func.HttpResponse(
                json.dumps({
                    "source_id": source_id,
                    "last_fetch_timestamp": entity.get("last_fetch_timestamp"),
                    "last_records_fetched": entity.get("last_records_fetched"),
                    "last_parquet_written": entity.get("last_parquet_written"),
                    "last_update": entity.get("last_update"),
                    "total_records_fetched": entity.get("total_records_fetched"),
                    "total_parquets_written": entity.get("total_parquets_written")
                }),
                mimetype="application/json"
            )
        
        except ResourceNotFoundError:
            return func.HttpResponse(
                json.dumps({
                    "source_id": source_id,
                    "message": "No ingestion metadata found"
                }),
                mimetype="application/json"
            )
    
    except Exception as e:
        logger.error(f"Failed to get metadata for {source_id}: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )


@bp_admin_trigger.route(route="mgmt/flush-status/{source_id}", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
async def get_flush_status(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get flush prediction for a source - when will the next flush happen?
    
    GET /api/mgmt/flush-status/{source_id}
    
    Returns information about:
    - Current buffer state (rows, age)
    - Configured thresholds (max_rows, max_age_minutes, min_rows)
    - Predicted flush triggers
    """
    from azure.data.tables import TableServiceClient
    from azure.core.exceptions import ResourceNotFoundError
    from datetime import timedelta
    
    source_id = req.route_params.get("source_id")
    
    if not source_id:
        return func.HttpResponse(
            json.dumps({"error": "source_id is required"}),
            mimetype="application/json",
            status_code=400
        )
    
    # URL-decode the source_id
    source_id = unquote(source_id)
    
    try:
        # Get source config for buffer settings
        loader = ConfigLoader()
        source = loader.get_source(source_id)
        
        if not source:
            return func.HttpResponse(
                json.dumps({"error": f"Source {source_id} not found"}),
                mimetype="application/json",
                status_code=404
            )
        
        # Buffer configuration
        buffer_config = source.buffer
        max_rows = buffer_config.max_rows if buffer_config else 100
        min_rows = buffer_config.min_rows if buffer_config else 10
        max_age_minutes = buffer_config.max_age_minutes if buffer_config else 60
        
        # Get current buffer state
        connection = os.getenv("PARQUET_CONFIG_STORAGE_CONNECTION")
        service = TableServiceClient.from_connection_string(connection)
        table = service.get_table_client("BufferState")
        
        partition_key = source_id.replace("/", "_")
        
        now = datetime.now(timezone.utc)
        
        try:
            entity = table.get_entity(partition_key=partition_key, row_key="state")
            
            row_count = entity.get("row_count", 0)
            first_record_timestamp = entity.get("first_record_timestamp")
            last_append_timestamp = entity.get("last_append_timestamp")
            last_flush_timestamp = entity.get("last_flush_timestamp")
            
            # Calculate buffer age
            buffer_age_minutes = None
            time_until_age_flush = None
            age_flush_at = None
            
            if first_record_timestamp:
                try:
                    first_record_dt = datetime.fromisoformat(first_record_timestamp.replace("Z", "+00:00"))
                    buffer_age_minutes = (now - first_record_dt).total_seconds() / 60
                    
                    # Calculate time until max age flush
                    remaining_minutes = max_age_minutes - buffer_age_minutes
                    if remaining_minutes > 0:
                        time_until_age_flush = remaining_minutes
                        age_flush_at = (first_record_dt + timedelta(minutes=max_age_minutes)).isoformat()
                    else:
                        time_until_age_flush = 0
                        age_flush_at = "NOW (overdue)"
                except Exception:
                    pass
            
            # Calculate rows until flush
            rows_until_flush = max(0, max_rows - row_count)
            rows_percentage = (row_count / max_rows * 100) if max_rows > 0 else 0
            
            # Determine flush triggers
            will_flush_on_rows = row_count >= max_rows
            will_flush_on_age = buffer_age_minutes is not None and buffer_age_minutes >= max_age_minutes
            can_flush = row_count >= min_rows  # Has enough rows to flush if triggered
            
            # Next flush prediction
            flush_prediction = "unknown"
            if row_count == 0:
                flush_prediction = "buffer_empty"
            elif will_flush_on_rows:
                flush_prediction = "immediate_row_threshold"
            elif will_flush_on_age:
                flush_prediction = "immediate_age_threshold"
            elif time_until_age_flush is not None:
                flush_prediction = f"age_threshold_in_{time_until_age_flush:.1f}_minutes"
            
            return func.HttpResponse(
                json.dumps({
                    "source_id": source_id,
                    "current_state": {
                        "row_count": row_count,
                        "buffer_age_minutes": round(buffer_age_minutes, 2) if buffer_age_minutes else None,
                        "first_record_timestamp": first_record_timestamp,
                        "last_append_timestamp": last_append_timestamp,
                        "last_flush_timestamp": last_flush_timestamp
                    },
                    "thresholds": {
                        "max_rows": max_rows,
                        "min_rows": min_rows,
                        "max_age_minutes": max_age_minutes
                    },
                    "flush_status": {
                        "rows_until_flush": rows_until_flush,
                        "rows_percentage": round(rows_percentage, 1),
                        "time_until_age_flush_minutes": round(time_until_age_flush, 2) if time_until_age_flush else None,
                        "age_flush_at": age_flush_at,
                        "will_flush_on_rows": will_flush_on_rows,
                        "will_flush_on_age": will_flush_on_age,
                        "can_flush": can_flush,
                        "prediction": flush_prediction
                    },
                    "timestamp": now.isoformat()
                }),
                mimetype="application/json"
            )
        
        except ResourceNotFoundError:
            return func.HttpResponse(
                json.dumps({
                    "source_id": source_id,
                    "current_state": {
                        "row_count": 0,
                        "buffer_age_minutes": None,
                        "first_record_timestamp": None,
                        "last_append_timestamp": None,
                        "last_flush_timestamp": None
                    },
                    "thresholds": {
                        "max_rows": max_rows,
                        "min_rows": min_rows,
                        "max_age_minutes": max_age_minutes
                    },
                    "flush_status": {
                        "rows_until_flush": max_rows,
                        "rows_percentage": 0,
                        "time_until_age_flush_minutes": None,
                        "age_flush_at": None,
                        "will_flush_on_rows": False,
                        "will_flush_on_age": False,
                        "can_flush": False,
                        "prediction": "buffer_empty"
                    },
                    "timestamp": now.isoformat()
                }),
                mimetype="application/json"
            )
    
    except Exception as e:
        logger.error(f"Failed to get flush status for {source_id}: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )
