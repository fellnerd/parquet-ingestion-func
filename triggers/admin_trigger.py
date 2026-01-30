"""
Admin Trigger - Administrative endpoints for management.
"""
import azure.durable_functions as df
import azure.functions as func
import json
import logging
import os
from datetime import datetime, timezone

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
