"""
Ingest Orchestrator - Main workflow for data ingestion.

Orchestrates the complete ingest pipeline:
1. Load configuration
2. Check buffer state
3. Fetch data from API (or receive from input)
4. Parse payload using JSONPath
5. Transform data according to schema
6. Manage buffer (append or flush)
7. Write Parquet if conditions are met
8. Update metadata
"""
import azure.functions as func
import azure.durable_functions as df
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

bp_ingest_orchestrator = df.Blueprint()


@bp_ingest_orchestrator.orchestration_trigger(context_name="context")
def ingest_orchestrator(context: df.DurableOrchestrationContext):
    """
    Main orchestrator for ingesting data from a single source.
    
    Input:
        {
            "source_id": str,           # Required: Source identifier
            "force_flush": bool,        # Optional: Force write Parquet
            "trigger_type": str,        # Optional: http, timer, event
            "records": list[dict]       # Optional: Records for passive sources
        }
    
    Output:
        {
            "status": str,              # success, error, skipped
            "source_id": str,
            "records_fetched": int,
            "buffer_rows": int,
            "parquet_written": str | None,
            "error": str | None
        }
    """
    # Get input
    input_data = context.get_input() or {}
    source_id = input_data.get("source_id")
    force_flush = input_data.get("force_flush", False)
    trigger_type = input_data.get("trigger_type", "http")
    input_records = input_data.get("records")
    
    if not source_id:
        return {
            "status": "error",
            "source_id": None,
            "error": "source_id is required"
        }
    
    logger.info(f"Starting ingest orchestration for source: {source_id}")
    
    try:
        # Step 1: Load configuration
        config = yield context.call_activity("load_config", {
            "source_id": source_id
        })
        
        if not config:
            return {
                "status": "error",
                "source_id": source_id,
                "error": f"Source {source_id} not found in configuration"
            }
        
        if not config.get("enabled", True):
            return {
                "status": "skipped",
                "source_id": source_id,
                "error": "Source is disabled"
            }
        
        # Step 2: Check current buffer state
        buffer_state = yield context.call_activity("check_buffer", {
            "source_id": source_id
        })
        
        # Step 3: Fetch data (if active source) or use input records (passive)
        records = []
        fetch_type = config.get("fetch", {}).get("type", "active")
        
        if fetch_type != "passive" and not input_records:
            # Active source: fetch from API
            fetch_result = yield context.call_activity("fetch_api", {
                "config": config,
                "last_fetch": buffer_state.get("last_fetch_timestamp")
            })
            
            if not fetch_result.get("success"):
                return {
                    "status": "error",
                    "source_id": source_id,
                    "error": fetch_result.get("error", "Unknown fetch error")
                }
            
            raw_data = fetch_result.get("data")
            
            # Step 4: Parse payload using JSONPath
            if raw_data:
                records = yield context.call_activity("parse_payload", {
                    "data": raw_data,
                    "data_path": config.get("response", {}).get("data_path", "$")
                })
        else:
            # Passive source or records provided
            records = input_records or []
        
        records_fetched = len(records) if records else 0
        
        if not records:
            logger.info(f"No records to process for source {source_id}")
            return {
                "status": "success",
                "source_id": source_id,
                "records_fetched": 0,
                "buffer_rows": buffer_state.get("row_count", 0),
                "parquet_written": None
            }
        
        # Step 5: Transform data according to schema
        transformed = yield context.call_activity("transform_data", {
            "records": records,
            "config": config
        })
        
        # Step 6: Append to buffer
        new_buffer_state = yield context.call_activity("append_buffer", {
            "source_id": source_id,
            "records": transformed,
            "current_state": buffer_state
        })
        
        # Step 7: Decide if we should write Parquet
        buffer_config = config.get("buffer", {})
        should_write = _should_flush_buffer(
            buffer_state=new_buffer_state,
            buffer_config=buffer_config,
            force_flush=force_flush,
            trigger_type=trigger_type
        )
        
        parquet_written = None
        
        if should_write:
            # Step 8: Write Parquet file
            parquet_result = yield context.call_activity("write_parquet", {
                "source_id": source_id,
                "config": config,
                "buffer_state": new_buffer_state
            })
            
            parquet_written = parquet_result.get("file_path")
            
            # Step 9: Clear buffer after successful write
            yield context.call_activity("clear_buffer", {
                "source_id": source_id
            })
            
            new_buffer_state["row_count"] = 0
        
        # Step 10: Update metadata
        yield context.call_activity("update_metadata", {
            "source_id": source_id,
            "last_fetch": datetime.now(timezone.utc).isoformat(),
            "records_fetched": records_fetched,
            "parquet_written": parquet_written
        })
        
        return {
            "status": "success",
            "source_id": source_id,
            "records_fetched": records_fetched,
            "buffer_rows": new_buffer_state.get("row_count", 0),
            "parquet_written": parquet_written
        }
    
    except Exception as e:
        logger.error(f"Orchestration failed for {source_id}: {e}")
        return {
            "status": "error",
            "source_id": source_id,
            "error": str(e)
        }


def _should_flush_buffer(
    buffer_state: dict,
    buffer_config: dict,
    force_flush: bool,
    trigger_type: str
) -> bool:
    """
    Determine if buffer should be flushed to Parquet.
    
    Conditions:
    1. force_flush is True
    2. row_count >= max_rows
    3. buffer age >= max_age_minutes
    4. row_count >= min_rows AND trigger is timer
    """
    if force_flush:
        return True
    
    row_count = buffer_state.get("row_count", 0)
    max_rows = buffer_config.get("max_rows", 10000)
    min_rows = buffer_config.get("min_rows", 100)
    max_age_minutes = buffer_config.get("max_age_minutes", 60)
    
    # Check max rows
    if row_count >= max_rows:
        logger.info(f"Flushing buffer: row_count ({row_count}) >= max_rows ({max_rows})")
        return True
    
    # Check buffer age
    first_ts = buffer_state.get("first_record_timestamp")
    if first_ts:
        try:
            if isinstance(first_ts, str):
                first_dt = datetime.fromisoformat(first_ts.replace("Z", "+00:00"))
            else:
                first_dt = first_ts
            
            age_minutes = (datetime.now(timezone.utc) - first_dt).total_seconds() / 60
            
            if age_minutes >= max_age_minutes:
                logger.info(f"Flushing buffer: age ({age_minutes:.1f}m) >= max_age ({max_age_minutes}m)")
                return True
        except Exception as e:
            logger.warning(f"Failed to parse first_record_timestamp: {e}")
    
    # Check min rows for timer trigger
    if trigger_type == "timer" and row_count >= min_rows:
        logger.info(f"Flushing buffer: timer trigger with row_count ({row_count}) >= min_rows ({min_rows})")
        return True
    
    return False
