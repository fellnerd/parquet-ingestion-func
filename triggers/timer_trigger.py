"""
Timer Trigger - Scheduled trigger for periodic API fetches.
"""
import azure.durable_functions as df
import azure.functions as func
import json
import logging
import os

from core.config_loader import ConfigLoader

logger = logging.getLogger(__name__)

bp_timer_trigger = df.Blueprint()


@bp_timer_trigger.timer_trigger(
    schedule="%PARQUET_FETCH_SCHEDULE%",  # e.g., "0 */15 * * * *" (every 15 min)
    arg_name="timer",
    run_on_startup=False
)
@bp_timer_trigger.durable_client_input(client_name="client")
async def timer_ingest(timer: func.TimerRequest, client: df.DurableOrchestrationClient) -> None:
    """
    Timer trigger to run scheduled ingests for all active sources.
    
    Environment Variables:
        PARQUET_FETCH_SCHEDULE: CRON expression (default: "0 */15 * * * *")
    
    Behavior:
        1. Loads all source configs
        2. Filters to active sources with "schedule.enabled = true"
        3. Starts orchestration for each source
    """
    if timer.past_due:
        logger.warning("Timer trigger is running late!")
    
    logger.info("Timer trigger fired - starting scheduled ingests")
    
    try:
        loader = ConfigLoader()
        all_sources = loader.get_all_sources()
        
        started_count = 0
        skipped_count = 0
        
        for source_config in all_sources:
            source_id = source_config.source_id
            schedule = source_config.schedule
            
            # Check if scheduled fetch is enabled
            if not (schedule and schedule.enabled):
                logger.debug(f"Skipping {source_id} - scheduling not enabled")
                skipped_count += 1
                continue
            
            # Check fetch type (skip passive sources)
            if source_config.fetch.type == "passive":
                logger.debug(f"Skipping {source_id} - passive source")
                skipped_count += 1
                continue
            
            # Check if source has active fetch endpoint
            if not source_config.fetch.endpoint:
                logger.debug(f"Skipping {source_id} - no endpoint configured")
                skipped_count += 1
                continue
            
            # Start orchestration
            instance_id = await client.start_new(
                orchestration_function_name="ingest_orchestrator",
                instance_id=None,
                client_input={
                    "source_id": source_id,
                    "force_flush": False,
                    "payload": None
                }
            )
            
            logger.info(f"Started orchestration for {source_id}: {instance_id}")
            started_count += 1
        
        logger.info(f"Timer trigger completed: {started_count} started, {skipped_count} skipped")
    
    except Exception as e:
        logger.error(f"Timer trigger failed: {e}")
        raise


@bp_timer_trigger.timer_trigger(
    schedule="%PARQUET_BUFFER_FLUSH_SCHEDULE%",  # e.g., "0 0 * * * *" (every hour)
    arg_name="timer",
    run_on_startup=False
)
@bp_timer_trigger.durable_client_input(client_name="client")
async def timer_buffer_flush(timer: func.TimerRequest, client: df.DurableOrchestrationClient) -> None:
    """
    Timer trigger to force-flush old buffers.
    
    This ensures data doesn't sit in buffers forever when fetch volume is low.
    
    Environment Variables:
        PARQUET_BUFFER_FLUSH_SCHEDULE: CRON expression (default: "0 0 * * * *")
    """
    if timer.past_due:
        logger.warning("Buffer flush timer is running late!")
    
    logger.info("Buffer flush timer fired")
    
    try:
        loader = ConfigLoader()
        all_sources = loader.get_all_sources()
        
        flushed_count = 0
        
        for source_config in all_sources:
            source_id = source_config.source_id
            buffer_config = source_config.buffer
            
            # Only process sources with max_age configured
            if not (buffer_config and buffer_config.max_age_minutes):
                continue
            
            # Start orchestration with force_flush
            instance_id = await client.start_new(
                orchestration_function_name="ingest_orchestrator",
                instance_id=None,
                client_input={
                    "source_id": source_id,
                    "force_flush": True,  # Force flush regardless of row count
                    "payload": None
                }
            )
            
            logger.info(f"Started flush orchestration for {source_id}: {instance_id}")
            flushed_count += 1
        
        logger.info(f"Buffer flush timer completed: {flushed_count} sources")
    
    except Exception as e:
        logger.error(f"Buffer flush timer failed: {e}")
        raise
