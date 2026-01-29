"""
Load Config Activity - Loads source configuration.
"""
import azure.durable_functions as df
import logging

from core.config_loader import ConfigLoader

logger = logging.getLogger(__name__)

bp_load_config = df.Blueprint()


@bp_load_config.activity_trigger(input_name="input")
def load_config(input: dict) -> dict | None:
    """
    Load configuration for a specific source.
    
    Input:
        {
            "source_id": str,          # Required: Source identifier
            "force_reload": bool       # Optional: Bypass cache
        }
    
    Output:
        Source configuration dict or None if not found
    """
    source_id = input.get("source_id")
    force_reload = input.get("force_reload", False)
    
    if not source_id:
        logger.error("source_id is required")
        return None
    
    logger.info(f"Loading config for source: {source_id}")
    
    try:
        loader = ConfigLoader()
        source = loader.get_source(source_id, force_reload=force_reload)
        
        if source:
            # Convert Pydantic model to dict
            return source.model_dump()
        
        return None
    
    except Exception as e:
        logger.error(f"Failed to load config for {source_id}: {e}")
        return None
