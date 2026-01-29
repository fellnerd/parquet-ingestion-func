"""
Parquet Ingestion Function - Main Entry Point

Azure Durable Function App for ingesting data from APIs and writing Parquet files.
Uses a modular architecture with separate orchestrators, activities, and triggers.
"""
import azure.functions as func
import azure.durable_functions as df
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create the Durable Functions app
app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

# =============================================================================
# ORCHESTRATORS
# =============================================================================
from orchestrators.ingest_orchestrator import bp_ingest_orchestrator
app.register_functions(bp_ingest_orchestrator)

# =============================================================================
# ACTIVITIES
# =============================================================================
from activities.load_config import bp_load_config
from activities.fetch_api import bp_fetch_api
from activities.parse_payload import bp_parse_payload
from activities.transform_data import bp_transform_data
from activities.buffer_manager import bp_buffer_manager
from activities.write_parquet import bp_write_parquet
from activities.update_metadata import bp_update_metadata

app.register_functions(bp_load_config)
app.register_functions(bp_fetch_api)
app.register_functions(bp_parse_payload)
app.register_functions(bp_transform_data)
app.register_functions(bp_buffer_manager)
app.register_functions(bp_write_parquet)
app.register_functions(bp_update_metadata)

# =============================================================================
# TRIGGERS
# =============================================================================
from triggers.http_trigger import bp_http_trigger
from triggers.timer_trigger import bp_timer_trigger
from triggers.admin_trigger import bp_admin_trigger

app.register_functions(bp_http_trigger)
app.register_functions(bp_timer_trigger)
app.register_functions(bp_admin_trigger)

logger.info("Parquet Ingestion Function App initialized successfully")
