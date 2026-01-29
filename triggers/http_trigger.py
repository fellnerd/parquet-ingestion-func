"""
HTTP Trigger - Manual/webhook trigger for ingest orchestrator.
"""
import azure.durable_functions as df
import azure.functions as func
import json
import logging

logger = logging.getLogger(__name__)

bp_http_trigger = df.Blueprint()


@bp_http_trigger.route(route="ingest/{source_id}", methods=["POST"])
@bp_http_trigger.durable_client_input(client_name="client")
async def http_ingest(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """
    HTTP trigger to start ingest orchestration.
    
    POST /api/ingest/{source_id}
    
    Query params:
        - force_flush: bool (default: false) - Force Parquet write regardless of buffer
    
    Body (optional, for passive sources):
        JSON payload to ingest directly
    
    Returns:
        {
            "instance_id": str,
            "status_url": str
        }
    """
    source_id = req.route_params.get("source_id")
    
    if not source_id:
        return func.HttpResponse(
            json.dumps({"error": "source_id is required"}),
            mimetype="application/json",
            status_code=400
        )
    
    # Parse query params
    force_flush = req.params.get("force_flush", "false").lower() == "true"
    
    # Check for payload (passive source)
    payload = None
    try:
        body = req.get_body()
        if body:
            payload = json.loads(body.decode("utf-8"))
    except (json.JSONDecodeError, ValueError):
        pass
    
    logger.info(f"HTTP trigger for source: {source_id}, force_flush: {force_flush}")
    
    # Start orchestration
    instance_id = await client.start_new(
        orchestration_function_name="ingest_orchestrator",
        instance_id=None,
        client_input={
            "source_id": source_id,
            "force_flush": force_flush,
            "payload": payload  # For passive sources with direct data
        }
    )
    
    logger.info(f"Started orchestration with ID: {instance_id}")
    
    # Return management URLs
    return client.create_check_status_response(req, instance_id)


@bp_http_trigger.route(route="ingest/{source_id}", methods=["GET"])
@bp_http_trigger.durable_client_input(client_name="client")
async def http_ingest_status(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """
    Get status of running/completed orchestrations for a source.
    
    GET /api/ingest/{source_id}?instance_id=xxx
    
    Query params:
        - instance_id: Specific orchestration instance (optional)
    
    Returns:
        Orchestration status
    """
    source_id = req.route_params.get("source_id")
    instance_id = req.params.get("instance_id")
    
    if not source_id:
        return func.HttpResponse(
            json.dumps({"error": "source_id is required"}),
            mimetype="application/json",
            status_code=400
        )
    
    if instance_id:
        # Get specific instance status
        status = await client.get_status(instance_id)
        
        if status is None:
            return func.HttpResponse(
                json.dumps({"error": "Instance not found"}),
                mimetype="application/json",
                status_code=404
            )
        
        return func.HttpResponse(
            json.dumps({
                "instance_id": status.instance_id,
                "runtime_status": str(status.runtime_status),
                "created_time": status.created_time.isoformat() if status.created_time else None,
                "last_updated_time": status.last_updated_time.isoformat() if status.last_updated_time else None,
                "output": status.output
            }),
            mimetype="application/json"
        )
    
    # List recent orchestrations (limited to last 100)
    # Note: This requires additional filtering in production
    return func.HttpResponse(
        json.dumps({
            "message": f"Use instance_id param to check specific orchestration for {source_id}"
        }),
        mimetype="application/json"
    )
