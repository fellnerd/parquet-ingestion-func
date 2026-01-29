"""
Fetch API Activity - Fetches data from configured API endpoints.
"""
import azure.durable_functions as df
import httpx
import logging
from datetime import datetime, timezone
from typing import Any

from core.auth_handler import AuthHandler

logger = logging.getLogger(__name__)

bp_fetch_api = df.Blueprint()


@bp_fetch_api.activity_trigger(input_name="input")
async def fetch_api(input: dict) -> dict:
    """
    Fetch data from an API endpoint.
    
    Input:
        {
            "config": dict,            # Source configuration
            "last_fetch": str          # Optional: ISO timestamp of last fetch
        }
    
    Output:
        {
            "success": bool,
            "data": Any,               # Raw API response
            "error": str | None,
            "records_count": int,
            "fetch_timestamp": str
        }
    """
    config = input.get("config", {})
    last_fetch = input.get("last_fetch")
    
    fetch_config = config.get("fetch", {})
    fetch_type = fetch_config.get("type", "active")
    
    # Passive sources don't fetch
    if fetch_type == "passive":
        return {
            "success": True,
            "data": None,
            "error": None,
            "records_count": 0,
            "fetch_timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    endpoint = fetch_config.get("endpoint")
    method = fetch_config.get("method", "GET").upper()
    timeout = fetch_config.get("timeout", 30)
    
    if not endpoint:
        return {
            "success": False,
            "data": None,
            "error": "No endpoint configured",
            "records_count": 0,
            "fetch_timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    logger.info(f"Fetching from {endpoint} using {method}")
    
    try:
        # Setup authentication
        auth_config = fetch_config.get("auth")
        auth_handler = AuthHandler.from_dict(auth_config)
        
        # Build headers
        headers = {
            "Accept": "application/json",
            "User-Agent": "ParquetIngestionFunc/1.0",
            **fetch_config.get("headers", {}),
            **auth_handler.get_headers()
        }
        
        # Build params
        params = _resolve_params(
            fetch_config.get("params", {}),
            last_fetch=last_fetch,
            config=config
        )
        params.update(auth_handler.get_query_params())
        
        # Build body
        body = _resolve_body(
            fetch_config.get("body"),
            last_fetch=last_fetch,
            config=config
        )
        
        # Make request
        async with httpx.AsyncClient(timeout=timeout) as client:
            if method == "GET":
                response = await client.get(endpoint, headers=headers, params=params)
            elif method == "POST":
                response = await client.post(endpoint, headers=headers, params=params, json=body)
            elif method == "PUT":
                response = await client.put(endpoint, headers=headers, params=params, json=body)
            else:
                return {
                    "success": False,
                    "data": None,
                    "error": f"Unsupported method: {method}",
                    "records_count": 0,
                    "fetch_timestamp": datetime.now(timezone.utc).isoformat()
                }
            
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"Successfully fetched data from {endpoint}")
            
            return {
                "success": True,
                "data": data,
                "error": None,
                "records_count": _count_records(data, config),
                "fetch_timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP {e.response.status_code}: {e.response.text[:500]}"
        logger.error(f"HTTP error fetching from {endpoint}: {error_msg}")
        return {
            "success": False,
            "data": None,
            "error": error_msg,
            "records_count": 0,
            "fetch_timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    except httpx.RequestError as e:
        error_msg = f"Request error: {str(e)}"
        logger.error(f"Request error fetching from {endpoint}: {error_msg}")
        return {
            "success": False,
            "data": None,
            "error": error_msg,
            "records_count": 0,
            "fetch_timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"Unexpected error fetching from {endpoint}: {error_msg}")
        return {
            "success": False,
            "data": None,
            "error": error_msg,
            "records_count": 0,
            "fetch_timestamp": datetime.now(timezone.utc).isoformat()
        }


def _resolve_params(params: dict, last_fetch: str | None, config: dict) -> dict:
    """Resolve dynamic parameters in config."""
    resolved = {}
    
    schedule_config = config.get("schedule", {})
    fetch_window = schedule_config.get("fetch_window_minutes", 20)
    
    for key, value in params.items():
        if isinstance(value, str):
            # Replace placeholders
            value = value.replace("{last_fetch_timestamp}", last_fetch or "")
            value = value.replace("{fetch_window_minutes}", str(fetch_window))
            value = value.replace("-{fetch_window_minutes}m", f"-{fetch_window}m")
        
        resolved[key] = value
    
    return resolved


def _resolve_body(body: dict | None, last_fetch: str | None, config: dict) -> dict | None:
    """Resolve dynamic values in request body."""
    if not body:
        return None
    
    import json
    body_str = json.dumps(body)
    
    # Replace placeholders
    body_str = body_str.replace("{last_fetch_timestamp}", last_fetch or "")
    
    return json.loads(body_str)


def _count_records(data: Any, config: dict) -> int:
    """Try to count records in response data."""
    try:
        from core.jsonpath_parser import extract_by_path
        
        data_path = config.get("response", {}).get("data_path", "$")
        records = extract_by_path(data, data_path)
        
        if isinstance(records, list):
            return len(records)
    except Exception:
        pass
    
    return 0
