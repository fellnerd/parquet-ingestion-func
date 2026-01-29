"""
Parse Payload Activity - Extracts data array from API response using JSONPath.
"""
import azure.durable_functions as df
import logging
from typing import Any

from core.jsonpath_parser import extract_by_path

logger = logging.getLogger(__name__)

bp_parse_payload = df.Blueprint()


@bp_parse_payload.activity_trigger(input_name="input")
def parse_payload(input: dict) -> list[dict]:
    """
    Extract array of records from API response using JSONPath.
    
    Input:
        {
            "data": Any,               # Raw API response
            "data_path": str           # JSONPath to data array (e.g., "data.rows")
        }
    
    Output:
        List of record dictionaries
    
    Examples:
        data_path="issues"           → response["issues"]
        data_path="data.rows"        → response["data"]["rows"]
        data_path="$" or ""          → response (if already array)
    """
    data = input.get("data")
    data_path = input.get("data_path", "$")
    
    if data is None:
        logger.warning("No data provided to parse")
        return []
    
    logger.info(f"Parsing payload with data_path: {data_path}")
    
    try:
        # Extract using JSONPath
        records = extract_by_path(data, data_path)
        
        # Validate result is a list
        if not isinstance(records, list):
            logger.warning(f"Expected list at path '{data_path}', got {type(records).__name__}")
            # Try to wrap single record in list
            if isinstance(records, dict):
                records = [records]
            else:
                return []
        
        # Filter out non-dict items
        valid_records = [r for r in records if isinstance(r, dict)]
        
        if len(valid_records) != len(records):
            logger.warning(
                f"Filtered out {len(records) - len(valid_records)} non-dict items from records"
            )
        
        logger.info(f"Parsed {len(valid_records)} records")
        return valid_records
    
    except (KeyError, IndexError) as e:
        logger.error(f"Failed to extract data at path '{data_path}': {e}")
        return []
    
    except Exception as e:
        logger.error(f"Unexpected error parsing payload: {e}")
        return []
