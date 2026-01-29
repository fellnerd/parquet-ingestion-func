"""
JSONPath Parser - Extract nested data using dot notation paths.

Supports:
- Simple paths: "data", "items"
- Nested paths: "data.rows", "response.body.items"
- Array indices: "items[0]", "data.results[0].name"
"""
from typing import Any
import logging

logger = logging.getLogger(__name__)


def extract_by_path(data: Any, path: str) -> Any:
    """
    Extract value from nested dict/list using dot notation path.
    
    Args:
        data: Source dictionary or list
        path: Dot-separated path (e.g., "data.rows", "items[0].name")
              Use "$" or empty string for root
    
    Returns:
        Value at path
        
    Raises:
        KeyError: If path does not exist
        IndexError: If array index is out of bounds
        
    Examples:
        >>> extract_by_path({"data": {"rows": [1, 2, 3]}}, "data.rows")
        [1, 2, 3]
        
        >>> extract_by_path({"items": [{"name": "a"}, {"name": "b"}]}, "items[0].name")
        "a"
        
        >>> extract_by_path([1, 2, 3], "$")
        [1, 2, 3]
    """
    if not path or path == "$":
        return data
    
    current = data
    parts = _split_path(path)
    
    for part in parts:
        try:
            # Handle array index notation: items[0]
            if "[" in part:
                key, indices = _parse_array_access(part)
                if key:
                    current = current[key]
                for idx in indices:
                    current = current[idx]
            else:
                if isinstance(current, dict):
                    current = current[part]
                elif isinstance(current, list) and part.isdigit():
                    current = current[int(part)]
                else:
                    raise KeyError(f"Cannot traverse '{part}' in {type(current).__name__}")
        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"Path extraction failed at '{part}' in path '{path}': {e}")
            raise
    
    return current


def _split_path(path: str) -> list[str]:
    """
    Split a path string into parts, preserving array notation.
    
    Args:
        path: Dot-separated path
        
    Returns:
        List of path parts
    """
    parts = []
    current = ""
    
    for char in path:
        if char == ".":
            if current:
                parts.append(current)
                current = ""
        else:
            current += char
    
    if current:
        parts.append(current)
    
    return parts


def _parse_array_access(part: str) -> tuple[str, list[int]]:
    """
    Parse array access notation from a path part.
    
    Args:
        part: Path part like "items[0]" or "[0][1]"
        
    Returns:
        Tuple of (key, list of indices)
    """
    indices = []
    key = ""
    
    i = 0
    while i < len(part):
        if part[i] == "[":
            # Find closing bracket
            j = part.index("]", i)
            idx = int(part[i+1:j])
            indices.append(idx)
            i = j + 1
        else:
            key += part[i]
            i += 1
    
    return key, indices


def flatten_record(record: dict, prefix: str = "", separator: str = ".") -> dict:
    """
    Flatten nested dict to single-level dict with dot notation keys.
    
    Args:
        record: Nested dictionary to flatten
        prefix: Prefix for keys (used in recursion)
        separator: Separator for nested keys (default ".")
    
    Returns:
        Flattened dictionary
        
    Example:
        >>> flatten_record({"a": {"b": {"c": 1}}, "d": 2})
        {"a.b.c": 1, "d": 2}
    """
    result = {}
    
    for key, value in record.items():
        full_key = f"{prefix}{separator}{key}" if prefix else key
        
        if isinstance(value, dict):
            result.update(flatten_record(value, full_key, separator))
        elif isinstance(value, list):
            # Keep lists as-is (could optionally expand to item[0], item[1], ...)
            result[full_key] = value
        else:
            result[full_key] = value
    
    return result


def unflatten_record(record: dict, separator: str = ".") -> dict:
    """
    Unflatten a dot-notation dict back to nested structure.
    
    Args:
        record: Flattened dictionary with dot-notation keys
        separator: Separator used in keys (default ".")
    
    Returns:
        Nested dictionary
        
    Example:
        >>> unflatten_record({"a.b.c": 1, "d": 2})
        {"a": {"b": {"c": 1}}, "d": 2}
    """
    result = {}
    
    for key, value in record.items():
        parts = key.split(separator)
        current = result
        
        for i, part in enumerate(parts[:-1]):
            if part not in current:
                current[part] = {}
            current = current[part]
        
        current[parts[-1]] = value
    
    return result


def apply_mapping(record: dict, mappings: list[dict]) -> dict:
    """
    Apply field mappings to transform a record.
    
    Args:
        record: Source record
        mappings: List of mapping definitions with source_path and target_column
    
    Returns:
        Transformed record with mapped fields
        
    Example mapping:
        [
            {"source_path": "fields.summary", "target_column": "summary"},
            {"source_path": "id", "target_column": "issue_id", "required": True}
        ]
    """
    result = {}
    flattened = flatten_record(record)
    
    for mapping in mappings:
        source_path = mapping["source_path"]
        target_column = mapping["target_column"]
        required = mapping.get("required", False)
        default = mapping.get("default")
        
        # Try to get value from flattened record
        value = flattened.get(source_path)
        
        # If not found in flattened, try direct extraction
        if value is None:
            try:
                value = extract_by_path(record, source_path)
            except (KeyError, IndexError):
                value = None
        
        # Apply default or check required
        if value is None:
            if default is not None:
                value = default
            elif required:
                raise ValueError(f"Required field '{source_path}' not found in record")
        
        if value is not None:
            result[target_column] = value
    
    return result
