"""
Core module - Shared utilities and base classes.
"""
from .config_loader import ConfigLoader, SourceConfig, BufferConfig
from .jsonpath_parser import extract_by_path, flatten_record
from .auth_handler import AuthHandler, AuthConfig
from .parquet_utils import ParquetWriter
from .models import (
    IngestRequest,
    IngestResult,
    BufferState,
    FetchResult,
    SourceDefinition,
)

__all__ = [
    "ConfigLoader",
    "SourceConfig", 
    "BufferConfig",
    "extract_by_path",
    "flatten_record",
    "AuthHandler",
    "AuthConfig",
    "ParquetWriter",
    "IngestRequest",
    "IngestResult",
    "BufferState",
    "FetchResult",
    "SourceDefinition",
]
