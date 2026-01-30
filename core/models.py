"""
Pydantic models for type-safe data handling throughout the application.
"""
from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, Field


class AuthConfig(BaseModel):
    """Authentication configuration for API calls."""
    type: str = Field(..., description="Auth type: basic, bearer, api_key, none")
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    api_key: Optional[str] = None
    api_key_header: str = "X-API-Key"


class FetchConfig(BaseModel):
    """Configuration for fetching data from an API."""
    endpoint: str
    method: str = "GET"
    auth: Optional[AuthConfig] = None
    headers: dict[str, str] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    body: Optional[dict[str, Any]] = None
    timeout: int = 30
    type: str = "active"  # active = fetch from API, passive = receive via HTTP


class PaginationConfig(BaseModel):
    """Pagination configuration for API responses."""
    type: str = "offset"  # offset, cursor, link
    offset_param: str = "offset"
    page_size_param: str = "limit"
    page_size: int = 100
    total_path: Optional[str] = None
    next_cursor_path: Optional[str] = None


class ResponseConfig(BaseModel):
    """Configuration for parsing API responses."""
    data_path: str = Field(..., description="JSONPath to the data array")
    schema_ref: Optional[str] = None


class BufferConfig(BaseModel):
    """Buffer configuration for controlling when to write Parquet files."""
    min_rows: int = 100
    max_rows: int = 10000
    max_age_minutes: int = 60


class ScheduleConfig(BaseModel):
    """Schedule configuration for timer triggers."""
    enabled: bool = True
    type: str = "timer"  # timer, http_trigger
    cron: Optional[str] = None
    fetch_window_minutes: int = 20


class OutputConfig(BaseModel):
    """Output configuration for Parquet files."""
    compression: str = "snappy"
    row_group_size: int = 100000


class SourceConfig(BaseModel):
    """Complete source configuration."""
    id: str
    enabled: bool = True
    concept: str
    source: str
    entity: str
    description: Optional[str] = None
    fetch: FetchConfig
    response: ResponseConfig
    pagination: Optional[PaginationConfig] = None
    schedule: Optional[ScheduleConfig] = None
    buffer: BufferConfig = Field(default_factory=BufferConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    
    @property
    def source_id(self) -> str:
        """Alias for id to maintain compatibility."""
        return self.id


class SourceDefinition(BaseModel):
    """Root configuration containing all sources."""
    version: str = "1.0"
    defaults: Optional[dict[str, Any]] = None
    sources: list[SourceConfig]


class BufferState(BaseModel):
    """Current state of a buffer for a specific source."""
    source_id: str
    row_count: int = 0
    first_record_timestamp: Optional[datetime] = None
    last_append_timestamp: Optional[datetime] = None
    last_flush_timestamp: Optional[datetime] = None
    buffer_file_path: Optional[str] = None


class FetchResult(BaseModel):
    """Result of an API fetch operation."""
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    records_count: int = 0
    fetch_timestamp: datetime = Field(default_factory=datetime.utcnow)


class IngestRequest(BaseModel):
    """Request to ingest data for a specific source."""
    source_id: str
    force_flush: bool = False
    trigger_type: str = "http"  # http, timer, event
    records: Optional[list[dict[str, Any]]] = None  # For passive sources


class IngestResult(BaseModel):
    """Result of an ingest operation."""
    status: str  # success, error, skipped
    source_id: str
    records_fetched: int = 0
    records_transformed: int = 0
    buffer_rows: int = 0
    parquet_written: Optional[str] = None
    error: Optional[str] = None
    duration_seconds: float = 0.0


class ParquetMetadata(BaseModel):
    """Metadata for a written Parquet file."""
    file_path: str
    row_count: int
    file_size_bytes: int
    created_at: datetime = Field(default_factory=datetime.utcnow)
    source_id: str
    concept: str
    entity: str
