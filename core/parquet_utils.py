"""
Parquet Writer - Utilities for writing data to Parquet files.

Uses PyArrow for efficient Parquet file creation with:
- Automatic schema inference
- Compression support (snappy, gzip, zstd)
- Metadata injection (dss_* columns)
"""
import os
import logging
from datetime import datetime, timezone
from typing import Optional, Any
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)


class ParquetWriter:
    """
    Writes data to Parquet files in Azure Data Lake Storage.
    
    Features:
    - Automatic schema inference from data
    - Configurable compression
    - Metadata column injection
    - Streaming upload to ADLS
    """
    
    def __init__(
        self,
        adls_account: Optional[str] = None,
        container: str = "stage-fs",
        connection_string: Optional[str] = None
    ):
        self.adls_account = adls_account or os.getenv("PARQUET_OUTPUT_ADLS_ACCOUNT")
        self.container = container or os.getenv("PARQUET_OUTPUT_CONTAINER", "stage-fs")
        self.connection_string = connection_string
    
    def _get_blob_service(self) -> BlobServiceClient:
        """Create blob service client for ADLS."""
        if self.connection_string:
            if self.connection_string == "UseDevelopmentStorage=true":
                return BlobServiceClient.from_connection_string(self.connection_string)
            return BlobServiceClient.from_connection_string(self.connection_string)
        
        # Use managed identity
        account_url = f"https://{self.adls_account}.blob.core.windows.net"
        credential = DefaultAzureCredential()
        return BlobServiceClient(account_url, credential=credential)
    
    def _inject_metadata(
        self,
        records: list[dict],
        concept: str,
        source: str,
        file_name: str
    ) -> list[dict]:
        """
        Inject metadata columns into records.
        
        Adds:
        - dss_load_date: Current UTC timestamp
        - dss_record_source: concept/source identifier
        - dss_source_file_name: Parquet file name
        """
        timestamp = datetime.now(timezone.utc).isoformat()
        record_source = f"{concept}/{source}"
        
        for record in records:
            record["dss_load_date"] = timestamp
            record["dss_record_source"] = record_source
            record["dss_source_file_name"] = file_name
        
        return records
    
    def _infer_schema(self, records: list[dict]) -> pa.Schema:
        """
        Infer PyArrow schema from records.
        
        Handles common Python types and maps them to Arrow types.
        """
        if not records:
            raise ValueError("Cannot infer schema from empty records")
        
        # Collect all unique keys across records
        all_keys = set()
        for record in records:
            all_keys.update(record.keys())
        
        # Infer type for each key from first non-null value
        fields = []
        for key in sorted(all_keys):
            arrow_type = pa.string()  # Default to string
            
            for record in records:
                value = record.get(key)
                if value is not None:
                    arrow_type = self._python_to_arrow_type(value)
                    break
            
            fields.append(pa.field(key, arrow_type))
        
        return pa.schema(fields)
    
    def _python_to_arrow_type(self, value: Any) -> pa.DataType:
        """Map Python type to PyArrow type."""
        if isinstance(value, bool):
            return pa.bool_()
        elif isinstance(value, int):
            return pa.int64()
        elif isinstance(value, float):
            return pa.float64()
        elif isinstance(value, datetime):
            return pa.timestamp("us", tz="UTC")
        elif isinstance(value, list):
            if value and len(value) > 0:
                inner_type = self._python_to_arrow_type(value[0])
                return pa.list_(inner_type)
            return pa.list_(pa.string())
        elif isinstance(value, dict):
            # Convert dict to JSON string
            return pa.string()
        else:
            return pa.string()
    
    def _normalize_records(self, records: list[dict], schema: pa.Schema) -> list[dict]:
        """
        Normalize records to match schema.
        
        - Ensures all keys exist in each record
        - Converts dicts to JSON strings
        """
        import json
        
        field_names = [f.name for f in schema]
        normalized = []
        
        for record in records:
            norm_record = {}
            for name in field_names:
                value = record.get(name)
                
                # Convert dict to JSON string
                if isinstance(value, dict):
                    value = json.dumps(value)
                
                norm_record[name] = value
            
            normalized.append(norm_record)
        
        return normalized
    
    def write(
        self,
        records: list[dict],
        concept: str,
        source: str,
        entity: str,
        compression: str = "snappy",
        row_group_size: int = 100000,
        inject_metadata: bool = True
    ) -> dict:
        """
        Write records to a Parquet file in ADLS.
        
        Args:
            records: List of dictionaries to write
            concept: Concept name (e.g., "jira")
            source: Source name (e.g., "api")
            entity: Entity name (e.g., "vorgang")
            compression: Compression codec (snappy, gzip, zstd, none)
            row_group_size: Number of rows per row group
            inject_metadata: Whether to inject dss_* columns
        
        Returns:
            Dictionary with file_path, row_count, file_size_bytes
        """
        if not records:
            raise ValueError("Cannot write empty records")
        
        # Generate file name with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        file_name = f"{timestamp}.parquet"
        blob_path = f"{concept}/{source}/{entity}/delta/{file_name}"
        
        logger.info(f"Writing {len(records)} records to {blob_path}")
        
        # Inject metadata columns
        if inject_metadata:
            records = self._inject_metadata(records, concept, source, file_name)
        
        # Infer schema and normalize records
        schema = self._infer_schema(records)
        normalized = self._normalize_records(records, schema)
        
        # Create PyArrow table
        table = pa.Table.from_pylist(normalized, schema=schema)
        
        # Write to buffer
        buffer = BytesIO()
        pq.write_table(
            table,
            buffer,
            compression=compression if compression != "none" else None,
            row_group_size=row_group_size
        )
        
        # Get file size
        file_size = buffer.tell()
        buffer.seek(0)
        
        # Upload to ADLS
        blob_service = self._get_blob_service()
        blob_client = blob_service.get_blob_client(
            container=self.container,
            blob=blob_path
        )
        
        blob_client.upload_blob(buffer, overwrite=True)
        
        logger.info(f"Successfully wrote {blob_path} ({file_size} bytes)")
        
        return {
            "file_path": blob_path,
            "row_count": len(records),
            "file_size_bytes": file_size,
            "created_at": timestamp
        }
    
    def write_from_buffer(
        self,
        buffer_path: str,
        concept: str,
        source: str,
        entity: str,
        compression: str = "snappy"
    ) -> dict:
        """
        Read records from buffer blob and write to Parquet.
        
        Args:
            buffer_path: Path to buffer blob (JSONL format)
            concept, source, entity: Output path components
            compression: Compression codec
        
        Returns:
            Dictionary with file info
        """
        import json
        
        # Read buffer
        blob_service = self._get_blob_service()
        blob_client = blob_service.get_blob_client(
            container=self.container,
            blob=buffer_path
        )
        
        content = blob_client.download_blob().readall().decode("utf-8")
        
        # Parse JSONL
        records = []
        for line in content.strip().split("\n"):
            if line:
                records.append(json.loads(line))
        
        if not records:
            raise ValueError(f"Buffer {buffer_path} is empty")
        
        # Write to Parquet
        return self.write(records, concept, source, entity, compression)
