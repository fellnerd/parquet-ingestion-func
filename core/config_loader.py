"""
Configuration Loader - Merges environment variables with storage-based config.

Priority (highest to lowest):
1. Environment Variables (for secrets and overrides)
2. Storage Blob Config (sources.json)
3. Default Values
"""
import os
import json
import logging
import re
from typing import Optional, Any
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

from .models import SourceConfig, SourceDefinition, BufferConfig

logger = logging.getLogger(__name__)


class ConfigLoader:
    """
    Loads and merges configuration from multiple sources.
    
    Environment variables can override config values using pattern:
    - SOURCE_{SOURCE_ID}_{SETTING} for source-specific settings
    - BUFFER_{SETTING} for buffer defaults
    - PARQUET_{SETTING} for output settings
    """
    
    def __init__(
        self,
        storage_connection: Optional[str] = None,
        config_container: str = "config",
        config_blob: str = "sources.json"
    ):
        self.storage_connection = storage_connection or os.getenv("PARQUET_CONFIG_STORAGE_CONNECTION")
        self.config_container = config_container or os.getenv("PARQUET_CONFIG_CONTAINER", "config")
        self.config_blob = config_blob
        self._config_cache: Optional[SourceDefinition] = None
        self._cache_timestamp: Optional[float] = None
        self._cache_ttl_seconds = 300  # 5 minutes
    
    def _get_blob_client(self):
        """Create blob client for config storage."""
        if self.storage_connection:
            if self.storage_connection == "UseDevelopmentStorage=true":
                # Local development with Azurite
                return BlobServiceClient.from_connection_string(self.storage_connection)
            elif self.storage_connection.startswith("DefaultEndpointsProtocol"):
                return BlobServiceClient.from_connection_string(self.storage_connection)
        
        # Use managed identity
        account_url = f"https://{os.getenv('PARQUET_CONFIG_STORAGE_ACCOUNT')}.blob.core.windows.net"
        credential = DefaultAzureCredential()
        return BlobServiceClient(account_url, credential=credential)
    
    def _load_from_storage(self) -> Optional[dict]:
        """Load configuration from Azure Blob Storage."""
        try:
            logger.info(f"Loading config from storage: container={self.config_container}, blob={self.config_blob}")
            logger.info(f"Storage connection: {self.storage_connection[:50] if self.storage_connection else 'None'}...")
            blob_service = self._get_blob_client()
            blob_client = blob_service.get_blob_client(
                container=self.config_container,
                blob=self.config_blob
            )
            
            download = blob_client.download_blob()
            content = download.readall().decode("utf-8")
            config = json.loads(content)
            logger.info(f"Loaded config with {len(config.get('sources', []))} sources")
            return config
        
        except Exception as e:
            logger.error(f"Failed to load config from storage: {e}", exc_info=True)
            return None
    
    def _resolve_env_variables(self, value: Any) -> Any:
        """
        Resolve environment variable placeholders in config values.
        
        Supports patterns:
        - ${VAR_NAME} - Required variable
        - ${VAR_NAME:default} - Variable with default value
        """
        if not isinstance(value, str):
            return value
        
        pattern = r'\$\{([^}:]+)(?::([^}]*))?\}'
        
        def replacer(match):
            var_name = match.group(1)
            default = match.group(2)
            env_value = os.getenv(var_name)
            
            if env_value is not None:
                return env_value
            elif default is not None:
                return default
            else:
                logger.warning(f"Environment variable {var_name} not set and no default provided")
                return match.group(0)  # Return original placeholder
        
        return re.sub(pattern, replacer, value)
    
    def _resolve_config_values(self, config: dict) -> dict:
        """Recursively resolve environment variables in config."""
        if isinstance(config, dict):
            return {k: self._resolve_config_values(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._resolve_config_values(item) for item in config]
        else:
            return self._resolve_env_variables(config)
    
    def _apply_env_overrides(self, source: dict) -> dict:
        """Apply environment variable overrides to a source config."""
        source_id = source.get("id", "").upper().replace("-", "_")
        
        # Override endpoint
        endpoint_env = os.getenv(f"SOURCE_{source_id}_ENDPOINT")
        if endpoint_env and "fetch" in source:
            source["fetch"]["endpoint"] = endpoint_env
        
        # Override auth token
        token_env = os.getenv(f"SOURCE_{source_id}_TOKEN")
        if token_env and "fetch" in source and "auth" in source["fetch"]:
            source["fetch"]["auth"]["token"] = token_env
        
        api_token_env = os.getenv(f"SOURCE_{source_id}_API_TOKEN")
        if api_token_env and "fetch" in source and "auth" in source["fetch"]:
            source["fetch"]["auth"]["password"] = api_token_env
        
        # Override user
        user_env = os.getenv(f"SOURCE_{source_id}_USER")
        if user_env and "fetch" in source and "auth" in source["fetch"]:
            source["fetch"]["auth"]["username"] = user_env
        
        return source
    
    def _get_default_buffer_config(self) -> dict:
        """Get buffer defaults from environment variables."""
        return {
            "min_rows": int(os.getenv("BUFFER_MIN_ROWS", "100")),
            "max_rows": int(os.getenv("BUFFER_MAX_ROWS", "10000")),
            "max_age_minutes": int(os.getenv("BUFFER_MAX_AGE_MINUTES", "60"))
        }
    
    def load_all_sources(self, force_reload: bool = False) -> SourceDefinition:
        """
        Load all source configurations.
        
        Args:
            force_reload: If True, bypass cache and reload from storage
            
        Returns:
            SourceDefinition containing all configured sources
        """
        import time
        
        # Check cache
        if not force_reload and self._config_cache:
            if self._cache_timestamp and (time.time() - self._cache_timestamp) < self._cache_ttl_seconds:
                return self._config_cache
        
        # Load from storage
        raw_config = self._load_from_storage()
        
        if raw_config is None:
            # Return empty config if storage fails
            logger.warning("No config loaded from storage, using empty config")
            return SourceDefinition(sources=[])
        
        # Resolve environment variables
        resolved_config = self._resolve_config_values(raw_config)
        
        # Apply defaults
        defaults = resolved_config.get("defaults", {})
        default_buffer = {**self._get_default_buffer_config(), **defaults.get("buffer", {})}
        default_output = defaults.get("output", {"compression": "snappy", "row_group_size": 100000})
        
        # Process each source
        sources = []
        for source in resolved_config.get("sources", []):
            # Merge defaults
            if "buffer" not in source:
                source["buffer"] = default_buffer
            else:
                source["buffer"] = {**default_buffer, **source["buffer"]}
            
            if "output" not in source:
                source["output"] = default_output
            else:
                source["output"] = {**default_output, **source["output"]}
            
            # Apply env overrides
            source = self._apply_env_overrides(source)
            
            try:
                sources.append(SourceConfig(**source))
            except Exception as e:
                logger.error(f"Failed to parse source config {source.get('id')}: {e}")
        
        # Cache result
        self._config_cache = SourceDefinition(
            version=resolved_config.get("version", "1.0"),
            defaults=defaults,
            sources=sources
        )
        self._cache_timestamp = time.time()
        
        logger.info(f"Loaded {len(sources)} source configurations")
        return self._config_cache
    
    def get_source(self, source_id: str, force_reload: bool = False) -> Optional[SourceConfig]:
        """
        Get configuration for a specific source.
        
        Args:
            source_id: The unique identifier of the source
            force_reload: If True, bypass cache and reload from storage
            
        Returns:
            SourceConfig or None if not found
        """
        config = self.load_all_sources(force_reload)
        
        for source in config.sources:
            if source.id == source_id:
                return source
        
        logger.warning(f"Source {source_id} not found in configuration")
        return None
    
    def get_enabled_sources(self, schedule_type: Optional[str] = None) -> list[SourceConfig]:
        """
        Get all enabled sources, optionally filtered by schedule type.
        
        Args:
            schedule_type: Filter by schedule type (timer, http_trigger)
            
        Returns:
            List of enabled SourceConfig objects
        """
        config = self.load_all_sources()
        
        sources = [s for s in config.sources if s.enabled]
        
        if schedule_type:
            sources = [s for s in sources if s.schedule and s.schedule.type == schedule_type]
        
        return sources
    
    def invalidate_cache(self):
        """Invalidate the configuration cache."""
        self._config_cache = None
        self._cache_timestamp = None
        logger.info("Configuration cache invalidated")
