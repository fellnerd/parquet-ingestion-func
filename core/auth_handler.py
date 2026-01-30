"""
Authentication Handler - Supports multiple authentication strategies.

Supported auth types:
- none: No authentication
- basic: HTTP Basic Auth (username/password)
- bearer: Bearer token (Authorization: Bearer xxx)
- token: Token auth (Authorization: Token xxx) - used by Django REST Framework
- api_key: API key in header or query param
"""
import base64
import logging
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class AuthConfig:
    """Authentication configuration."""
    type: str  # none, basic, bearer, token, api_key
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    api_key: Optional[str] = None
    api_key_header: str = "X-API-Key"
    api_key_query_param: Optional[str] = None


class AuthHandler:
    """
    Handles authentication for API requests.
    
    Usage:
        auth = AuthHandler(AuthConfig(type="bearer", token="xxx"))
        headers = auth.get_headers()
        params = auth.get_query_params()
    """
    
    def __init__(self, config: Optional[AuthConfig] = None):
        self.config = config or AuthConfig(type="none")
    
    @classmethod
    def from_dict(cls, config_dict: Optional[dict]) -> "AuthHandler":
        """Create AuthHandler from a dictionary configuration."""
        if not config_dict:
            return cls(AuthConfig(type="none"))
        
        return cls(AuthConfig(
            type=config_dict.get("type", "none"),
            username=config_dict.get("username"),
            password=config_dict.get("password"),
            token=config_dict.get("token"),
            api_key=config_dict.get("api_key"),
            api_key_header=config_dict.get("api_key_header", "X-API-Key"),
            api_key_query_param=config_dict.get("api_key_query_param")
        ))
    
    def get_headers(self) -> dict[str, str]:
        """
        Get authentication headers for the request.
        
        Returns:
            Dictionary of headers to add to the request
        """
        headers = {}
        
        if self.config.type == "basic":
            if self.config.username and self.config.password:
                credentials = f"{self.config.username}:{self.config.password}"
                encoded = base64.b64encode(credentials.encode()).decode()
                headers["Authorization"] = f"Basic {encoded}"
            else:
                logger.warning("Basic auth configured but username/password missing")
        
        elif self.config.type == "bearer":
            if self.config.token:
                headers["Authorization"] = f"Bearer {self.config.token}"
            else:
                logger.warning("Bearer auth configured but token missing")
        
        elif self.config.type == "token":
            # Django REST Framework style token auth
            if self.config.token:
                headers["Authorization"] = f"Token {self.config.token}"
            else:
                logger.warning("Token auth configured but token missing")
        
        elif self.config.type == "api_key":
            if self.config.api_key and not self.config.api_key_query_param:
                headers[self.config.api_key_header] = self.config.api_key
            elif not self.config.api_key:
                logger.warning("API key auth configured but api_key missing")
        
        return headers
    
    def get_query_params(self) -> dict[str, str]:
        """
        Get authentication query parameters for the request.
        
        Returns:
            Dictionary of query parameters to add to the request
        """
        params = {}
        
        if self.config.type == "api_key":
            if self.config.api_key and self.config.api_key_query_param:
                params[self.config.api_key_query_param] = self.config.api_key
        
        return params
    
    def is_configured(self) -> bool:
        """Check if authentication is properly configured."""
        if self.config.type == "none":
            return True
        
        if self.config.type == "basic":
            return bool(self.config.username and self.config.password)
        
        if self.config.type == "bearer":
            return bool(self.config.token)
        
        if self.config.type == "token":
            return bool(self.config.token)
        
        if self.config.type == "api_key":
            return bool(self.config.api_key)
        
        return False
    
    def __repr__(self) -> str:
        return f"AuthHandler(type={self.config.type}, configured={self.is_configured()})"
