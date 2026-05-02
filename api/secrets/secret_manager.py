"""
Google Cloud Secret Manager Integration

Secure secrets management for:
- API keys (Pinecone, external services)
- Database credentials
- Service account keys
- Encryption keys

Features:
- Automatic secret rotation support
- Version management
- Local development fallback
- Caching for performance
"""

import os
import logging
from functools import lru_cache
from typing import Optional

from google.cloud import secretmanager
from google.api_core import exceptions

logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", os.environ.get("GOOGLE_CLOUD_PROJECT"))
USE_SECRET_MANAGER = os.environ.get("USE_SECRET_MANAGER", "true").lower() == "true"


class SecretManagerClient:
    """
    Client for Google Cloud Secret Manager.

    Provides secure access to secrets with caching and fallback support.
    """

    def __init__(self, project_id: Optional[str] = None):
        self.project_id = project_id or PROJECT_ID
        self._client: Optional[secretmanager.SecretManagerServiceClient] = None

    @property
    def client(self) -> secretmanager.SecretManagerServiceClient:
        """Lazy initialization of Secret Manager client."""
        if self._client is None:
            self._client = secretmanager.SecretManagerServiceClient()
        return self._client

    def get_secret(
        self,
        secret_id: str,
        version: str = "latest",
        fallback_env_var: Optional[str] = None,
    ) -> Optional[str]:
        """
        Retrieve a secret value.

        Args:
            secret_id: Secret identifier
            version: Secret version (default: "latest")
            fallback_env_var: Environment variable to use if Secret Manager unavailable

        Returns:
            Secret value as string, or None if not found
        """
        # Try environment variable first (local development)
        if fallback_env_var:
            env_value = os.environ.get(fallback_env_var)
            if env_value:
                logger.debug(f"Using environment variable for {secret_id}")
                return env_value

        # Skip Secret Manager if disabled
        if not USE_SECRET_MANAGER:
            logger.warning(f"Secret Manager disabled, no value for {secret_id}")
            return None

        try:
            name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version}"
            response = self.client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")

        except exceptions.NotFound:
            logger.error(f"Secret not found: {secret_id}")
            return None
        except exceptions.PermissionDenied:
            logger.error(f"Permission denied for secret: {secret_id}")
            return None
        except Exception as e:
            logger.error(f"Error accessing secret {secret_id}: {e}")
            return None

    def create_secret(self, secret_id: str, secret_value: str) -> bool:
        """
        Create a new secret.

        Args:
            secret_id: Secret identifier
            secret_value: Secret value to store

        Returns:
            True if successful
        """
        try:
            # Create the secret
            parent = f"projects/{self.project_id}"
            self.client.create_secret(
                request={
                    "parent": parent,
                    "secret_id": secret_id,
                    "secret": {"replication": {"automatic": {}}},
                }
            )

            # Add the secret version
            self.add_secret_version(secret_id, secret_value)
            logger.info(f"Created secret: {secret_id}")
            return True

        except exceptions.AlreadyExists:
            logger.warning(f"Secret already exists: {secret_id}")
            return False
        except Exception as e:
            logger.error(f"Error creating secret {secret_id}: {e}")
            return False

    def add_secret_version(self, secret_id: str, secret_value: str) -> Optional[str]:
        """
        Add a new version to an existing secret.

        Args:
            secret_id: Secret identifier
            secret_value: New secret value

        Returns:
            Version name if successful
        """
        try:
            parent = f"projects/{self.project_id}/secrets/{secret_id}"
            response = self.client.add_secret_version(
                request={
                    "parent": parent,
                    "payload": {"data": secret_value.encode("UTF-8")},
                }
            )
            logger.info(f"Added new version for secret: {secret_id}")
            return response.name

        except Exception as e:
            logger.error(f"Error adding secret version: {e}")
            return None

    def delete_secret(self, secret_id: str) -> bool:
        """Delete a secret."""
        try:
            name = f"projects/{self.project_id}/secrets/{secret_id}"
            self.client.delete_secret(request={"name": name})
            logger.info(f"Deleted secret: {secret_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting secret: {e}")
            return False


# Cached secret accessor
@lru_cache(maxsize=32)
def get_secret(secret_id: str, fallback_env_var: Optional[str] = None) -> Optional[str]:
    """
    Get a secret value with caching.

    Args:
        secret_id: Secret identifier
        fallback_env_var: Environment variable fallback

    Returns:
        Secret value
    """
    client = SecretManagerClient()
    return client.get_secret(secret_id, fallback_env_var=fallback_env_var)


# Pre-defined secret accessors for common secrets
def get_pinecone_api_key() -> Optional[str]:
    """Get Pinecone API key."""
    return get_secret("pinecone-api-key", fallback_env_var="PINECONE_API_KEY")


def get_redis_password() -> Optional[str]:
    """Get Redis password."""
    return get_secret("redis-password", fallback_env_var="REDIS_PASSWORD")


def get_database_url() -> Optional[str]:
    """Get database connection URL."""
    return get_secret("database-url", fallback_env_var="DATABASE_URL")


def get_api_key(service_name: str) -> Optional[str]:
    """Get API key for a specific service."""
    return get_secret(f"{service_name}-api-key", fallback_env_var=f"{service_name.upper()}_API_KEY")
