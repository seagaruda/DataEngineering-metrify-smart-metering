"""
Authentication Middleware
Handles JWT token validation and user authentication
"""

import logging
from typing import Optional
from fastapi import Request, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
from datetime import datetime, timedelta

from src.core.config.config_loader import get_security_config

logger = logging.getLogger(__name__)

security = HTTPBearer()


class AuthMiddleware:
    """
    Authentication middleware for JWT token validation
    
    Handles JWT token extraction, validation, and user context
    for secure API access.
    """
    
    def __init__(self, app):
        self.app = app
        self.security_config = get_security_config()
        self.secret_key = self.security_config.jwt_secret_key
        self.algorithm = self.security_config.jwt_algorithm
        self.token_expire_minutes = self.security_config.jwt_access_token_expire_minutes
    
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        
        request = Request(scope, receive)
        
        # Skip authentication for public endpoints
        if self._is_public_endpoint(request.url.path):
            await self.app(scope, receive, send)
            return
        
        try:
            # Extract and validate token
            token = await self._extract_token(request)
            if not token:
                await self._send_unauthorized_response(send)
                return
            
            # Validate token
            payload = await self._validate_token(token)
            if not payload:
                await self._send_unauthorized_response(send)
                return
            
            # Add user context to request
            scope["user"] = payload
            
            await self.app(scope, receive, send)
            
        except HTTPException:
            await self._send_unauthorized_response(send)
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            await self._send_unauthorized_response(send)
    
    def _is_public_endpoint(self, path: str) -> bool:
        """Check if endpoint is public (no authentication required)"""
        # Only true public/docs endpoints are whitelisted. All /api/v1/* data
        # endpoints (including upload/trigger) require authentication.
        public_endpoints = [
            "/health",
            "/docs",
            "/openapi.json",
            "/api/health",
            "/api/docs",
            "/api/openapi.json",
        ]
        return path in public_endpoints
    
    async def _extract_token(self, request: Request) -> Optional[str]:
        """Extract JWT token from request"""
        try:
            # Try to get token from Authorization header
            authorization = request.headers.get("Authorization")
            if authorization and authorization.startswith("Bearer "):
                return authorization.split(" ")[1]
            
            # Try to get token from query parameter
            token = request.query_params.get("token")
            if token:
                return token
            
            return None
            
        except Exception as e:
            logger.error(f"Error extracting token: {str(e)}")
            return None
    
    async def _validate_token(self, token: str) -> Optional[dict]:
        """Validate JWT token"""
        try:
            payload = jwt.decode(
                token,
                self.secret_key,
                algorithms=[self.algorithm]
            )
            
            # Check token expiration
            exp = payload.get("exp")
            if exp and datetime.utcnow().timestamp() > exp:
                logger.warning("Token has expired")
                return None
            
            # Check token issuer
            iss = payload.get("iss")
            if iss != "metrify-smart-metering":
                logger.warning(f"Invalid token issuer: {iss}")
                return None
            
            return payload
            
        except jwt.ExpiredSignatureError:
            logger.warning("Token has expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid token: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error validating token: {str(e)}")
            return None
    
    async def _send_unauthorized_response(self, send):
        """Send unauthorized response"""
        response = {
            "type": "http.response.start",
            "status": 401,
            "headers": [
                [b"content-type", b"application/json"],
                [b"www-authenticate", b'Bearer realm="api"']
            ]
        }
        await send(response)
        
        body = {
            "error": "Unauthorized",
            "message": "Invalid or missing authentication token"
        }
        await send({
            "type": "http.response.body",
            "body": str(body).encode()
        })


class AuthService:
    """
    Authentication service for token management
    
    Handles token generation, validation, and refresh operations.
    """
    
    def __init__(self):
        self.security_config = get_security_config()
        self.secret_key = self.security_config.jwt_secret_key
        self.algorithm = self.security_config.jwt_algorithm
        self.token_expire_minutes = self.security_config.jwt_access_token_expire_minutes
        self.refresh_token_expire_days = self.security_config.refresh_token_expire_days
    
    def create_access_token(self, user_id: str, username: str, roles: list) -> str:
        """Create JWT access token"""
        try:
            now = datetime.utcnow()
            expire = now + timedelta(minutes=self.token_expire_minutes)
            
            payload = {
                "sub": user_id,
                "username": username,
                "roles": roles,
                "iat": now.timestamp(),
                "exp": expire.timestamp(),
                "iss": "metrify-smart-metering",
                "type": "access"
            }
            
            token = jwt.encode(payload, self.secret_key, algorithm=self.algorithm)
            return token
            
        except Exception as e:
            logger.error(f"Error creating access token: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create access token"
            )
    
    def create_refresh_token(self, user_id: str) -> str:
        """Create JWT refresh token"""
        try:
            now = datetime.utcnow()
            expire = now + timedelta(days=self.refresh_token_expire_days)
            
            payload = {
                "sub": user_id,
                "iat": now.timestamp(),
                "exp": expire.timestamp(),
                "iss": "metrify-smart-metering",
                "type": "refresh"
            }
            
            token = jwt.encode(payload, self.secret_key, algorithm=self.algorithm)
            return token
            
        except Exception as e:
            logger.error(f"Error creating refresh token: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create refresh token"
            )
    
    def validate_token(self, token: str) -> dict:
        """Validate JWT token"""
        try:
            payload = jwt.decode(
                token,
                self.secret_key,
                algorithms=[self.algorithm]
            )
            
            # Check token type
            token_type = payload.get("type")
            if token_type not in ["access", "refresh"]:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid token type"
                )
            
            return payload
            
        except jwt.ExpiredSignatureError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has expired"
            )
        except jwt.InvalidTokenError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token"
            )
    
    def refresh_access_token(self, refresh_token: str) -> str:
        """Refresh access token using refresh token"""
        try:
            # Validate refresh token
            payload = self.validate_token(refresh_token)
            
            if payload.get("type") != "refresh":
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid refresh token"
                )
            
            user_id = payload.get("sub")
            if not user_id:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid refresh token payload"
                )
            
            # Create new access token
            # Note: In a real implementation, you would fetch user data from database
            new_access_token = self.create_access_token(
                user_id=user_id,
                username="user",  # Fetch from database
                roles=["user"]    # Fetch from database
            )
            
            return new_access_token
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error refreshing access token: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to refresh access token"
            )
