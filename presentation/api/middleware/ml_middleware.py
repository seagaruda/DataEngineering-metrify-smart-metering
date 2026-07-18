"""
ML Middleware
Custom middleware for ML inference API including authentication, rate limiting, and monitoring
"""

import time
import uuid
import os
import logging
from typing import Callable, Dict, Any
from fastapi import Request, Response, HTTPException
from fastapi.responses import JSONResponse
import redis
import jwt
from datetime import datetime, timedelta
import asyncio
from collections import defaultdict, deque
import json

logger = logging.getLogger(__name__)

class RateLimiter:
    """Rate limiter using sliding window algorithm"""
    
    def __init__(self, redis_client, max_requests: int = 100, window_seconds: int = 60):
        self.redis_client = redis_client
        self.max_requests = max_requests
        self.window_seconds = window_seconds
    
    async def is_allowed(self, client_id: str) -> bool:
        """Check if request is allowed for client"""
        try:
            current_time = int(time.time())
            window_start = current_time - self.window_seconds
            
            # Use Redis sorted set for sliding window
            key = f"rate_limit:{client_id}"
            
            # Remove old entries
            self.redis_client.zremrangebyscore(key, 0, window_start)
            
            # Count current requests
            current_requests = self.redis_client.zcard(key)
            
            if current_requests >= self.max_requests:
                return False
            
            # Add current request
            self.redis_client.zadd(key, {str(current_time): current_time})
            self.redis_client.expire(key, self.window_seconds)
            
            return True
            
        except Exception as e:
            logger.error(f"Rate limiting error: {str(e)}")
            return True  # Allow on error

class MLAuthenticationMiddleware:
    """JWT-based authentication middleware for ML endpoints"""
    
    def __init__(self, secret_key: str, algorithm: str = "HS256"):
        self.secret_key = secret_key
        self.algorithm = algorithm
    
    def verify_token(self, token: str) -> Dict[str, Any]:
        """Verify JWT token"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expired")
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid token")
    
    def create_token(self, user_id: str, expires_delta: timedelta = None) -> str:
        """Create JWT token"""
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(hours=24)
        
        payload = {
            "user_id": user_id,
            "exp": expire,
            "iat": datetime.utcnow()
        }
        
        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)

class MLMonitoringMiddleware:
    """Monitoring and metrics collection middleware for ML endpoints"""
    
    def __init__(self):
        self.request_counts = defaultdict(int)
        self.response_times = defaultdict(list)
        self.error_counts = defaultdict(int)
        self.active_requests = 0
    
    def record_request(self, endpoint: str, method: str, response_time: float, status_code: int):
        """Record request metrics"""
        key = f"{method}:{endpoint}"
        self.request_counts[key] += 1
        self.response_times[key].append(response_time)
        
        if status_code >= 400:
            self.error_counts[key] += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        metrics = {}
        
        for key, count in self.request_counts.items():
            response_times = self.response_times[key]
            avg_response_time = sum(response_times) / len(response_times) if response_times else 0
            
            metrics[key] = {
                "request_count": count,
                "error_count": self.error_counts[key],
                "average_response_time": avg_response_time,
                "error_rate": self.error_counts[key] / count if count > 0 else 0
            }
        
        return metrics

class MLMiddleware:
    """Main ML middleware class"""
    
    def __init__(self, 
                 redis_url: str = "redis://localhost:6379",
                 jwt_secret: str = os.getenv("JWT_SECRET_KEY", ""),
                 rate_limit_requests: int = 100,
                 rate_limit_window: int = 60):
        
        # Initialize Redis client
        try:
            self.redis_client = redis.from_url(redis_url, decode_responses=True)
            self.redis_client.ping()
        except Exception as e:
            logger.warning(f"Redis connection failed: {str(e)}")
            self.redis_client = None
        
        # Initialize components
        self.rate_limiter = RateLimiter(self.redis_client, rate_limit_requests, rate_limit_window)
        self.auth = MLAuthenticationMiddleware(jwt_secret)
        self.monitoring = MLMonitoringMiddleware()
        
        # Request tracking
        self.active_requests = {}
    
    async def __call__(self, request: Request, call_next: Callable) -> Response:
        """Main middleware function"""
        start_time = time.time()
        request_id = str(uuid.uuid4())
        
        # Add request ID to request state
        request.state.request_id = request_id
        request.state.start_time = start_time
        
        try:
            # Authentication (skip for health checks)
            if not request.url.path.startswith("/health"):
                await self._authenticate_request(request)
            
            # Rate limiting
            if not request.url.path.startswith("/health"):
                await self._rate_limit_request(request)
            
            # Process request
            response = await call_next(request)
            
            # Record metrics
            processing_time = (time.time() - start_time) * 1000
            self.monitoring.record_request(
                endpoint=request.url.path,
                method=request.method,
                response_time=processing_time,
                status_code=response.status_code
            )
            
            # Add headers
            response.headers["X-Request-ID"] = request_id
            response.headers["X-Processing-Time"] = str(processing_time)
            
            return response
            
        except HTTPException as e:
            # Handle HTTP exceptions
            processing_time = (time.time() - start_time) * 1000
            self.monitoring.record_request(
                endpoint=request.url.path,
                method=request.method,
                response_time=processing_time,
                status_code=e.status_code
            )
            
            return JSONResponse(
                status_code=e.status_code,
                content={
                    "error": e.detail,
                    "request_id": request_id,
                    "timestamp": datetime.now().isoformat()
                },
                headers={"X-Request-ID": request_id}
            )
            
        except Exception as e:
            # Handle unexpected exceptions
            processing_time = (time.time() - start_time) * 1000
            logger.error(f"Unexpected error in ML middleware: {str(e)}")
            
            self.monitoring.record_request(
                endpoint=request.url.path,
                method=request.method,
                response_time=processing_time,
                status_code=500
            )
            
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Internal server error",
                    "request_id": request_id,
                    "timestamp": datetime.now().isoformat()
                },
                headers={"X-Request-ID": request_id}
            )
        
        finally:
            # Cleanup
            if request_id in self.active_requests:
                del self.active_requests[request_id]
    
    async def _authenticate_request(self, request: Request):
        """Authenticate incoming request"""
        # Skip authentication for certain endpoints
        public_endpoints = ["/docs", "/redoc", "/openapi.json", "/health"]
        if request.url.path in public_endpoints:
            return
        
        # Get token from header
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Missing or invalid authorization header")
        
        token = auth_header.split(" ")[1]
        
        # Verify token
        try:
            payload = self.auth.verify_token(token)
            request.state.user_id = payload.get("user_id")
            request.state.user_roles = payload.get("roles", [])
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            raise HTTPException(status_code=401, detail="Authentication failed")
    
    async def _rate_limit_request(self, request: Request):
        """Apply rate limiting to request"""
        if not self.redis_client:
            return  # Skip rate limiting if Redis is not available
        
        # Get client identifier
        client_id = request.state.user_id or request.client.host
        
        # Check rate limit
        is_allowed = await self.rate_limiter.is_allowed(client_id)
        if not is_allowed:
            raise HTTPException(
                status_code=429, 
                detail="Rate limit exceeded. Please try again later."
            )
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get middleware health status"""
        return {
            "redis_connected": self.redis_client is not None,
            "active_requests": len(self.active_requests),
            "metrics": self.monitoring.get_metrics()
        }
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get detailed metrics"""
        return {
            "request_counts": dict(self.monitoring.request_counts),
            "response_times": {k: {
                "avg": sum(v) / len(v) if v else 0,
                "min": min(v) if v else 0,
                "max": max(v) if v else 0,
                "count": len(v)
            } for k, v in self.monitoring.response_times.items()},
            "error_counts": dict(self.monitoring.error_counts),
            "active_requests": len(self.active_requests)
        }

class ModelCacheMiddleware:
    """Model prediction caching middleware"""
    
    def __init__(self, redis_client, cache_ttl: int = 300):
        self.redis_client = redis_client
        self.cache_ttl = cache_ttl
    
    async def get_cached_prediction(self, cache_key: str) -> Dict[str, Any]:
        """Get cached prediction"""
        if not self.redis_client:
            return None
        
        try:
            cached = self.redis_client.get(cache_key)
            if cached:
                return json.loads(cached)
        except Exception as e:
            logger.error(f"Cache retrieval error: {str(e)}")
        
        return None
    
    async def cache_prediction(self, cache_key: str, prediction: Dict[str, Any]):
        """Cache prediction result"""
        if not self.redis_client:
            return
        
        try:
            self.redis_client.setex(
                cache_key, 
                self.cache_ttl, 
                json.dumps(prediction, default=str)
            )
        except Exception as e:
            logger.error(f"Cache storage error: {str(e)}")
    
    def generate_cache_key(self, model_name: str, data: str, user_id: str = None) -> str:
        """Generate cache key for prediction"""
        import hashlib
        
        key_data = f"{model_name}:{data}:{user_id or 'anonymous'}"
        return f"prediction:{hashlib.md5(key_data.encode()).hexdigest()}"

class MLRequestLoggingMiddleware:
    """Request logging middleware for ML endpoints"""
    
    def __init__(self, log_level: str = "INFO"):
        self.logger = logging.getLogger("ml_request_logger")
        self.log_level = getattr(logging, log_level.upper())
    
    async def __call__(self, request: Request, call_next: Callable) -> Response:
        """Log request details"""
        start_time = time.time()
        
        # Log request
        self.logger.log(self.log_level, f"ML Request started: {request.method} {request.url.path}")
        
        # Process request
        response = await call_next(request)
        
        # Log response
        processing_time = (time.time() - start_time) * 1000
        self.logger.log(
            self.log_level,
            f"ML Request completed: {request.method} {request.url.path} "
            f"Status: {response.status_code} Time: {processing_time:.2f}ms"
        )
        
        return response

# Factory function to create ML middleware stack
def create_ml_middleware_stack(
    redis_url: str = "redis://localhost:6379",
    jwt_secret: str = os.getenv("JWT_SECRET_KEY", ""),
    rate_limit_requests: int = 100,
    rate_limit_window: int = 60,
    enable_caching: bool = True,
    enable_logging: bool = True
) -> list:
    """Create a complete middleware stack for ML API"""
    
    middlewares = []
    
    # Main ML middleware
    ml_middleware = MLMiddleware(
        redis_url=redis_url,
        jwt_secret=jwt_secret,
        rate_limit_requests=rate_limit_requests,
        rate_limit_window=rate_limit_window
    )
    middlewares.append(ml_middleware)
    
    # Caching middleware
    if enable_caching:
        try:
            redis_client = redis.from_url(redis_url, decode_responses=True)
            cache_middleware = ModelCacheMiddleware(redis_client)
            middlewares.append(cache_middleware)
        except Exception as e:
            logger.warning(f"Failed to initialize caching middleware: {str(e)}")
    
    # Logging middleware
    if enable_logging:
        logging_middleware = MLRequestLoggingMiddleware()
        middlewares.append(logging_middleware)
    
    return middlewares
