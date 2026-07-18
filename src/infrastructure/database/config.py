"""
Database Configuration
Database connection and configuration management
"""

import os
from typing import Optional
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager

from .base import Base
# Import all models to register them with the shared Base
from .models.smart_meter_model import SmartMeterModel, SmartMeterReadingModel, MeterEventModel
from .models.grid_operator_model import GridOperatorModel, GridStatusModel, GridEventModel
from .models.weather_station_model import WeatherStationModel, WeatherObservationModel, WeatherEventModel


class DatabaseConfig:
    """Database configuration and connection management"""
    
    def __init__(
        self,
        database_url: Optional[str] = None,
        pool_size: int = 10,
        max_overflow: int = 20,
        pool_timeout: int = 30,
        pool_recycle: int = 3600,
        echo: bool = False
    ):
        self.database_url = database_url or self._get_database_url()
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle
        self.echo = echo
        
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
    
    def _get_database_url(self) -> str:
        """Get database URL from environment variables"""
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME", "metrify_smart_metering")
        db_user = os.getenv("DB_USER", "postgres")
        db_password = os.getenv("DB_PASSWORD", "password")
        
        # URL-encode credentials so special characters do not corrupt the
        # connection string, and so the real password is actually used.
        return f"postgresql://{quote_plus(db_user)}:{quote_plus(db_password)}@{db_host}:{db_port}/{db_name}"
    
    @property
    def engine(self) -> Engine:
        """Get database engine"""
        if self._engine is None:
            self._engine = create_engine(
                self.database_url,
                poolclass=QueuePool,
                pool_size=self.pool_size,
                max_overflow=self.max_overflow,
                pool_timeout=self.pool_timeout,
                pool_recycle=self.pool_recycle,
                echo=self.echo,
                connect_args={
                    "options": "-c timezone=utc"
                }
            )
        return self._engine
    
    @property
    def session_factory(self) -> sessionmaker:
        """Get session factory"""
        if self._session_factory is None:
            self._session_factory = sessionmaker(
                bind=self.engine,
                autocommit=False,
                autoflush=False
            )
        return self._session_factory
    
    def create_tables(self) -> None:
        """Create all database tables"""
        # Create tables for all models using the shared base
        Base.metadata.create_all(self.engine)
    
    def drop_tables(self) -> None:
        """Drop all database tables"""
        # Drop tables for all models using the shared base
        Base.metadata.drop_all(self.engine)
    
    @contextmanager
    def get_session(self) -> Session:
        """Get database session context manager"""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def get_session(self) -> Session:
        """Get database session"""
        return self.session_factory()
    
    def close(self) -> None:
        """Close database connections"""
        if self._engine:
            self._engine.dispose()
            self._engine = None
        self._session_factory = None


# Global database configuration instance
db_config = DatabaseConfig()


def get_database_config() -> DatabaseConfig:
    """Get database configuration instance"""
    return db_config


def get_session() -> Session:
    """Get database session"""
    return db_config.get_session()


@contextmanager
def get_session_context() -> Session:
    """Get database session context manager"""
    with db_config.get_session() as session:
        yield session
