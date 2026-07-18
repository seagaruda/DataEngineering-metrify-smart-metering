"""
Snowflake Client Implementation
Handles data warehouse operations and analytics queries
"""

import asyncio
import json
import re
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

try:
    import snowflake.connector
    from snowflake.connector import DictCursor
except ImportError:
    snowflake = None
    DictCursor = None

from ....core.exceptions.domain_exceptions import InfrastructureError

logger = logging.getLogger(__name__)

# Whitelist pattern for SQL identifiers (database/schema/table/column names).
# Only simple identifiers are allowed to prevent SQL injection through
# f-string interpolation of object names.
_IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

# Allowed file formats for CREATE STAGE / COPY INTO operations.
_ALLOWED_FILE_FORMATS = {"JSON", "CSV", "PARQUET", "AVRO", "ORC", "XML"}


def _validate_identifier(identifier: str, label: str = "identifier") -> None:
    """Validate a SQL identifier against a strict whitelist.

    Only identifiers matching ``^[a-zA-Z_][a-zA-Z0-9_]*$`` are permitted.
    Any identifier containing quoting, dots, semicolons, spaces, or other
    metacharacters is rejected to prevent SQL injection.

    Args:
        identifier: The identifier to validate.
        label: Human-readable label used in error messages (e.g. "table").

    Raises:
        ValueError: If the identifier is empty or contains disallowed characters.
    """
    if not isinstance(identifier, str) or not identifier or not _IDENTIFIER_PATTERN.match(identifier):
        raise ValueError(
            f"Invalid {label}: {identifier!r}. Only alphanumeric characters and "
            "underscores are allowed, and the identifier must start with a letter "
            "or underscore."
        )


class SnowflakeClient:
    """
    Snowflake Client for data warehouse operations
    
    Handles connection management, query execution, and data analytics
    """
    
    def __init__(self, config):
        """
        Initialize Snowflake client with configuration
        
        Args:
            config: SnowflakeConfig object containing Snowflake connection details
        """
        if snowflake is None:
            raise InfrastructureError("Snowflake connector not installed", service="snowflake")
        
        self.config = config
        self.account = config.account
        self.user = config.user
        self.password = config.password
        self.warehouse = config.warehouse
        self.database = config.database
        self.schema = config.schema
        self.role = config.role
        
        self._connection = None
        self._is_connected = False
    
    async def connect(self) -> None:
        """Connect to Snowflake and ensure infrastructure exists"""
        try:
            # First connect without specifying database/schema to create them if needed
            self._connection = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                role=self.role
            )
            
            # Ensure all required Snowflake infrastructure exists
            await self._ensure_infrastructure_exists()
            
            # Reconnect with the specific database and schema
            self._connection.close()
            self._connection = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                role=self.role
            )
            
            self._is_connected = True
            logger.info(f"Connected to Snowflake: {self.account}.{self.database}.{self.schema}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise InfrastructureError(f"Failed to connect to Snowflake: {str(e)}", service="snowflake")
    
    async def disconnect(self) -> None:
        """Disconnect from Snowflake"""
        if self._connection:
            self._connection.close()
            self._connection = None
            self._is_connected = False
            logger.info("Disconnected from Snowflake")
    
    async def _ensure_infrastructure_exists(self) -> None:
        """Ensure all required Snowflake infrastructure exists (warehouse, database, schema, tables)"""
        try:
            cursor = self._connection.cursor()
            
            # Ensure warehouse exists
            await self._ensure_warehouse_exists(cursor)
            
            # Ensure database exists
            await self._ensure_database_exists(cursor)
            
            # Ensure schema exists
            await self._ensure_schema_exists(cursor)
            
            # Ensure core tables exist
            await self._ensure_core_tables_exist(cursor)
            
            cursor.close()
            logger.info("Snowflake infrastructure setup completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to ensure Snowflake infrastructure exists: {str(e)}")
            raise InfrastructureError(f"Failed to setup Snowflake infrastructure: {str(e)}", service="snowflake")
    
    async def _ensure_warehouse_exists(self, cursor) -> None:
        """Ensure the warehouse exists, create it if it doesn't"""
        try:
            # Check if warehouse exists
            cursor.execute(f"SHOW WAREHOUSES LIKE '{self.warehouse}'")
            result = cursor.fetchone()
            
            if not result:
                # Warehouse doesn't exist, create it
                logger.info(f"Creating Snowflake warehouse: {self.warehouse}")
                create_warehouse_sql = f"""
                CREATE WAREHOUSE IF NOT EXISTS {self.warehouse}
                WITH 
                    WAREHOUSE_SIZE = 'XSMALL'
                    AUTO_SUSPEND = 60
                    AUTO_RESUME = TRUE
                    INITIALLY_SUSPENDED = TRUE
                    COMMENT = 'Metrify Smart Metering Data Warehouse'
                """
                cursor.execute(create_warehouse_sql)
                logger.info(f"Created warehouse: {self.warehouse}")
            else:
                logger.debug(f"Warehouse {self.warehouse} already exists")
                
        except Exception as e:
            logger.error(f"Failed to ensure warehouse exists: {str(e)}")
            raise
    
    async def _ensure_database_exists(self, cursor) -> None:
        """Ensure the database exists, create it if it doesn't"""
        try:
            # Check if database exists
            cursor.execute(f"SHOW DATABASES LIKE '{self.database}'")
            result = cursor.fetchone()
            
            if not result:
                # Database doesn't exist, create it
                logger.info(f"Creating Snowflake database: {self.database}")
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
                logger.info(f"Created database: {self.database}")
            else:
                logger.debug(f"Database {self.database} already exists")
                
        except Exception as e:
            logger.error(f"Failed to ensure database exists: {str(e)}")
            raise
    
    async def _ensure_schema_exists(self, cursor) -> None:
        """Ensure the schema exists, create it if it doesn't"""
        try:
            # Use the database first
            cursor.execute(f"USE DATABASE {self.database}")
            
            # Check if schema exists
            cursor.execute(f"SHOW SCHEMAS LIKE '{self.schema}'")
            result = cursor.fetchone()
            
            if not result:
                # Schema doesn't exist, create it
                logger.info(f"Creating Snowflake schema: {self.database}.{self.schema}")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
                logger.info(f"Created schema: {self.database}.{self.schema}")
            else:
                logger.debug(f"Schema {self.database}.{self.schema} already exists")
                
        except Exception as e:
            logger.error(f"Failed to ensure schema exists: {str(e)}")
            raise
    
    async def _ensure_core_tables_exist(self, cursor) -> None:
        """Ensure core tables exist, create them if they don't"""
        try:
            # Use the database and schema
            cursor.execute(f"USE DATABASE {self.database}")
            cursor.execute(f"USE SCHEMA {self.schema}")
            
            # Define core tables for the Metrify system
            core_tables = {
                'smart_meters': """
                    CREATE TABLE IF NOT EXISTS smart_meters (
                        id VARCHAR(36) PRIMARY KEY,
                        meter_id VARCHAR(255) UNIQUE NOT NULL,
                        latitude DECIMAL(10, 8) NOT NULL,
                        longitude DECIMAL(11, 8) NOT NULL,
                        address TEXT NOT NULL,
                        manufacturer VARCHAR(255) NOT NULL,
                        model VARCHAR(255) NOT NULL,
                        installation_date TIMESTAMP_NTZ NOT NULL,
                        status VARCHAR(50) NOT NULL DEFAULT 'ACTIVE',
                        quality_tier VARCHAR(50) NOT NULL DEFAULT 'UNKNOWN',
                        installed_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        last_reading_at TIMESTAMP_NTZ,
                        created_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        firmware_version VARCHAR(50) NOT NULL DEFAULT '1.0.0',
                        metadata VARIANT,
                        version INTEGER NOT NULL DEFAULT 0
                    )
                """,
                'meter_readings': """
                    CREATE TABLE IF NOT EXISTS meter_readings (
                        id VARCHAR(36) PRIMARY KEY,
                        meter_id VARCHAR(255) NOT NULL,
                        timestamp TIMESTAMP_NTZ NOT NULL,
                        voltage DECIMAL(10, 3) NOT NULL,
                        "current" DECIMAL(10, 3) NOT NULL,
                        power_factor DECIMAL(5, 3) NOT NULL,
                        frequency DECIMAL(5, 2) NOT NULL,
                        active_power DECIMAL(12, 3) NOT NULL,
                        reactive_power DECIMAL(12, 3) NOT NULL,
                        apparent_power DECIMAL(12, 3) NOT NULL,
                        data_quality_score DECIMAL(3, 2) NOT NULL DEFAULT 1.0,
                        is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
                        anomaly_type VARCHAR(100),
                        created_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (meter_id) REFERENCES smart_meters(meter_id)
                    )
                """,
                'grid_operators': """
                    CREATE TABLE IF NOT EXISTS grid_operators (
                        id VARCHAR(36) PRIMARY KEY,
                        operator_id VARCHAR(255) UNIQUE NOT NULL,
                        name VARCHAR(255) NOT NULL,
                        operator_type VARCHAR(100) NOT NULL,
                        latitude DECIMAL(10, 8) NOT NULL,
                        longitude DECIMAL(11, 8) NOT NULL,
                        address TEXT NOT NULL,
                        contact_email VARCHAR(255),
                        contact_phone VARCHAR(50),
                        website VARCHAR(255),
                        status VARCHAR(50) NOT NULL DEFAULT 'ACTIVE',
                        created_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        grid_capacity_mw DECIMAL(12, 2),
                        voltage_level_kv DECIMAL(8, 2),
                        coverage_area_km2 DECIMAL(12, 2),
                        operator_metadata VARIANT,
                        version INTEGER NOT NULL DEFAULT 0
                    )
                """,
                'weather_stations': """
                    CREATE TABLE IF NOT EXISTS weather_stations (
                        id VARCHAR(36) PRIMARY KEY,
                        station_id VARCHAR(255) UNIQUE NOT NULL,
                        name VARCHAR(255) NOT NULL,
                        station_type VARCHAR(100) NOT NULL,
                        latitude DECIMAL(10, 8) NOT NULL,
                        longitude DECIMAL(11, 8) NOT NULL,
                        address TEXT NOT NULL,
                        operator VARCHAR(255) NOT NULL,
                        contact_email VARCHAR(255),
                        contact_phone VARCHAR(50),
                        status VARCHAR(50) NOT NULL DEFAULT 'ACTIVE',
                        created_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        total_observations INTEGER NOT NULL DEFAULT 0,
                        average_quality_score DECIMAL(3, 2) NOT NULL DEFAULT 1.0,
                        last_observation_at TIMESTAMP_NTZ,
                        version INTEGER NOT NULL DEFAULT 0
                    )
                """
            }
            
            # Create each table if it doesn't exist
            for table_name, create_sql in core_tables.items():
                try:
                    cursor.execute(create_sql)
                    logger.debug(f"Ensured table {table_name} exists")
                except Exception as e:
                    logger.warning(f"Failed to create table {table_name}: {str(e)}")
                    # Continue with other tables even if one fails
                    
            logger.info("Core Snowflake tables setup completed")
            
        except Exception as e:
            logger.error(f"Failed to ensure core tables exist: {str(e)}")
            raise
    
    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Query results as list of dictionaries
        """
        if not self._is_connected or not self._connection:
            await self.connect()
        
        try:
            cursor = self._connection.cursor(DictCursor)
            cursor.execute(query, params or {})
            results = cursor.fetchall()
            cursor.close()
            
            logger.debug(f"Executed query, returned {len(results)} rows")
            return results
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise InfrastructureError(f"Failed to execute query: {str(e)}", service="snowflake")
    
    async def execute_ddl(self, ddl_statement: str) -> None:
        """
        Execute a DDL statement (CREATE, ALTER, DROP)
        
        Args:
            ddl_statement: DDL statement to execute
        """
        if not self._is_connected or not self._connection:
            await self.connect()
        
        try:
            cursor = self._connection.cursor()
            cursor.execute(ddl_statement)
            cursor.close()
            
            logger.info(f"Executed DDL: {ddl_statement[:100]}...")
            
        except Exception as e:
            logger.error(f"Error executing DDL: {str(e)}")
            raise InfrastructureError(f"Failed to execute DDL: {str(e)}", service="snowflake")
    
    async def create_table_from_s3(
        self,
        table_name: str,
        s3_path: str,
        file_format: str = "JSON",
        columns: Optional[List[Dict[str, str]]] = None
    ) -> None:
        """
        Create a table from S3 data
        
        Args:
            table_name: Name of the table to create
            s3_path: S3 path to the data files
            file_format: File format (JSON, CSV, PARQUET)
            columns: Column definitions
        """
        try:
            # Validate identifiers to prevent SQL injection. Table and column
            # names are interpolated into DDL via f-strings, so they must be
            # strictly whitelisted.
            _validate_identifier(table_name, "table_name")
            if columns:
                for col in columns:
                    _validate_identifier(col.get("name", ""), "column name")
            # Validate file format against a fixed whitelist
            normalized_format = (file_format or "").upper()
            if normalized_format not in _ALLOWED_FILE_FORMATS:
                raise ValueError(
                    f"Invalid file_format: {file_format!r}. Allowed formats: "
                    f"{sorted(_ALLOWED_FILE_FORMATS)}"
                )

            if columns:
                column_defs = ", ".join([f"{col['name']} {col['type']}" for col in columns])
                create_sql = f"""
                CREATE OR REPLACE TABLE {table_name} (
                    {column_defs}
                )
                """
            else:
                create_sql = f"CREATE OR REPLACE TABLE {table_name} (data VARIANT)"
            
            await self.execute_ddl(create_sql)
            
            # Create stage for S3 data
            stage_name = f"{table_name}_stage"
            create_stage_sql = f"""
            CREATE OR REPLACE STAGE {stage_name}
            URL = '{s3_path}'
            FILE_FORMAT = {normalized_format}
            """
            await self.execute_ddl(create_stage_sql)
            
            # Copy data from stage to table
            copy_sql = f"""
            COPY INTO {table_name}
            FROM @{stage_name}
            FILE_FORMAT = {normalized_format}
            """
            await self.execute_ddl(copy_sql)
            
            logger.info(f"Created table {table_name} from S3 data")
            
        except Exception as e:
            logger.error(f"Error creating table from S3: {str(e)}")
            raise InfrastructureError(f"Failed to create table from S3: {str(e)}", service="snowflake")
    
    async def get_meter_analytics(
        self,
        start_date: datetime,
        end_date: datetime,
        meter_ids: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get smart meter analytics data"""
        query = """
        SELECT 
            meter_id,
            DATE(timestamp) as reading_date,
            COUNT(*) as reading_count,
            AVG(voltage) as avg_voltage,
            AVG(current) as avg_current,
            AVG(active_power) as avg_active_power,
            AVG(data_quality_score) as avg_quality_score,
            SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count
        FROM meter_readings
        WHERE timestamp BETWEEN %(start_date)s AND %(end_date)s
        """
        
        if meter_ids:
            query += " AND meter_id IN %(meter_ids)s"
        
        query += """
        GROUP BY meter_id, DATE(timestamp)
        ORDER BY meter_id, reading_date
        """
        
        params = {
            'start_date': start_date,
            'end_date': end_date,
            'meter_ids': tuple(meter_ids) if meter_ids else None
        }
        
        return await self.execute_query(query, params)
    
    async def get_grid_analytics(
        self,
        start_date: datetime,
        end_date: datetime,
        operator_ids: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get grid operator analytics data"""
        query = """
        SELECT 
            operator_id,
            DATE(timestamp) as status_date,
            COUNT(*) as status_count,
            AVG(voltage_level) as avg_voltage_level,
            AVG(frequency) as avg_frequency,
            AVG(load_percentage) as avg_load_percentage,
            AVG(stability_score) as avg_stability_score,
            AVG(power_quality_score) as avg_power_quality_score,
            SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count
        FROM grid_statuses
        WHERE timestamp BETWEEN %(start_date)s AND %(end_date)s
        """
        
        if operator_ids:
            query += " AND operator_id IN %(operator_ids)s"
        
        query += """
        GROUP BY operator_id, DATE(timestamp)
        ORDER BY operator_id, status_date
        """
        
        params = {
            'start_date': start_date,
            'end_date': end_date,
            'operator_ids': tuple(operator_ids) if operator_ids else None
        }
        
        return await self.execute_query(query, params)
    
    async def get_weather_analytics(
        self,
        start_date: datetime,
        end_date: datetime,
        station_ids: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get weather station analytics data"""
        query = """
        SELECT 
            station_id,
            DATE(timestamp) as observation_date,
            COUNT(*) as observation_count,
            AVG(temperature_celsius) as avg_temperature,
            AVG(humidity_percent) as avg_humidity,
            AVG(pressure_hpa) as avg_pressure,
            AVG(wind_speed_ms) as avg_wind_speed,
            AVG(data_quality_score) as avg_quality_score,
            SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count
        FROM weather_observations
        WHERE timestamp BETWEEN %(start_date)s AND %(end_date)s
        """
        
        if station_ids:
            query += " AND station_id IN %(station_ids)s"
        
        query += """
        GROUP BY station_id, DATE(timestamp)
        ORDER BY station_id, observation_date
        """
        
        params = {
            'start_date': start_date,
            'end_date': end_date,
            'station_ids': tuple(station_ids) if station_ids else None
        }
        
        return await self.execute_query(query, params)
    
    async def get_energy_demand_correlation(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Get energy demand correlation with weather data"""
        query = """
        WITH weather_daily AS (
            SELECT 
                DATE(timestamp) as observation_date,
                AVG(temperature_celsius) as avg_temperature,
                AVG(humidity_percent) as avg_humidity,
                AVG(pressure_hpa) as avg_pressure
            FROM weather_observations
            WHERE timestamp BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY DATE(timestamp)
        ),
        energy_daily AS (
            SELECT 
                DATE(timestamp) as reading_date,
                AVG(active_power) as avg_power_consumption
            FROM meter_readings
            WHERE timestamp BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY DATE(timestamp)
        )
        SELECT 
            w.observation_date,
            w.avg_temperature,
            w.avg_humidity,
            w.avg_pressure,
            e.avg_power_consumption,
            CORR(w.avg_temperature, e.avg_power_consumption) as temp_correlation,
            CORR(w.avg_humidity, e.avg_power_consumption) as humidity_correlation
        FROM weather_daily w
        JOIN energy_daily e ON w.observation_date = e.reading_date
        ORDER BY w.observation_date
        """
        
        params = {
            'start_date': start_date,
            'end_date': end_date
        }
        
        return await self.execute_query(query, params)
    
    def is_connected(self) -> bool:
        """Check if client is connected to Snowflake"""
        return self._is_connected and self._connection is not None
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get Snowflake client metrics"""
        if not self._connection:
            return {"connected": False}
        
        try:
            # Get warehouse status
            warehouse_query = f"SHOW WAREHOUSES LIKE '{self.warehouse}'"
            warehouse_info = await self.execute_query(warehouse_query)
            
            return {
                "connected": self._is_connected,
                "account": self.account,
                "database": self.database,
                "schema": self.schema,
                "warehouse": self.warehouse,
                "warehouse_info": warehouse_info[0] if warehouse_info else {}
            }
        except Exception as e:
            logger.error(f"Failed to get metrics: {str(e)}")
            return {"connected": self._is_connected, "error": str(e)}
    
    async def insert_data(self, database: str, schema: str, table: str, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Insert data into Snowflake table"""
        try:
            # Validate identifiers to prevent SQL injection. database/schema/table
            # and column names are interpolated into the INSERT statement via
            # f-strings, so they must be strictly whitelisted.
            _validate_identifier(database, "database")
            _validate_identifier(schema, "schema")
            _validate_identifier(table, "table")

            await self.connect()
            
            if not data:
                return {"inserted_rows": 0}
            
            # Get column names from first record
            columns = list(data[0].keys())
            # Validate every column name before interpolating into SQL
            for col in columns:
                _validate_identifier(col, "column name")
            placeholders = ", ".join(["%s"] * len(columns))
            column_names = ", ".join(columns)
            
            # Build INSERT query
            query = f"""
            INSERT INTO {database}.{schema}.{table} ({column_names})
            VALUES ({placeholders})
            """
            
            # Prepare data for insertion
            values_list = []
            for record in data:
                values = [record.get(col) for col in columns]
                values_list.append(values)
            
            # Execute batch insert
            cursor = self._connection.cursor()
            cursor.executemany(query, values_list)
            cursor.close()
            
            return {"inserted_rows": len(values_list)}
            
        except Exception as e:
            logger.error(f"Error inserting data to Snowflake: {e}")
            raise InfrastructureError(f"Failed to insert data to Snowflake: {e}")
    
    async def health_check(self) -> bool:
        """Perform health check on Snowflake connection"""
        try:
            await self.connect()
            # Try to execute a simple query
            result = await self.execute_query("SELECT 1")
            return len(result) > 0
        except Exception as e:
            logger.error(f"Snowflake health check failed: {e}")
            return False
