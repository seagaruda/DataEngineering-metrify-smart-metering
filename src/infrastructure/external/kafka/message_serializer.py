"""
Kafka Message Serializer
Handles message serialization and deserialization for Kafka
"""

import json
import avro.schema
import avro.io
from typing import Any, Dict, Optional, Union, List
from datetime import datetime
from io import BytesIO
import logging

from ....core.exceptions.domain_exceptions import InfrastructureError

logger = logging.getLogger(__name__)


class MessageSerializer:
    """
    Kafka Message Serializer
    
    Handles serialization and deserialization of messages for Kafka
    with support for multiple formats (JSON, Avro)
    """
    
    def __init__(self, default_format: str = "json"):
        self.default_format = default_format
        self._avro_schemas = {}
        self._initialize_avro_schemas()
    
    def _initialize_avro_schemas(self) -> None:
        """Initialize Avro schemas for different message types"""
        try:
            # Meter reading schema
            self._avro_schemas["meter_reading"] = avro.schema.parse(json.dumps({
                "type": "record",
                "name": "MeterReading",
                "fields": [
                    {"name": "meter_id", "type": "string"},
                    {"name": "timestamp", "type": "long"},
                    {"name": "voltage", "type": "double"},
                    {"name": "current", "type": "double"},
                    {"name": "power_factor", "type": "double"},
                    {"name": "frequency", "type": "double"},
                    {"name": "active_power", "type": "double"},
                    {"name": "reactive_power", "type": "double"},
                    {"name": "apparent_power", "type": "double"},
                    {"name": "data_quality_score", "type": "double"},
                    {"name": "is_anomaly", "type": "boolean"},
                    {"name": "anomaly_type", "type": ["null", "string"], "default": None}
                ]
            }))
            
            # Grid status schema
            self._avro_schemas["grid_status"] = avro.schema.parse(json.dumps({
                "type": "record",
                "name": "GridStatus",
                "fields": [
                    {"name": "operator_id", "type": "string"},
                    {"name": "timestamp", "type": "long"},
                    {"name": "voltage_level", "type": "double"},
                    {"name": "frequency", "type": "double"},
                    {"name": "load_percentage", "type": "double"},
                    {"name": "stability_score", "type": "double"},
                    {"name": "power_quality_score", "type": "double"},
                    {"name": "data_quality_score", "type": "double"},
                    {"name": "is_anomaly", "type": "boolean"},
                    {"name": "anomaly_type", "type": ["null", "string"], "default": None}
                ]
            }))
            
            # Weather observation schema
            self._avro_schemas["weather_observation"] = avro.schema.parse(json.dumps({
                "type": "record",
                "name": "WeatherObservation",
                "fields": [
                    {"name": "station_id", "type": "string"},
                    {"name": "timestamp", "type": "long"},
                    {"name": "temperature_celsius", "type": "double"},
                    {"name": "humidity_percent", "type": "double"},
                    {"name": "pressure_hpa", "type": "double"},
                    {"name": "wind_speed_ms", "type": "double"},
                    {"name": "wind_direction_degrees", "type": "double"},
                    {"name": "cloud_cover_percent", "type": "double"},
                    {"name": "visibility_km", "type": "double"},
                    {"name": "uv_index", "type": ["null", "double"], "default": None},
                    {"name": "precipitation_mm", "type": ["null", "double"], "default": None},
                    {"name": "data_quality_score", "type": "double"},
                    {"name": "is_anomaly", "type": "boolean"},
                    {"name": "anomaly_type", "type": ["null", "string"], "default": None}
                ]
            }))
            
            logger.info("Avro schemas initialized")
            
        except Exception as e:
            logger.warning(f"Failed to initialize Avro schemas: {str(e)}")
    
    def serialize(
        self,
        data: Any,
        format_type: Optional[str] = None,
        schema_name: Optional[str] = None
    ) -> bytes:
        """
        Serialize data to bytes
        
        Args:
            data: Data to serialize
            format_type: Serialization format (json, avro)
            schema_name: Avro schema name (required for Avro format)
            
        Returns:
            Serialized data as bytes
        """
        format_type = format_type or self.default_format
        
        try:
            if format_type == "json":
                return self._serialize_json(data)
            elif format_type == "avro":
                return self._serialize_avro(data, schema_name)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
                
        except Exception as e:
            logger.error(f"Error serializing data: {str(e)}")
            raise InfrastructureError(f"Failed to serialize data: {str(e)}", service="kafka")
    
    def deserialize(
        self,
        data: bytes,
        format_type: Optional[str] = None,
        schema_name: Optional[str] = None
    ) -> Any:
        """
        Deserialize bytes to data
        
        Args:
            data: Serialized data as bytes
            format_type: Serialization format (json, avro)
            schema_name: Avro schema name (required for Avro format)
            
        Returns:
            Deserialized data
        """
        format_type = format_type or self.default_format
        
        try:
            if format_type == "json":
                return self._deserialize_json(data)
            elif format_type == "avro":
                return self._deserialize_avro(data, schema_name)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
                
        except Exception as e:
            logger.error(f"Error deserializing data: {str(e)}")
            raise InfrastructureError(f"Failed to deserialize data: {str(e)}", service="kafka")
    
    def _serialize_json(self, data: Any) -> bytes:
        """Serialize data to JSON"""
        # Handle datetime objects
        if isinstance(data, dict):
            data = self._convert_datetime_to_iso(data)
        
        return json.dumps(data, default=str).encode('utf-8')
    
    def _deserialize_json(self, data: bytes) -> Any:
        """Deserialize data from JSON"""
        return json.loads(data.decode('utf-8'))
    
    def _serialize_avro(self, data: Any, schema_name: Optional[str] = None) -> bytes:
        """Serialize data to Avro"""
        if not schema_name or schema_name not in self._avro_schemas:
            raise ValueError(f"Avro schema {schema_name} not found")
        
        schema = self._avro_schemas[schema_name]
        
        # Convert datetime to timestamp
        if isinstance(data, dict):
            data = self._convert_datetime_to_timestamp(data)
        
        bytes_writer = BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer = avro.io.DatumWriter(schema)
        writer.write(data, encoder)
        
        return bytes_writer.getvalue()
    
    def _deserialize_avro(self, data: bytes, schema_name: Optional[str] = None) -> Any:
        """Deserialize data from Avro"""
        if not schema_name or schema_name not in self._avro_schemas:
            raise ValueError(f"Avro schema {schema_name} not found")
        
        schema = self._avro_schemas[schema_name]
        
        bytes_reader = BytesIO(data)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        
        return reader.read(decoder)
    
    def _serialize_pickle(self, data: Any) -> bytes:
        """Serialize data to Pickle

        .. deprecated::
            Pickle serialization is disabled for security reasons (pickle
            deserialization can lead to arbitrary code execution). This method
            is retained only for API compatibility and always raises.
        """
        raise NotImplementedError(
            "Pickle serialization is disabled for security reasons "
            "(pickle deserialization enables remote code execution). "
            "Use JSON or Avro instead."
        )

    def _deserialize_pickle(self, data: bytes) -> Any:
        """Deserialize data from Pickle

        .. deprecated::
            Pickle deserialization is disabled for security reasons
            (``pickle.loads`` can execute arbitrary code). This method is
            retained only for API compatibility and always raises.
        """
        raise NotImplementedError(
            "Pickle deserialization is disabled for security reasons "
            "(pickle.loads enables remote code execution). "
            "Use JSON or Avro instead."
        )
    
    def _convert_datetime_to_iso(self, data: Any) -> Any:
        """Convert datetime objects to ISO format strings"""
        if isinstance(data, dict):
            return {k: self._convert_datetime_to_iso(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._convert_datetime_to_iso(item) for item in data]
        elif isinstance(data, datetime):
            return data.isoformat()
        else:
            return data
    
    def _convert_datetime_to_timestamp(self, data: Any) -> Any:
        """Convert datetime objects to Unix timestamps"""
        if isinstance(data, dict):
            return {k: self._convert_datetime_to_timestamp(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._convert_datetime_to_timestamp(item) for item in data]
        elif isinstance(data, datetime):
            return int(data.timestamp() * 1000)  # Convert to milliseconds
        else:
            return data
    
    def create_envelope(
        self,
        payload: Any,
        message_type: str,
        version: str = "1.0",
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a message envelope with metadata
        
        Args:
            payload: Message payload
            message_type: Type of message
            version: Message version
            metadata: Additional metadata
            
        Returns:
            Message envelope dictionary
        """
        envelope = {
            "payload": payload,
            "metadata": {
                "message_type": message_type,
                "version": version,
                "timestamp": datetime.utcnow().isoformat(),
                "producer": "metrify-smart-metering",
                **(metadata or {})
            }
        }
        
        return envelope
    
    def extract_envelope(self, data: Any) -> tuple[Any, Dict[str, Any]]:
        """
        Extract payload and metadata from message envelope
        
        Args:
            data: Message envelope data
            
        Returns:
            Tuple of (payload, metadata)
        """
        if isinstance(data, dict) and "payload" in data and "metadata" in data:
            return data["payload"], data["metadata"]
        else:
            return data, {}
    
    def get_supported_formats(self) -> List[str]:
        """Get list of supported serialization formats"""
        return ["json", "avro"]
    
    def get_available_schemas(self) -> List[str]:
        """Get list of available Avro schemas"""
        return list(self._avro_schemas.keys())
    
    def add_avro_schema(self, schema_name: str, schema_definition: Dict[str, Any]) -> None:
        """
        Add a new Avro schema
        
        Args:
            schema_name: Name of the schema
            schema_definition: Avro schema definition
        """
        try:
            self._avro_schemas[schema_name] = avro.schema.parse(json.dumps(schema_definition))
            logger.info(f"Added Avro schema: {schema_name}")
        except Exception as e:
            logger.error(f"Error adding Avro schema: {str(e)}")
            raise InfrastructureError(f"Failed to add Avro schema: {str(e)}", service="kafka")
