"""
Data Pipeline API Endpoints
REST API endpoints for data upload, ETL orchestration, and pipeline management
"""

import logging
import os
import time
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Form, status
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta
import json
import asyncio
from pydantic import BaseModel

from src.infrastructure.database.config import get_database_config
from src.infrastructure.external.s3.s3_client import S3Client
from src.infrastructure.external.snowflake.snowflake_client import SnowflakeClient
from src.infrastructure.external.airflow.airflow_client import AirflowClient
from src.core.config.config_loader import get_s3_config, get_snowflake_config
from src.infrastructure.database.repositories.smart_meter_repository import SmartMeterRepository
from src.infrastructure.database.repositories.grid_operator_repository import GridOperatorRepository
from src.infrastructure.database.repositories.weather_station_repository import WeatherStationRepository
from src.spark_etl.services.spark_etl_service import spark_etl_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/pipeline", tags=["Data Pipeline"])


def _convert_entity_to_snowflake_data(entity, data_type: str) -> Dict[str, Any]:
    """
    Convert domain entity to Snowflake-compatible flat dictionary
    
    Args:
        entity: Domain entity (SmartMeter, GridOperator, or WeatherStation)
        data_type: Type of data ("smart_meter", "grid_operator", "weather_station")
    
    Returns:
        Dict with flattened data suitable for Snowflake insertion
    """
    if data_type == "smart_meter":
        return {
            "meter_id": entity.meter_id.value,
            "latitude": entity.location.latitude,
            "longitude": entity.location.longitude,
            "address": entity.location.address,
            "manufacturer": entity.specifications.manufacturer,
            "model": entity.specifications.model,
            "installation_date": entity.specifications.installation_date,
            "status": entity.status.value,
            "quality_tier": entity.quality_tier.value,
            "installed_at": datetime.utcnow().isoformat(),
            "firmware_version": getattr(entity, 'firmware_version', '1.0.0'),
            "metadata": getattr(entity, 'metadata', {})
        }
    elif data_type == "grid_operator":
        return {
            "operator_id": entity.operator_id,
            "name": entity.name,
            "operator_type": entity.operator_type.value if hasattr(entity.operator_type, 'value') else str(entity.operator_type),
            "latitude": entity.headquarters.latitude,
            "longitude": entity.headquarters.longitude,
            "address": entity.headquarters.address,
            "contact_email": entity.contact_email,
            "status": entity.status.value,
            "region": getattr(entity, 'region', 'Unknown'),
            "grid_capacity_mw": getattr(entity, 'grid_capacity_mw', None),
            "voltage_level_kv": getattr(entity, 'voltage_level_kv', None),
            "coverage_area_km2": getattr(entity, 'coverage_area_km2', None),
            "metadata": getattr(entity, 'metadata', {})
        }
    elif data_type == "weather_station":
        return {
            "station_id": entity.station_id,
            "name": entity.name,
            "station_type": entity.station_type.value if hasattr(entity.station_type, 'value') else str(entity.station_type),
            "latitude": entity.location.latitude,
            "longitude": entity.location.longitude,
            "address": entity.location.address,
            "operator": entity.operator,
            "status": entity.status.value,
            "elevation_m": getattr(entity, 'elevation_m', None),
            "installation_date": getattr(entity, 'installation_date', None),
            "metadata": getattr(entity, 'metadata', {})
        }
    else:
        raise ValueError(f"Unknown data_type: {data_type}")


class CompleteDataFlowRequest(BaseModel):
    """Request model for complete data flow"""
    data: Dict[str, Any]
    data_type: str
    batch_id: Optional[str] = None
    include_s3: bool = True
    include_snowflake: bool = True
    trigger_etl: bool = False


class SparkETLRequest(BaseModel):
    """Request model for Spark ETL job"""
    data_type: str
    source_path: str
    target_path: str
    output_format: str = "delta"
    data: Optional[Dict[str, Any]] = None
    batch_id: Optional[str] = None


def get_s3_client() -> S3Client:
    """Get S3 client instance"""
    s3_config = get_s3_config()
    return S3Client(s3_config)


def get_snowflake_client() -> SnowflakeClient:
    """Get Snowflake client instance"""
    snowflake_config = get_snowflake_config()
    return SnowflakeClient(snowflake_config)


def get_airflow_client() -> AirflowClient:
    """Get Airflow client instance"""
    from src.core.config.config_loader import get_airflow_config
    airflow_config = get_airflow_config()
    return AirflowClient(
        base_url=airflow_config.base_url,
        username=os.getenv("AIRFLOW_API_USERNAME", ""),
        password=os.getenv("AIRFLOW_API_PASSWORD", ""),
    )


def get_smart_meter_repository():
    """Get smart meter repository instance"""
    db_config = get_database_config()
    session = db_config.session_factory()
    return SmartMeterRepository(session)


def get_grid_operator_repository():
    """Get grid operator repository instance"""
    db_config = get_database_config()
    session = db_config.session_factory()
    return GridOperatorRepository(session)


def get_weather_station_repository():
    """Get weather station repository instance"""
    db_config = get_database_config()
    session = db_config.session_factory()
    return WeatherStationRepository(session)


@router.post("/data-flow/complete", response_model=Dict[str, Any])
async def complete_data_flow(
    request: CompleteDataFlowRequest,
    smart_meter_repo: SmartMeterRepository = Depends(get_smart_meter_repository),
    grid_operator_repo: GridOperatorRepository = Depends(get_grid_operator_repository),
    weather_station_repo: WeatherStationRepository = Depends(get_weather_station_repository),
    s3_client: S3Client = Depends(get_s3_client),
    snowflake_client: SnowflakeClient = Depends(get_snowflake_client),
    airflow_client: AirflowClient = Depends(get_airflow_client)
):
    """
    Complete data flow: Raw Data → PostgreSQL → S3 → Snowflake
    This endpoint demonstrates the proper data flow through all storage systems
    """
    try:
        # Extract parameters from request
        data = request.data
        data_type = request.data_type
        batch_id = request.batch_id
        include_s3 = request.include_s3
        include_snowflake = request.include_snowflake
        trigger_etl = request.trigger_etl
        
        # Validate data type
        allowed_types = ["smart_meter", "grid_operator", "weather_station"]
        if data_type not in allowed_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid data_type. Must be one of: {allowed_types}"
            )
        
        flow_results = {
            "postgresql": {"success": False, "message": "", "record_id": None},
            "s3": {"success": False, "message": "", "s3_key": None},
            "snowflake": {"success": False, "message": "", "records_uploaded": 0},
            "etl_trigger": {"success": False, "message": "", "run_id": None}
        }
        
        # Step 1: Insert into PostgreSQL (Operational Database)
        try:
            if data_type == "smart_meter":
                # Convert dict to SmartMeter entity and save
                from src.core.domain.entities.smart_meter import SmartMeter
                from src.core.domain.enums.meter_status import MeterStatus
                from src.core.domain.enums.quality_tier import QualityTier
                
                # Create proper value objects
                from src.core.domain.value_objects.meter_id import MeterId
                from src.core.domain.value_objects.location import Location
                from src.core.domain.value_objects.meter_specifications import MeterSpecifications, MeterAccuracy, CommunicationProtocol
                
                # Generate proper meter ID format: MET-{REGION}-{TYPE}-{SEQUENCE}
                meter_id_value = data.get("meter_id", f"MET-QUI-E-{int(time.time()) % 1000000:06d}")
                meter_id = MeterId(meter_id_value)
                location = Location(
                    address=data.get("location", "Unknown Address"),
                    latitude=data.get("latitude", 0.0),
                    longitude=data.get("longitude", 0.0)
                )
                specifications = MeterSpecifications(
                    manufacturer=data.get("manufacturer", "Unknown"),
                    model=data.get("model", "Unknown"),
                    serial_number=data.get("serial_number", f"SN-{int(time.time())}"),
                    accuracy_class=MeterAccuracy.CLASS_1,
                    voltage_rating=data.get("voltage_rating", 230.0),
                    current_rating=data.get("current_rating", 100.0),
                    power_rating=data.get("power_rating", 23.0),
                    frequency_rating=data.get("frequency_rating", 50.0),
                    communication_protocol=CommunicationProtocol.DLMS_COSEM
                )
                
                meter = SmartMeter(
                    meter_id=meter_id,
                    location=location,
                    specifications=specifications,
                    status=MeterStatus.ACTIVE,
                    quality_tier=QualityTier.HIGH
                )
                await smart_meter_repo.save(meter)
                saved_meter = meter
                flow_results["postgresql"]["success"] = True
                flow_results["postgresql"]["message"] = "Smart meter saved to PostgreSQL"
                flow_results["postgresql"]["record_id"] = saved_meter.meter_id
                
            elif data_type == "grid_operator":
                from src.core.domain.entities.grid_operator import GridOperator
                from src.core.domain.enums.grid_operator_status import GridOperatorStatus
                
                # Create proper value objects for GridOperator
                from src.core.domain.value_objects.location import Location
                from src.core.domain.entities.grid_operator import GridOperatorType
                
                headquarters = Location(
                    address=data.get("headquarters", "Unknown Headquarters"),
                    latitude=data.get("latitude", 0.0),
                    longitude=data.get("longitude", 0.0)
                )
                
                operator = GridOperator(
                    operator_id=data.get("operator_id", f"GO_{int(time.time())}"),
                    name=data.get("name", "Unknown Operator"),
                    operator_type=GridOperatorType.TRANSMISSION,
                    headquarters=headquarters,
                    coverage_regions=[data.get("region", "Unknown Region")],
                    contact_email=data.get("contact_email", "contact@operator.com"),
                    status=GridOperatorStatus.ACTIVE
                )
                await grid_operator_repo.save(operator)
                saved_operator = operator
                flow_results["postgresql"]["success"] = True
                flow_results["postgresql"]["message"] = "Grid operator saved to PostgreSQL"
                flow_results["postgresql"]["record_id"] = saved_operator.operator_id
                
            elif data_type == "weather_station":
                from src.core.domain.entities.weather_station import WeatherStation
                from src.core.domain.enums.weather_station_status import WeatherStationStatus
                
                # Create proper value objects for WeatherStation
                from src.core.domain.value_objects.location import Location
                from src.core.domain.entities.weather_station import WeatherStationType
                
                location = Location(
                    address=data.get("location", "Unknown Location"),
                    latitude=data.get("latitude", 0.0),
                    longitude=data.get("longitude", 0.0)
                )
                
                station = WeatherStation(
                    station_id=data.get("station_id", f"WS_{int(time.time())}"),
                    name=data.get("name", "Unknown Station"),
                    station_type=WeatherStationType.AUTOMATIC,
                    location=location,
                    operator=data.get("operator", "Unknown"),
                    status=WeatherStationStatus.ACTIVE
                )
                await weather_station_repo.save(station)
                saved_station = station
                flow_results["postgresql"]["success"] = True
                flow_results["postgresql"]["message"] = "Weather station saved to PostgreSQL"
                flow_results["postgresql"]["record_id"] = saved_station.station_id
                
        except Exception as e:
            flow_results["postgresql"]["message"] = f"PostgreSQL error: {str(e)}"
            logger.error(f"PostgreSQL insertion failed: {str(e)}")
        
        # Step 2: Upload to S3 (Data Lake)
        if include_s3 and flow_results["postgresql"]["success"]:
            try:
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                s3_key = f"processed/{data_type}/{batch_id or 'no_batch'}/{timestamp}_{data_type}_data.json"
                
                # Convert data to JSON
                json_content = json.dumps(data, indent=2).encode('utf-8')
                
                await s3_client.upload_data(
                    data=json_content,
                    s3_key=s3_key,
                    content_type="application/json"
                )
                
                flow_results["s3"]["success"] = True
                flow_results["s3"]["message"] = "Data uploaded to S3 data lake"
                flow_results["s3"]["s3_key"] = s3_key
                
            except Exception as e:
                flow_results["s3"]["message"] = f"S3 error: {str(e)}"
                logger.error(f"S3 upload failed: {str(e)}")
        
        # Step 3: Upload to Snowflake (Data Warehouse)
        if include_snowflake and flow_results["postgresql"]["success"]:
            try:
                table_mapping = {
                    "smart_meter": "smart_meters",
                    "grid_operator": "grid_operators",
                    "weather_station": "weather_stations"
                }
                
                table_name = table_mapping.get(data_type)
                if table_name:
                    # Convert domain entity to Snowflake-compatible data
                    snowflake_data = _convert_entity_to_snowflake_data(saved_entity, data_type)
                    await snowflake_client.insert_data(
                        database="METRIFY_ANALYTICS",
                        schema="RAW",
                        table=table_name,
                        data=[snowflake_data]
                    )
                    
                    flow_results["snowflake"]["success"] = True
                    flow_results["snowflake"]["message"] = "Data uploaded to Snowflake"
                    flow_results["snowflake"]["records_uploaded"] = 1
                    
            except Exception as e:
                flow_results["snowflake"]["message"] = f"Snowflake error: {str(e)}"
                logger.error(f"Snowflake upload failed: {str(e)}")
        
        # Step 4: Trigger ETL Pipeline (Optional)
        if trigger_etl and flow_results["postgresql"]["success"]:
            try:
                dag_mapping = {
                    "smart_meter": "smart_meter_data_pipeline",
                    "grid_operator": "grid_operator_pipeline",
                    "weather_station": "weather_data_pipeline"
                }
                
                dag_id = dag_mapping.get(data_type)
                if dag_id:
                    async with airflow_client:
                        dag_run = await airflow_client.trigger_dag(
                            dag_id=dag_id,
                            conf={"batch_id": batch_id} if batch_id else {}
                        )
                        
                        flow_results["etl_trigger"]["success"] = True
                        flow_results["etl_trigger"]["message"] = "ETL pipeline triggered"
                        flow_results["etl_trigger"]["run_id"] = dag_run.run_id
                    
            except Exception as e:
                flow_results["etl_trigger"]["message"] = f"ETL trigger error: {str(e)}"
                logger.error(f"ETL trigger failed: {str(e)}")
        
        # Calculate overall success
        overall_success = (
            flow_results["postgresql"]["success"] and
            (not include_s3 or flow_results["s3"]["success"]) and
            (not include_snowflake or flow_results["snowflake"]["success"]) and
            (not trigger_etl or flow_results["etl_trigger"]["success"])
        )
        
        return {
            "status": "success" if overall_success else "partial_success",
            "message": "Complete data flow executed",
            "data": {
                "data_type": data_type,
                "batch_id": batch_id,
                "flow_results": flow_results,
                "overall_success": overall_success,
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Error in complete data flow: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute complete data flow: {str(e)}"
        )


@router.post("/upload/s3", response_model=Dict[str, Any])
async def upload_to_s3_data_lake(
    file: UploadFile = File(...),
    data_type: str = Form(..., description="Type of data: smart_meter, grid_operator, weather"),
    batch_id: Optional[str] = Form(None, description="Optional batch ID for tracking"),
    s3_client: S3Client = Depends(get_s3_client)
):
    """Upload data file to S3 data lake"""
    try:
        # Validate file type
        allowed_types = ["smart_meter", "grid_operator", "weather_station"]
        if data_type not in allowed_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid data_type. Must be one of: {allowed_types}"
            )
        
        # Generate S3 key
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        if batch_id:
            s3_key = f"raw/{data_type}/{batch_id}/{timestamp}_{file.filename}"
        else:
            s3_key = f"raw/{data_type}/{timestamp}_{file.filename}"
        
        # Read file content
        content = await file.read()
        
        # Upload to S3
        upload_result = await s3_client.upload_data(
            data=content,
            s3_key=s3_key,
            content_type=file.content_type or "application/octet-stream"
        )
        
        # Log the upload
        logger.info(f"File uploaded to S3: {s3_key}, size: {len(content)} bytes")
        
        return {
            "status": "success",
            "message": "File uploaded to S3 data lake successfully",
            "data": {
                "s3_key": s3_key,
                "bucket": "metrify-data-lake",
                "file_size": len(content),
                "data_type": data_type,
                "batch_id": batch_id,
                "upload_timestamp": timestamp
            }
        }
        
    except Exception as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload file to S3: {str(e)}"
        )


class SnowflakeUploadRequest(BaseModel):
    """Request model for Snowflake upload"""
    data: Dict[str, Any]
    table_name: str
    data_type: str


@router.post("/upload/snowflake", response_model=Dict[str, Any])
async def upload_to_snowflake_warehouse(
    request: SnowflakeUploadRequest,
    snowflake_client: SnowflakeClient = Depends(get_snowflake_client)
):
    """Upload data directly to Snowflake data warehouse"""
    try:
        # Extract parameters from request
        data = request.data
        table_name = request.table_name
        data_type = request.data_type
        
        # Validate data type
        allowed_types = ["smart_meter", "grid_operator", "weather_station"]
        if data_type not in allowed_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid data_type. Must be one of: {allowed_types}"
            )
        
        # Prepare data for Snowflake
        if isinstance(data, dict):
            data_list = [data]
        else:
            data_list = data
        
        # Upload to Snowflake
        upload_result = await snowflake_client.insert_data(
            database="METRIFY_ANALYTICS",
            schema="RAW",
            table=table_name,
            data=data_list
        )
        
        logger.info(f"Data uploaded to Snowflake: {len(data_list)} records to {table_name}")
        
        return {
            "status": "success",
            "message": "Data uploaded to Snowflake successfully",
            "data": {
                "table_name": table_name,
                "records_uploaded": len(data_list),
                "data_type": data_type,
                "upload_timestamp": datetime.utcnow().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Error uploading to Snowflake: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload data to Snowflake: {str(e)}"
        )


class ETLTriggerRequest(BaseModel):
    """Request model for ETL trigger"""
    data_type: str
    batch_id: Optional[str] = None


@router.post("/trigger/etl", response_model=Dict[str, Any])
async def trigger_etl_pipeline(
    request: ETLTriggerRequest,
    airflow_client: AirflowClient = Depends(get_airflow_client)
):
    """Trigger ETL pipeline to move data from PostgreSQL to S3"""
    try:
        # Extract parameters from request
        data_type = request.data_type
        batch_id = request.batch_id
        
        # Map data type to DAG ID
        dag_mapping = {
            "smart_meter": "smart_meter_data_pipeline",
            "grid_operator": "grid_operator_pipeline",
            "weather_station": "weather_data_pipeline"
        }
        
        if data_type not in dag_mapping:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid data_type. Must be one of: {list(dag_mapping.keys())}"
            )
        
        dag_id = dag_mapping[data_type]
        
        # Trigger DAG
        async with airflow_client:
            dag_run = await airflow_client.trigger_dag(
                dag_id=dag_id,
                conf={"batch_id": batch_id} if batch_id else {}
            )
        
        logger.info(f"ETL pipeline triggered: {dag_id}, run_id: {dag_run.run_id}")
        
        return {
            "status": "success",
            "message": "ETL pipeline triggered successfully",
            "data": {
                "dag_id": dag_id,
                "run_id": dag_run.run_id,
                "data_type": data_type,
                "batch_id": batch_id,
                "trigger_timestamp": datetime.utcnow().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Error triggering ETL pipeline: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger ETL pipeline: {str(e)}"
        )


class DBTTriggerRequest(BaseModel):
    """Request model for DBT trigger"""
    models: Optional[str] = None
    full_refresh: bool = False


@router.post("/trigger/dbt", response_model=Dict[str, Any])
async def trigger_dbt_transformations(
    request: DBTTriggerRequest,
    airflow_client: AirflowClient = Depends(get_airflow_client)
):
    """Trigger DBT transformations to process data from S3 to Snowflake"""
    try:
        # Extract parameters from request
        models = request.models
        full_refresh = request.full_refresh
        
        # Trigger DBT DAG
        async with airflow_client:
            dag_run = await airflow_client.trigger_dag(
                dag_id="dbt_transformations",
                conf={
                    "models": models,
                    "full_refresh": full_refresh
                }
            )
        
        logger.info(f"DBT transformations triggered, run_id: {dag_run.run_id}")
        
        return {
            "status": "success",
            "message": "DBT transformations triggered successfully",
            "data": {
                "dag_id": "dbt_transformations",
                "run_id": dag_run.run_id,
                "models": models,
                "full_refresh": full_refresh,
                "trigger_timestamp": datetime.utcnow().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Error triggering DBT transformations: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger DBT transformations: {str(e)}"
        )


class FullPipelineTriggerRequest(BaseModel):
    """Request model for full pipeline trigger"""
    data_type: str
    batch_id: Optional[str] = None


@router.post("/trigger/full-pipeline", response_model=Dict[str, Any])
async def trigger_full_data_pipeline(
    request: FullPipelineTriggerRequest,
    airflow_client: AirflowClient = Depends(get_airflow_client)
):
    """Trigger complete data pipeline: ETL + DBT transformations"""
    try:
        # Extract parameters from request
        data_type = request.data_type
        batch_id = request.batch_id
        
        # Map data type to DAG ID
        dag_mapping = {
            "smart_meter": "smart_meter_data_pipeline",
            "grid_operator": "grid_operator_pipeline",
            "weather_station": "weather_data_pipeline"
        }
        
        if data_type not in dag_mapping:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid data_type. Must be one of: {list(dag_mapping.keys())}"
            )
        
        dag_id = dag_mapping[data_type]
        
        # Trigger main pipeline DAG (which includes both ETL and DBT)
        async with airflow_client:
            dag_run = await airflow_client.trigger_dag(
                dag_id=dag_id,
                conf={
                    "batch_id": batch_id,
                    "include_dbt": True
                } if batch_id else {"include_dbt": True}
            )
        
        logger.info(f"Full data pipeline triggered: {dag_id}, run_id: {dag_run.run_id}")
        
        return {
            "status": "success",
            "message": "Full data pipeline triggered successfully",
            "data": {
                "dag_id": dag_id,
                "run_id": dag_run.run_id,
                "data_type": data_type,
                "batch_id": batch_id,
                "includes_dbt": True,
                "trigger_timestamp": datetime.utcnow().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Error triggering full data pipeline: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger full data pipeline: {str(e)}"
        )


@router.get("/status/{run_id}", response_model=Dict[str, Any])
async def get_pipeline_status(
    run_id: str,
    airflow_client: AirflowClient = Depends(get_airflow_client)
):
    """Get status of a pipeline run"""
    try:
        # Get DAG run status
        async with airflow_client:
            dag_run = await airflow_client.get_dag_run_status(run_id)
        
        return {
            "status": "success",
            "data": {
                "run_id": run_id,
                "dag_id": dag_run.dag_id,
                "state": dag_run.state,
                "start_date": dag_run.start_date.isoformat() if dag_run.start_date else None,
                "end_date": dag_run.end_date.isoformat() if dag_run.end_date else None,
                "duration": str(dag_run.end_date - dag_run.start_date) if dag_run.start_date and dag_run.end_date else None
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting pipeline status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get pipeline status: {str(e)}"
        )


@router.get("/dags", response_model=Dict[str, Any])
async def list_available_dags(
    airflow_client: AirflowClient = Depends(get_airflow_client)
):
    """List all available DAGs for data processing"""
    try:
        async with airflow_client:
            dags = await airflow_client.list_dags()
        
        # Filter for data processing DAGs
        data_dags = [dag for dag in dags if any(keyword in dag.dag_id.lower() 
                    for keyword in ['smart_meter', 'grid_operator', 'weather', 'dbt'])]
        
        return {
            "status": "success",
            "data": {
                "total_dags": len(data_dags),
                "dags": [
                    {
                        "dag_id": dag.dag_id,
                        "description": dag.description,
                        "is_active": dag.is_active,
                        "last_run": dag.last_run.isoformat() if dag.last_run else None
                    }
                    for dag in data_dags
                ]
            }
        }
        
    except Exception as e:
        logger.error(f"Error listing DAGs: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list DAGs: {str(e)}"
        )


@router.get("/data-flow/validate", response_model=Dict[str, Any])
async def validate_data_flow(
    data_type: str,
    record_id: str,
    smart_meter_repo: SmartMeterRepository = Depends(get_smart_meter_repository),
    grid_operator_repo: GridOperatorRepository = Depends(get_grid_operator_repository),
    weather_station_repo: WeatherStationRepository = Depends(get_weather_station_repository),
    s3_client: S3Client = Depends(get_s3_client),
    snowflake_client: SnowflakeClient = Depends(get_snowflake_client)
):
    """
    Validate that data exists in all storage systems (PostgreSQL, S3, Snowflake)
    This provides comprehensive data flow validation
    """
    try:
        validation_results = {
            "postgresql": {"exists": False, "message": ""},
            "s3": {"exists": False, "message": "", "s3_keys": []},
            "snowflake": {"exists": False, "message": "", "record_count": 0}
        }
        
        # Validate PostgreSQL
        try:
            if data_type == "smart_meter":
                meter = await smart_meter_repo.get_by_id(record_id)
                validation_results["postgresql"]["exists"] = meter is not None
                validation_results["postgresql"]["message"] = "Found in PostgreSQL" if meter else "Not found in PostgreSQL"
            elif data_type == "grid_operator":
                operator = await grid_operator_repo.get_by_id(record_id)
                validation_results["postgresql"]["exists"] = operator is not None
                validation_results["postgresql"]["message"] = "Found in PostgreSQL" if operator else "Not found in PostgreSQL"
            elif data_type == "weather_station":
                station = await weather_station_repo.get_by_id(record_id)
                validation_results["postgresql"]["exists"] = station is not None
                validation_results["postgresql"]["message"] = "Found in PostgreSQL" if station else "Not found in PostgreSQL"
        except Exception as e:
            validation_results["postgresql"]["message"] = f"PostgreSQL validation error: {str(e)}"
        
        # Validate S3 (search for files containing the record_id)
        try:
            # List objects in the data lake bucket
            objects = await s3_client.list_objects("metrify-data-lake", prefix=f"processed/{data_type}/")
            matching_keys = []
            
            for obj in objects:
                if record_id in obj.get("Key", ""):
                    matching_keys.append(obj["Key"])
            
            validation_results["s3"]["exists"] = len(matching_keys) > 0
            validation_results["s3"]["message"] = f"Found {len(matching_keys)} matching files" if matching_keys else "No matching files found"
            validation_results["s3"]["s3_keys"] = matching_keys
            
        except Exception as e:
            validation_results["s3"]["message"] = f"S3 validation error: {str(e)}"
        
        # Validate Snowflake
        try:
            table_mapping = {
                "smart_meter": "smart_meters",
                "grid_operator": "grid_operators",
                "weather_station": "weather_stations"
            }
            
            table_name = table_mapping.get(data_type)
            if table_name:
                # Query Snowflake for the record
                id_field = f"{data_type}_id" if data_type != "weather_station" else "station_id"
                query = f"SELECT COUNT(*) as record_count FROM METRIFY_ANALYTICS.RAW.{table_name} WHERE {id_field} = '{record_id}'"
                
                result = await snowflake_client.execute_query(query)
                record_count = result[0]["RECORD_COUNT"] if result else 0
                
                validation_results["snowflake"]["exists"] = record_count > 0
                validation_results["snowflake"]["message"] = f"Found {record_count} records" if record_count > 0 else "No records found"
                validation_results["snowflake"]["record_count"] = record_count
                
        except Exception as e:
            validation_results["snowflake"]["message"] = f"Snowflake validation error: {str(e)}"
        
        # Calculate overall validation status
        all_systems_valid = (
            validation_results["postgresql"]["exists"] and
            validation_results["s3"]["exists"] and
            validation_results["snowflake"]["exists"]
        )
        
        return {
            "status": "success",
            "data": {
                "data_type": data_type,
                "record_id": record_id,
                "validation_results": validation_results,
                "all_systems_valid": all_systems_valid,
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Error validating data flow: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to validate data flow: {str(e)}"
        )


@router.get("/health", response_model=Dict[str, Any])
async def pipeline_health_check(
    s3_client: S3Client = Depends(get_s3_client),
    snowflake_client: SnowflakeClient = Depends(get_snowflake_client),
    airflow_client: AirflowClient = Depends(get_airflow_client)
):
    """Health check for all pipeline components"""
    try:
        health_status = {
            "s3": False,
            "snowflake": False,
            "airflow": False
        }
        
        # Check S3
        try:
            await s3_client.health_check()
            health_status["s3"] = True
        except Exception as e:
            logger.warning(f"S3 health check failed: {e}")
        
        # Check Snowflake
        try:
            await snowflake_client.health_check()
            health_status["snowflake"] = True
        except Exception as e:
            logger.warning(f"Snowflake health check failed: {e}")
        
        # Check Airflow
        try:
            async with airflow_client:
                await airflow_client.health_check()
                health_status["airflow"] = True
        except Exception as e:
            logger.warning(f"Airflow health check failed: {e}")
        
        overall_health = all(health_status.values())
        
        return {
            "status": "healthy" if overall_health else "degraded",
            "components": health_status,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in pipeline health check: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Pipeline health check failed: {str(e)}"
        )


@router.post("/trigger/spark-etl")
async def trigger_spark_etl(request: SparkETLRequest):
    """
    Trigger Spark ETL job for data processing
    
    Args:
        request: Spark ETL request with job parameters
    
    Returns:
        ETL job execution result
    """
    try:
        logger.info(f"Triggering Spark ETL job for {request.data_type}")
        
        # Validate data type
        allowed_types = ["smart_meter", "grid_operator", "weather_station"]
        if request.data_type not in allowed_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid data_type. Must be one of: {allowed_types}"
            )
        
        # Validate output format
        allowed_formats = ["delta", "parquet"]
        if request.output_format not in allowed_formats:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid output_format. Must be one of: {allowed_formats}"
            )
        
        # Submit job to Spark ETL service
        etl_result = await spark_etl_service.submit_job(
            data_type=request.data_type,
            source_path=request.source_path,
            target_path=request.target_path,
            output_format=request.output_format,
            batch_id=request.batch_id
        )
        
        logger.info(f"Spark ETL job submitted: {etl_result['job_id']}")
        return etl_result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error triggering Spark ETL job: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger Spark ETL job: {str(e)}"
        )


@router.get("/spark-etl/status/{job_id}")
async def get_spark_etl_status(job_id: str):
    """
    Get Spark ETL job status
    
    Args:
        job_id: Spark ETL job ID
    
    Returns:
        Job status information
    """
    try:
        logger.info(f"Getting Spark ETL job status: {job_id}")
        
        # Get job status from Spark ETL service
        status_result = await spark_etl_service.get_job_status(job_id)
        
        return status_result
        
    except Exception as e:
        logger.error(f"Error getting Spark ETL job status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get Spark ETL job status: {str(e)}"
        )


@router.post("/spark-etl/cancel/{job_id}")
async def cancel_spark_etl_job(job_id: str):
    """
    Cancel a running Spark ETL job
    
    Args:
        job_id: Spark ETL job ID to cancel
    
    Returns:
        Cancellation result
    """
    try:
        logger.info(f"Cancelling Spark ETL job: {job_id}")
        
        # Cancel job via Spark ETL service
        cancel_result = await spark_etl_service.cancel_job(job_id)
        
        return cancel_result
        
    except Exception as e:
        logger.error(f"Error cancelling Spark ETL job: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel Spark ETL job: {str(e)}"
        )


@router.get("/spark-etl/jobs")
async def list_spark_etl_jobs(status_filter: Optional[str] = None):
    """
    List Spark ETL jobs
    
    Args:
        status_filter: Optional status filter (submitted, running, completed, failed, cancelled)
    
    Returns:
        List of Spark ETL jobs
    """
    try:
        logger.info(f"Listing Spark ETL jobs with filter: {status_filter}")
        
        # Get jobs from Spark ETL service
        jobs_result = await spark_etl_service.list_jobs(status_filter)
        
        return jobs_result
        
    except Exception as e:
        logger.error(f"Error listing Spark ETL jobs: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list Spark ETL jobs: {str(e)}"
        )
