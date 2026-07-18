"""
Governance API Endpoints
Comprehensive API for data governance, compliance, and quality management
"""

import logging
import os
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel, Field
import pandas as pd

# Import governance components
from src.governance.lineage import AtlasIntegration, AtlasConfig, LineageTracker, LineageVisualizer
from src.governance.compliance import (
    GDPRProcessor, PrivacyController, AuditLogger,
    ConsentManager, DataSubjectProcessor, RetentionPolicyEngine
)
from src.governance.quality import (
    QualityAssessor, ValidationEngine, QualityMonitor
)

logger = logging.getLogger(__name__)

router = APIRouter()

# Initialize governance components
atlas_integration = AtlasIntegration(
    AtlasConfig(
        base_url=os.getenv("ATLAS_BASE_URL", "http://localhost:21000"),
        username=os.getenv("ATLAS_USERNAME", ""),
        password=os.getenv("ATLAS_PASSWORD", ""),
    )
)

lineage_tracker = LineageTracker(atlas_integration)
lineage_visualizer = LineageVisualizer()
gdpr_processor = GDPRProcessor()
privacy_controller = PrivacyController()
audit_logger = AuditLogger()
consent_manager = ConsentManager()
data_subject_processor = DataSubjectProcessor()
retention_engine = RetentionPolicyEngine()
quality_assessor = QualityAssessor()
validation_engine = ValidationEngine()
quality_monitor = QualityMonitor()

# Pydantic models for request/response
class LineageEventRequest(BaseModel):
    event_type: str
    source_entity: str
    target_entity: str
    process_name: str
    metadata: Dict[str, Any] = {}
    user_id: Optional[str] = None

class DataSubjectRequest(BaseModel):
    subject_id: str
    right_type: str
    description: str
    request_data: Dict[str, Any] = {}

class ConsentRequest(BaseModel):
    subject_id: str
    template_id: str
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None

class QualityAssessmentRequest(BaseModel):
    data: Dict[str, Any]
    schema: Optional[Dict[str, Any]] = None
    business_rules: Optional[List[Dict[str, Any]]] = None

class ValidationRequest(BaseModel):
    data: Dict[str, Any]
    rules: Optional[List[str]] = None
    columns: Optional[List[str]] = None

class MonitorRequest(BaseModel):
    monitor_id: str
    metric_name: str
    threshold: float
    alert_level: str
    description: str = ""

# Lineage Endpoints
@router.post("/lineage/events")
async def track_lineage_event(request: LineageEventRequest):
    """Track a lineage event"""
    try:
        from src.governance.lineage.lineage_tracker import LineageEventType
        
        event_type = LineageEventType(request.event_type)
        event_id = lineage_tracker.track_lineage_event(
            event_type=event_type,
            source_entity=request.source_entity,
            target_entity=request.target_entity,
            process_name=request.process_name,
            metadata=request.metadata,
            user_id=request.user_id
        )
        
        return {
            "event_id": event_id,
            "status": "success",
            "message": "Lineage event tracked successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to track lineage event: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/lineage/entities/{entity_id}")
async def get_entity_lineage(entity_id: str, max_depth: int = 3):
    """Get lineage for a specific entity"""
    try:
        lineage = lineage_tracker.get_entity_lineage(entity_id, max_depth)
        return lineage
        
    except Exception as e:
        logger.error(f"Failed to get entity lineage: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/lineage/visualization/{entity_id}")
async def get_lineage_visualization(entity_id: str, format: str = "plotly"):
    """Get lineage visualization"""
    try:
        lineage_data = lineage_tracker.export_lineage_data("json")
        
        if format == "plotly":
            visualization = lineage_visualizer.visualize_lineage_plotly(
                json.loads(lineage_data)
            )
            return {"visualization": visualization}
        else:
            return {"lineage_data": json.loads(lineage_data)}
            
    except Exception as e:
        logger.error(f"Failed to get lineage visualization: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Compliance Endpoints
@router.post("/compliance/gdpr/subjects")
async def register_data_subject(
    email: str,
    name: Optional[str] = None,
    phone: Optional[str] = None,
    address: Optional[str] = None
):
    """Register a new data subject"""
    try:
        subject_id = gdpr_processor.register_data_subject(
            email=email,
            name=name,
            phone=phone,
            address=address
        )
        
        return {
            "subject_id": subject_id,
            "status": "success",
            "message": "Data subject registered successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to register data subject: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/compliance/gdpr/consent")
async def collect_consent(request: ConsentRequest):
    """Collect consent from a data subject"""
    try:
        consent_id = consent_manager.collect_consent(
            subject_id=request.subject_id,
            template_id=request.template_id,
            ip_address=request.ip_address,
            user_agent=request.user_agent
        )
        
        return {
            "consent_id": consent_id,
            "status": "success",
            "message": "Consent collected successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to collect consent: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/compliance/gdpr/requests")
async def submit_data_subject_request(request: DataSubjectRequest):
    """Submit a data subject rights request"""
    try:
        from src.governance.compliance.data_subject_processor import DataSubjectRight
        
        right_type = DataSubjectRight(request.right_type)
        request_id = data_subject_processor.submit_request(
            subject_id=request.subject_id,
            right_type=right_type,
            description=request.description,
            request_data=request.request_data
        )
        
        return {
            "request_id": request_id,
            "status": "success",
            "message": "Data subject request submitted successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to submit data subject request: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/compliance/gdpr/requests/{request_id}")
async def get_data_subject_request(request_id: str):
    """Get a data subject request"""
    try:
        request = data_subject_processor.get_request(request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found")
        
        return {
            "request_id": request.request_id,
            "subject_id": request.subject_id,
            "right_type": request.right_type.value,
            "status": request.status.value,
            "submitted_at": request.submitted_at.isoformat(),
            "due_date": request.due_date.isoformat(),
            "description": request.description
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get data subject request: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/compliance/privacy/analyze")
async def analyze_privacy(data: Dict[str, Any]):
    """Analyze data for privacy compliance"""
    try:
        report = privacy_controller.generate_privacy_report(data)
        return report
        
    except Exception as e:
        logger.error(f"Failed to analyze privacy: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/compliance/privacy/anonymize")
async def anonymize_data(
    data: Dict[str, Any],
    anonymization_level: str = "medium"
):
    """Anonymize data"""
    try:
        anonymized_data = privacy_controller.anonymize_data(data, anonymization_level)
        return {
            "anonymized_data": anonymized_data,
            "anonymization_level": anonymization_level,
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Failed to anonymize data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/compliance/audit/events")
async def get_audit_events(
    user_id: Optional[str] = None,
    event_types: Optional[List[str]] = None,
    severity_levels: Optional[List[str]] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: int = 1000
):
    """Get audit events"""
    try:
        from src.governance.compliance.audit_logger import AuditEventType, AuditSeverity
        
        # Convert string parameters to enums
        event_type_enums = None
        if event_types:
            event_type_enums = [AuditEventType(et) for et in event_types]
        
        severity_enums = None
        if severity_levels:
            severity_enums = [AuditSeverity(sl) for sl in severity_levels]
        
        events = audit_logger.query_audit_events(
            user_id=user_id,
            event_types=event_type_enums,
            severity_levels=severity_enums,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )
        
        return {
            "events": [
                {
                    "event_id": event.event_id,
                    "event_type": event.event_type.value,
                    "timestamp": event.timestamp.isoformat(),
                    "user_id": event.user_id,
                    "resource_type": event.resource_type,
                    "resource_id": event.resource_id,
                    "action": event.action,
                    "description": event.description,
                    "severity": event.severity.value,
                    "result": event.result
                }
                for event in events
            ],
            "total_events": len(events)
        }
        
    except Exception as e:
        logger.error(f"Failed to get audit events: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Quality Endpoints
@router.post("/quality/assess")
async def assess_data_quality(request: QualityAssessmentRequest):
    """Assess data quality"""
    try:
        # Convert data to DataFrame
        df = pd.DataFrame(request.data)
        
        quality_score = quality_assessor.assess_data_quality(
            data=df,
            schema=request.schema,
            business_rules=request.business_rules
        )
        
        return {
            "overall_score": quality_score.overall_score,
            "quality_level": quality_score.level.value,
            "dimension_scores": {
                dim.value: score for dim, score in quality_score.dimension_scores.items()
            },
            "assessment_date": quality_score.assessment_date.isoformat(),
            "data_size": quality_score.data_size,
            "issues_found": quality_score.issues_found
        }
        
    except Exception as e:
        logger.error(f"Failed to assess data quality: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/quality/validate")
async def validate_data(request: ValidationRequest):
    """Validate data against rules"""
    try:
        # Convert data to DataFrame
        df = pd.DataFrame(request.data)
        
        results = validation_engine.validate_data(
            data=df,
            rules=request.rules,
            columns=request.columns
        )
        
        summary = validation_engine.get_validation_summary(results)
        
        return {
            "validation_results": [
                {
                    "rule_id": result.rule_id,
                    "rule_name": result.rule_name,
                    "passed": result.passed,
                    "severity": result.severity.value,
                    "message": result.message,
                    "affected_rows": result.affected_rows,
                    "affected_columns": result.affected_columns
                }
                for result in results
            ],
            "summary": summary
        }
        
    except Exception as e:
        logger.error(f"Failed to validate data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/quality/monitors")
async def add_quality_monitor(request: MonitorRequest):
    """Add a quality monitor"""
    try:
        from src.governance.quality.quality_monitor import AlertLevel
        
        alert_level = AlertLevel(request.alert_level)
        
        # Create a simple check function (in real implementation, this would be more sophisticated)
        def check_function():
            return 0.95  # Placeholder value
        
        success = quality_monitor.add_monitor(
            monitor_id=request.monitor_id,
            metric_name=request.metric_name,
            threshold=request.threshold,
            alert_level=alert_level,
            check_function=check_function,
            description=request.description
        )
        
        if success:
            return {
                "monitor_id": request.monitor_id,
                "status": "success",
                "message": "Quality monitor added successfully"
            }
        else:
            raise HTTPException(status_code=400, detail="Failed to add monitor")
            
    except Exception as e:
        logger.error(f"Failed to add quality monitor: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/quality/monitors/status")
async def get_monitoring_status():
    """Get quality monitoring status"""
    try:
        status = quality_monitor.get_monitoring_status()
        return status
        
    except Exception as e:
        logger.error(f"Failed to get monitoring status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/quality/alerts")
async def get_quality_alerts(level: Optional[str] = None):
    """Get quality alerts"""
    try:
        if level:
            from src.governance.quality.quality_monitor import AlertLevel
            alert_level = AlertLevel(level)
            alerts = quality_monitor.get_alerts_by_level(alert_level)
        else:
            alerts = quality_monitor.get_active_alerts()
        
        return {
            "alerts": [
                {
                    "alert_id": alert.alert_id,
                    "monitor_id": alert.monitor_id,
                    "level": alert.level.value,
                    "message": alert.message,
                    "metric_name": alert.metric_name,
                    "current_value": alert.current_value,
                    "threshold_value": alert.threshold_value,
                    "timestamp": alert.timestamp.isoformat(),
                    "resolved": alert.resolved
                }
                for alert in alerts
            ],
            "total_alerts": len(alerts)
        }
        
    except Exception as e:
        logger.error(f"Failed to get quality alerts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/quality/alerts/{alert_id}/resolve")
async def resolve_alert(alert_id: str):
    """Resolve a quality alert"""
    try:
        success = quality_monitor.resolve_alert(alert_id)
        
        if success:
            return {
                "alert_id": alert_id,
                "status": "success",
                "message": "Alert resolved successfully"
            }
        else:
            raise HTTPException(status_code=404, detail="Alert not found or already resolved")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to resolve alert: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# System Status Endpoints
@router.get("/governance/status")
async def get_governance_status():
    """Get overall governance system status"""
    try:
        # Get status from various components
        lineage_stats = lineage_tracker.get_lineage_statistics()
        compliance_report = gdpr_processor.get_compliance_report()
        audit_stats = audit_logger.get_audit_statistics()
        monitoring_status = quality_monitor.get_monitoring_status()
        
        return {
            "system_status": "operational",
            "components": {
                "lineage": {
                    "status": "operational",
                    "total_entities": lineage_stats.get("total_entities", 0),
                    "total_events": lineage_stats.get("total_events", 0)
                },
                "compliance": {
                    "status": "operational",
                    "data_subjects": compliance_report.get("data_subjects", {}).get("total", 0),
                    "processing_activities": compliance_report.get("processing_activities", {}).get("total", 0)
                },
                "audit": {
                    "status": "operational",
                    "total_events": audit_stats.get("total_events", 0),
                    "success_rate": audit_stats.get("success_rate", 0)
                },
                "quality": {
                    "status": monitoring_status.get("status", "unknown"),
                    "active_monitors": monitoring_status.get("active_monitors", 0),
                    "active_alerts": monitoring_status.get("active_alerts", 0)
                }
            },
            "generated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get governance status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/governance/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Basic health checks
        atlas_health = atlas_integration.get_health_status()
        
        return {
            "status": "healthy",
            "components": {
                "atlas_integration": atlas_health.get("status", "unknown"),
                "lineage_tracker": "operational",
                "gdpr_processor": "operational",
                "privacy_controller": "operational",
                "audit_logger": "operational",
                "quality_assessor": "operational",
                "validation_engine": "operational",
                "quality_monitor": "operational"
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }
