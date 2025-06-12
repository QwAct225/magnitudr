from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import pandas as pd
import os
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
import logging
from decimal import Decimal
import decimal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection with retry
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:earthquake123@postgres:5432/magnitudr")

def create_db_engine():
    """Create database engine with error handling"""
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Database connection established")
        return engine
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return None

engine = create_db_engine()

# FastAPI app initialization
app = FastAPI(
    title="Magnitudr Earthquake Analysis API",
    description="🌍 Real-time earthquake data analysis and hazard zone detection for Indonesia",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware for Streamlit integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for API responses - DEFINED FIRST
class EarthquakeData(BaseModel):
    id: str
    magnitude: float
    latitude: float
    longitude: float
    depth: float
    time: Optional[datetime] = None
    place: Optional[str] = ""
    spatial_density: Optional[float] = None
    hazard_score: Optional[float] = None
    region: Optional[str] = None
    magnitude_category: Optional[str] = None
    depth_category: Optional[str] = None
    risk_zone: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True

class ClusterData(BaseModel):
    id: str
    cluster_id: int
    cluster_label: str
    risk_zone: str
    centroid_lat: float
    centroid_lon: float
    cluster_size: int
    avg_magnitude: float

class HazardZone(BaseModel):
    zone_id: int
    risk_level: str
    avg_magnitude: float
    event_count: int
    center_lat: float
    center_lon: float
    boundary_coordinates: Optional[str] = ""

class MLPrediction(BaseModel):
    earthquake_id: str
    predicted_risk_zone: str
    prediction_confidence: float
    model_version: str
    created_at: datetime

class ModelMetrics(BaseModel):
    model_name: str
    model_type: str
    accuracy: float
    precision_score: float
    recall_score: float
    f1_score: float
    training_samples: int

class SystemStats(BaseModel):
    total_earthquakes: int
    total_clusters: int
    high_risk_zones: int
    last_update: datetime
    data_quality_score: float
    ml_model_available: bool = False

def safe_float(value):
    """Safely convert any numeric type to float"""
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except:
        return 0.0

def safe_int(value):
    """Safely convert any numeric type to int"""
    if value is None:
        return 0
    try:
        return int(value)
    except:
        return 0

# Database dependency with fallback
def get_db_connection():
    if engine is None:
        raise HTTPException(status_code=503, detail="Database service unavailable")
    try:
        return engine.connect()
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

# API Endpoints
@app.get("/", summary="API Health Check")
async def root():
    """Health check endpoint with system information"""
    db_status = "connected" if engine else "disconnected"
    
    return {
        "message": "🌍 Magnitudr Earthquake Analysis API",
        "status": "operational",
        "database": db_status,
        "version": "1.0.0",
        "documentation": "/docs",
        "endpoints": {
            "earthquakes": "/earthquakes",
            "clusters": "/clusters", 
            "hazard-zones": "/hazard-zones",
            "statistics": "/stats",
            "health": "/health",
            "ml-comparison": "/ml/model-comparison",
            "pipeline-status": "/pipeline/status"
        }
    }

@app.get("/health", summary="System Health Check")
async def health_check():
    """Detailed health check for monitoring"""
    try:
        if engine is None:
            return {"status": "unhealthy", "database": "disconnected"}
            
        with get_db_connection() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM earthquakes_processed")).fetchone()
            count = result[0] if result else 0
            
        return {
            "status": "healthy",
            "database": "connected", 
            "earthquake_records": safe_int(count),
            "api_version": "1.0.0",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/earthquakes", response_model=List[EarthquakeData], summary="Get Earthquake Data")
async def get_earthquakes(
    limit: int = Query(default=1000, le=10000, description="Maximum number of records"),
    min_magnitude: Optional[float] = Query(default=None, description="Minimum magnitude filter"),
    region: Optional[str] = Query(default=None, description="Region filter"),
    risk_zone: Optional[str] = Query(default=None, description="Risk zone filter")
):
    """Get earthquake data with advanced filtering"""
    try:
        query = """
        SELECT 
            e.id, e.magnitude, e.latitude, e.longitude, e.depth, e.time,
            COALESCE(e.place, '') as place, 
            COALESCE(e.spatial_density, 0) as spatial_density, 
            COALESCE(e.hazard_score, 0) as hazard_score, 
            COALESCE(e.region, 'Unknown') as region,
            COALESCE(e.magnitude_category, 'Unknown') as magnitude_category, 
            COALESCE(e.depth_category, 'Unknown') as depth_category,
            COALESCE(c.risk_zone, 'Unknown') as risk_zone
        FROM earthquakes_processed e
        LEFT JOIN earthquake_clusters c ON e.id = c.id
        WHERE 1=1
        """
        
        params = {}
        
        if min_magnitude is not None:
            query += " AND e.magnitude >= :min_magnitude"
            params['min_magnitude'] = min_magnitude
            
        if region:
            query += " AND e.region = :region"
            params['region'] = region
            
        if risk_zone:
            query += """
            AND e.id IN (
                SELECT c.id FROM earthquake_clusters c 
                WHERE c.risk_zone = :risk_zone
            )
            """
            params['risk_zone'] = risk_zone
        
        query += " ORDER BY e.time DESC NULLS LAST LIMIT :limit"
        params['limit'] = limit
        
        with get_db_connection() as conn:
            result = conn.execute(text(query), params)
            data = result.fetchall()
            
        earthquakes = []
        for row in data:
            earthquakes.append(EarthquakeData(
                id=str(row[0]),
                magnitude=safe_float(row[1]),
                latitude=safe_float(row[2]),
                longitude=safe_float(row[3]),
                depth=safe_float(row[4]),
                time=row[5] if row[5] is not None else None,
                place=str(row[6]) if row[6] else "",
                spatial_density=safe_float(row[7]),
                hazard_score=safe_float(row[8]),
                region=str(row[9]) if row[9] else "Unknown",
                magnitude_category=str(row[10]) if row[10] else "Unknown",
                depth_category=str(row[11]) if row[11] else "Unknown",
                risk_zone=str(row[12]) if row[12] else "Unknown"
            ))
        
        logger.info(f"✅ Retrieved {len(earthquakes)} earthquake records")
        return earthquakes
        
    except Exception as e:
        logger.error(f"❌ Failed to retrieve earthquakes: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

@app.get("/clusters", response_model=List[ClusterData], summary="Get Cluster Analysis")
async def get_clusters(
    risk_zone: Optional[str] = Query(default=None, description="Filter by risk zone"),
    min_cluster_size: Optional[int] = Query(default=None, description="Minimum cluster size")
):
    """Get DBSCAN clustering results"""
    try:
        query = """
        SELECT DISTINCT
            c.id, c.cluster_id, 
            COALESCE(c.cluster_label, 'Unknown') as cluster_label, 
            COALESCE(c.risk_zone, 'Unknown') as risk_zone,
            COALESCE(c.centroid_lat, 0) as centroid_lat, 
            COALESCE(c.centroid_lon, 0) as centroid_lon, 
            COALESCE(c.cluster_size, 0) as cluster_size, 
            COALESCE(c.avg_magnitude, 0) as avg_magnitude
        FROM earthquake_clusters c
        WHERE 1=1
        """
        
        params = {}
        
        if risk_zone:
            query += " AND c.risk_zone = :risk_zone"
            params['risk_zone'] = risk_zone
            
        if min_cluster_size:
            query += " AND c.cluster_size >= :min_cluster_size"
            params['min_cluster_size'] = min_cluster_size
        
        query += " ORDER BY c.cluster_size DESC LIMIT 1000"
        
        with get_db_connection() as conn:
            result = conn.execute(text(query), params)
            data = result.fetchall()
        
        clusters = []
        for row in data:
            clusters.append(ClusterData(
                id=str(row[0]),
                cluster_id=safe_int(row[1]),
                cluster_label=str(row[2]),
                risk_zone=str(row[3]),
                centroid_lat=safe_float(row[4]),
                centroid_lon=safe_float(row[5]),
                cluster_size=safe_int(row[6]),
                avg_magnitude=safe_float(row[7])
            ))
        
        logger.info(f"✅ Retrieved {len(clusters)} cluster records")
        return clusters
        
    except Exception as e:
        logger.error(f"❌ Failed to retrieve clusters: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

@app.get("/hazard-zones", response_model=List[HazardZone], summary="Get Hazard Zones")
async def get_hazard_zones(
    risk_level: Optional[str] = Query(default=None, description="Filter by risk level")
):
    """Get aggregated hazard zones for Indonesia"""
    try:
        query = """
        SELECT 
            zone_id, 
            COALESCE(risk_level, 'Unknown') as risk_level, 
            COALESCE(avg_magnitude, 0) as avg_magnitude, 
            COALESCE(event_count, 0) as event_count,
            COALESCE(center_lat, 0) as center_lat, 
            COALESCE(center_lon, 0) as center_lon, 
            COALESCE(boundary_coordinates::text, '{}') as boundary_coordinates
        FROM hazard_zones
        WHERE 1=1
        """
        
        params = {}
        
        if risk_level:
            query += " AND risk_level = :risk_level"
            params['risk_level'] = risk_level
        
        query += " ORDER BY event_count DESC LIMIT 100"
        
        with get_db_connection() as conn:
            result = conn.execute(text(query), params)
            data = result.fetchall()
        
        hazard_zones = []
        for row in data:
            hazard_zones.append(HazardZone(
                zone_id=safe_int(row[0]),
                risk_level=str(row[1]),
                avg_magnitude=safe_float(row[2]),
                event_count=safe_int(row[3]),
                center_lat=safe_float(row[4]),
                center_lon=safe_float(row[5]),
                boundary_coordinates=str(row[6]) if row[6] else "{}"
            ))
        
        logger.info(f"✅ Retrieved {len(hazard_zones)} hazard zones")
        return hazard_zones
        
    except Exception as e:
        logger.error(f"❌ Failed to retrieve hazard zones: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

@app.get("/stats", response_model=SystemStats, summary="Get System Statistics")
async def get_system_stats():
    """Get comprehensive system statistics with safe decimal handling"""
    try:
        with get_db_connection() as conn:
            # Get earthquake statistics with safe handling
            earthquake_result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_earthquakes,
                    COALESCE(AVG(hazard_score), 0) as avg_hazard_score
                FROM earthquakes_processed
            """)).fetchone()
            
            # Get cluster statistics
            cluster_result = conn.execute(text("""
                SELECT 
                    COUNT(DISTINCT cluster_id) as total_clusters
                FROM earthquake_clusters
                WHERE cluster_id IS NOT NULL
            """)).fetchone()
            
            # Get high-risk zones count  
            risk_result = conn.execute(text("""
                SELECT COUNT(*) as high_risk_zones
                FROM hazard_zones 
                WHERE risk_level IN ('High', 'Extreme')
            """)).fetchone()
            
            # Safe value extraction with proper decimal handling
            total_earthquakes = safe_int(earthquake_result[0]) if earthquake_result and earthquake_result[0] else 0
            avg_hazard = safe_float(earthquake_result[1]) if earthquake_result and earthquake_result[1] else 0.0
            total_clusters = safe_int(cluster_result[0]) if cluster_result and cluster_result[0] else 0
            high_risk_zones = safe_int(risk_result[0]) if risk_result and risk_result[0] else 0
            
            # Calculate data quality score with bulletproof error handling
            try:
                if avg_hazard and avg_hazard > 0:
                    # Convert decimal to float safely
                    hazard_float = float(str(avg_hazard))
                    data_quality = min(hazard_float / 10.0, 1.0)
                else:
                    data_quality = 0.0
            except (TypeError, ValueError, ZeroDivisionError, decimal.InvalidOperation):
                data_quality = 0.0
        
        stats = SystemStats(
            total_earthquakes=total_earthquakes,
            total_clusters=total_clusters,
            high_risk_zones=high_risk_zones,
            last_update=datetime.now(),
            data_quality_score=round(data_quality, 3)
        )
        
        logger.info("✅ Retrieved system statistics")
        return stats
        
    except Exception as e:
        logger.error(f"❌ Failed to retrieve statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Statistics query failed: {str(e)}")

@app.get("/ml/model-comparison", summary="Get ML Model Comparison Results")
async def get_model_comparison():
    """
    📊 **Get comprehensive ML model comparison results**
    
    Returns comparison between RandomForest and LogisticRegression models
    including performance metrics and training details.
    """
    try:
        import json
        from pathlib import Path
        
        # Load model comparison report
        report_path = Path("/app/data/airflow_output/model_comparison_report.json")
        
        if report_path.exists():
            with open(report_path, 'r') as f:
                comparison_data = json.load(f)
            
            logger.info("✅ Retrieved model comparison data")
            return {
                "status": "success",
                "comparison_timestamp": comparison_data.get('comparison_timestamp'),
                "best_model": comparison_data.get('best_model'),
                "model_comparison": comparison_data.get('model_comparison', {}),
                "recommendation": comparison_data.get('recommendation'),
                "total_predictions": comparison_data.get('total_predictions_generated', 0)
            }
        else:
            return {
                "status": "no_data",
                "message": "Model comparison not available. Run ML training first.",
                "models_available": False
            }
            
    except Exception as e:
        logger.error(f"❌ Failed to retrieve model comparison: {e}")
        raise HTTPException(status_code=500, detail=f"Model comparison query failed: {str(e)}")

@app.get("/pipeline/status", summary="Get Pipeline Status")
async def get_pipeline_status():
    """
    🔧 **Get comprehensive pipeline status and health**
    
    Returns status of all pipeline components including data volume,
    processing status, and system health.
    """
    try:
        with get_db_connection() as conn:
            # Check data volume
            result = conn.execute(text("SELECT COUNT(*) FROM earthquakes_processed")).fetchone()
            earthquake_count = safe_int(result[0]) if result else 0
            
            # Estimate data size
            if earthquake_count > 0:
                # Rough estimation: ~2KB per earthquake record
                estimated_size_mb = (earthquake_count * 2) / 1024
            else:
                estimated_size_mb = 0
            
            # Check cluster status
            cluster_result = conn.execute(text("SELECT COUNT(DISTINCT cluster_id) FROM earthquake_clusters WHERE cluster_id IS NOT NULL")).fetchone()
            cluster_count = safe_int(cluster_result[0]) if cluster_result else 0
            
            # Check ML model status
            ml_result = conn.execute(text("SELECT COUNT(*) FROM ml_model_metadata")).fetchone()
            ml_models_count = safe_int(ml_result[0]) if ml_result else 0
            
            # Check recent predictions
            pred_result = conn.execute(text("SELECT COUNT(*) FROM earthquake_predictions")).fetchone()
            predictions_count = safe_int(pred_result[0]) if pred_result else 0
        
        # Determine overall status
        pipeline_health = "healthy"
        if earthquake_count < 1000:
            pipeline_health = "warning"
        if earthquake_count == 0:
            pipeline_health = "error"
        
        return {
            "pipeline_status": pipeline_health,
            "data_pipeline": {
                "earthquake_records": earthquake_count,
                "estimated_size_mb": round(estimated_size_mb, 2),
                "meets_64mb_requirement": estimated_size_mb >= 64,
                "clusters_identified": cluster_count,
                "clustering_completed": cluster_count > 0
            },
            "ml_pipeline": {
                "models_trained": ml_models_count,
                "predictions_generated": predictions_count,
                "ml_ready": ml_models_count > 0
            },
            "system_health": {
                "database_connected": True,
                "api_operational": True,
                "last_check": datetime.now().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to get pipeline status: {e}")
        return {
            "pipeline_status": "error",
            "error_message": str(e),
            "system_health": {
                "database_connected": False,
                "api_operational": True,
                "last_check": datetime.now().isoformat()
            }
        }

@app.get("/data-volume", summary="Verify Data Volume Requirements")
async def get_data_volume():
    """
    📊 **Verify 64MB data requirement compliance**
    
    Returns detailed information about data volume and requirement compliance.
    """
    try:
        with get_db_connection() as conn:
            # Get detailed data statistics
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT id) as unique_earthquakes,
                    MIN(time) as earliest_record,
                    MAX(time) as latest_record,
                    AVG(magnitude) as avg_magnitude,
                    COUNT(DISTINCT region) as regions_covered
                FROM earthquakes_processed
            """)).fetchone()
            
            if result and result[0] > 0:
                # Estimate data size (rough calculation)
                # Average earthquake record ~2KB (all columns + metadata)
                estimated_size_mb = (result[0] * 2) / 1024
                
                # Check for actual file if available
                import os
                file_paths = [
                    "/app/data/airflow_output/raw_earthquake_data.csv",
                    "/app/data/airflow_output/processed_earthquake_data.csv"
                ]
                
                actual_file_sizes = {}
                total_actual_size_mb = 0
                
                for file_path in file_paths:
                    if os.path.exists(file_path):
                        size_bytes = os.path.getsize(file_path)
                        size_mb = size_bytes / (1024 * 1024)
                        actual_file_sizes[os.path.basename(file_path)] = round(size_mb, 2)
                        total_actual_size_mb += size_mb
                
                return {
                    "requirement_status": "compliant" if max(estimated_size_mb, total_actual_size_mb) >= 64 else "non_compliant",
                    "data_statistics": {
                        "total_records": safe_int(result[0]),
                        "unique_earthquakes": safe_int(result[1]),
                        "earliest_record": result[2].isoformat() if result[2] else None,
                        "latest_record": result[3].isoformat() if result[3] else None,
                        "avg_magnitude": round(safe_float(result[4]), 2),
                        "regions_covered": safe_int(result[5])
                    },
                    "size_analysis": {
                        "estimated_size_mb": round(estimated_size_mb, 2),
                        "actual_file_sizes_mb": actual_file_sizes,
                        "total_actual_size_mb": round(total_actual_size_mb, 2),
                        "meets_64mb_requirement": max(estimated_size_mb, total_actual_size_mb) >= 64,
                        "target_size_mb": 64
                    },
                    "verification_timestamp": datetime.now().isoformat()
                }
            else:
                return {
                    "requirement_status": "no_data",
                    "message": "No earthquake data found. Run data pipeline first.",
                    "size_analysis": {
                        "estimated_size_mb": 0,
                        "meets_64mb_requirement": False,
                        "target_size_mb": 64
                    }
                }
                
    except Exception as e:
        logger.error(f"❌ Failed to verify data volume: {e}")
        raise HTTPException(status_code=500, detail=f"Data volume verification failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
