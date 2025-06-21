from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import pandas as pd
import os
from typing import List, Optional
from pydantic import BaseModel, ConfigDict
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

# Pydantic models for API responses - FIXED NAMESPACE ISSUES
class EarthquakeData(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
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

class ClusterData(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    id: str
    cluster_id: int
    cluster_label: str
    risk_zone: str
    centroid_lat: float
    centroid_lon: float
    cluster_size: int
    avg_magnitude: float

class HazardZone(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    zone_id: int
    risk_level: str
    avg_magnitude: float
    event_count: int
    center_lat: float
    center_lon: float
    boundary_coordinates: Optional[str] = ""

class RiskClassification(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    earthquake_id: str
    classified_risk_zone: str
    classification_confidence: float
    model_version: str
    created_at: datetime

class ModelMetrics(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    model_name: str
    model_type: str
    accuracy: float
    precision_score: float
    recall_score: float
    f1_score: float
    training_samples: int

class SystemStats(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
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

@app.get("/", summary="API Health Check")
async def root():
    """Health check endpoint with system information"""
    db_status = "connected" if engine else "disconnected"
    
    return {
        "message": "Magnitudr Earthquake Analysis API",
        "status": "operational",
        "database": db_status,
        "version": "1.0.0",
        "architecture": "Spark + DBSCAN + ML Hybrid",
        "documentation": "/docs",
        "endpoints": {
            "earthquakes": "/earthquakes",
            "clusters": "/clusters", 
            "hazard-zones": "/hazard-zones",
            "risk-classifications": "/risk-classifications",
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
            "architecture": "Spark + ML Hybrid",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@app.get("/earthquakes", tags=["Data"])
def get_earthquakes(limit: int = 100):
    """Mendapatkan daftar data gempa yang telah diproses."""
    if engine is None:
        raise HTTPException(status_code=503, detail="Koneksi database tidak tersedia.")

    query = text("""
        SELECT 
            id, magnitude, latitude, longitude, depth, time, place,
            spatial_density, hazard_score, region,
            magnitude_category, depth_category, risk_zone
        FROM earthquakes_processed 
        ORDER BY time DESC 
        LIMIT :limit
    """)

    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            return df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error saat mengambil data gempa: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Terjadi kesalahan internal: {e}")


@app.get("/clusters", tags=["Data"])
def get_clusters():
    """Mendapatkan ringkasan data klaster gempa."""
    if engine is None:
        raise HTTPException(status_code=503, detail="Koneksi database tidak tersedia.")

    query = text("""
        SELECT 
            cluster_id, 
            cluster_label,
            risk_zone,
            centroid_lat,
            centroid_lon,
            cluster_size,
            avg_magnitude,
            max_magnitude 
        FROM earthquake_clusters
    """)

    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            df_unique = df.drop_duplicates(subset=['cluster_id']).reset_index(drop=True)
            return df_unique.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error saat mengambil data klaster: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Terjadi kesalahan internal: {e}")

@app.get("/risk-classifications", response_model=List[RiskClassification], summary="Get ML Risk Classifications")
async def get_risk_classifications(
    limit: int = Query(default=1000, le=10000, description="Maximum number of classifications"),
    min_confidence: Optional[float] = Query(default=None, description="Minimum classification confidence")
):
    """
    🤖 **Get ML model risk classifications**
    
    Returns ML-generated risk zone classifications with confidence scores.
    """
    try:
        query = """
        SELECT 
            earthquake_id, classified_risk_zone, classification_confidence, 
            model_version, created_at
        FROM earthquake_risk_classifications
        WHERE 1=1
        """
        
        params = {}
        
        if min_confidence is not None:
            query += " AND classification_confidence >= :min_confidence"
            params['min_confidence'] = min_confidence
        
        query += " ORDER BY classification_confidence DESC LIMIT :limit"
        params['limit'] = limit
        
        with get_db_connection() as conn:
            result = conn.execute(text(query), params)
            data = result.fetchall()
        
        classifications = []
        for row in data:
            classifications.append(RiskClassification(
                earthquake_id=str(row[0]),
                classified_risk_zone=str(row[1]),
                classification_confidence=safe_float(row[2]),
                model_version=str(row[3]),
                created_at=row[4] if row[4] else datetime.now()
            ))
        
        logger.info(f"✅ Retrieved {len(classifications)} risk classifications")
        return classifications
        
    except Exception as e:
        logger.error(f"❌ Failed to retrieve risk classifications: {e}")
        raise HTTPException(status_code=500, detail=f"Risk classifications query failed: {str(e)}")

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


@app.get("/stats", tags=["Statistics"])
def get_stats():
    """Mendapatkan statistik ringkasan dari data gempa."""
    if engine is None:
        raise HTTPException(status_code=503, detail="Koneksi database tidak tersedia.")

    try:
        with engine.connect() as connection:
            total_earthquakes = connection.execute(text("SELECT COUNT(*) FROM earthquakes_processed")).scalar()
            total_clusters = connection.execute(
                text("SELECT COUNT(DISTINCT cluster_id) FROM earthquake_clusters")).scalar()

            risk_distribution_query = text(
                "SELECT classified_risk_zone, COUNT(*) as count FROM earthquake_risk_classifications GROUP BY classified_risk_zone")
            risk_distribution = pd.read_sql(risk_distribution_query, connection).to_dict('records')

            return {
                "total_earthquakes": total_earthquakes,
                "total_clusters": total_clusters,
                "risk_distribution": risk_distribution
            }
    except Exception as e:
        logger.error(f"Error saat mengambil statistik: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Terjadi kesalahan internal: {e}")

@app.get("/ml/model-comparison", summary="Get ML Model Comparison Results")
async def get_model_comparison():
    """Get comprehensive ML model comparison results"""
    try:
        import json
        from pathlib import Path
        
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
                "total_classifications": comparison_data.get('total_classifications_generated', 0)
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
    """Get comprehensive pipeline status and health"""
    try:
        with get_db_connection() as conn:
            # Check data volume
            result = conn.execute(text("SELECT COUNT(*) FROM earthquakes_processed")).fetchone()
            earthquake_count = safe_int(result[0]) if result else 0
            
            estimated_size_mb = (earthquake_count * 2) / 1024 if earthquake_count > 0 else 0
            
            # Check cluster status
            cluster_result = conn.execute(text("SELECT COUNT(DISTINCT cluster_id) FROM earthquake_clusters WHERE cluster_id IS NOT NULL")).fetchone()
            cluster_count = safe_int(cluster_result[0]) if cluster_result else 0
            
            # Check ML model status
            ml_result = conn.execute(text("SELECT COUNT(*) FROM ml_model_metadata")).fetchone()
            ml_models_count = safe_int(ml_result[0]) if ml_result else 0
            
            # Check risk classifications
            class_result = conn.execute(text("SELECT COUNT(*) FROM earthquake_risk_classifications")).fetchone()
            classifications_count = safe_int(class_result[0]) if class_result else 0
        
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
                "classifications_generated": classifications_count,
                "ml_ready": ml_models_count > 0
            },
            "system_health": {
                "database_connected": True,
                "api_operational": True,
                "spark_integrated": True,
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
    """Verify 64MB data requirement compliance"""
    try:
        with get_db_connection() as conn:
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
                estimated_size_mb = (result[0] * 2) / 1024
                
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
                        "target_size_mb": 64,
                        "processing_method": "Apache Spark"
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
