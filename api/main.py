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

# Pydantic models for API responses
class EarthquakeData(BaseModel):
    id: str
    magnitude: float
    latitude: float
    longitude: float
    depth: float
    time: Optional[datetime] = None  # Make datetime optional
    place: Optional[str] = ""
    spatial_density: Optional[float] = None
    hazard_score: Optional[float] = None
    region: Optional[str] = None
    magnitude_category: Optional[str] = None
    depth_category: Optional[str] = None
    risk_zone: Optional[str] = None

    class Config:
        # Allow None values for datetime
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
            "health": "/health"
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
                time=row[5] if row[5] is not None else None,  # Handle None time
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

@app.get("/regions", summary="Get Available Regions")
async def get_regions():
    """Get list of available Indonesian regions"""
    try:
        with get_db_connection() as conn:
            result = conn.execute(text("""
                SELECT 
                    region,
                    COUNT(*) as event_count,
                    AVG(magnitude) as avg_magnitude,
                    MAX(magnitude) as max_magnitude
                FROM earthquakes_processed 
                GROUP BY region
                ORDER BY event_count DESC
            """))
            
            regions = []
            for row in result:
                regions.append({
                    "region": str(row[0]) if row[0] else "Unknown",
                    "event_count": safe_int(row[1]),
                    "avg_magnitude": round(safe_float(row[2]), 2),
                    "max_magnitude": safe_float(row[3])
                })
        
        logger.info(f"✅ Retrieved {len(regions)} regions")
        return {"regions": regions}
        
    except Exception as e:
class SystemStats(BaseModel):
    total_earthquakes: int
    total_clusters: int
    high_risk_zones: int
    last_update: datetime
    data_quality_score: float
    ml_model_available: bool = False

@app.get("/ml/predictions", response_model=List[MLPrediction], summary="Get ML Predictions")
async def get_ml_predictions(
    limit: int = Query(default=1000, le=10000, description="Maximum number of predictions"),
    min_confidence: Optional[float] = Query(default=None, description="Minimum prediction confidence")
):
    """
    🤖 **Get ML model predictions for earthquake risk zones**
    
    - **limit**: Maximum predictions to return
    - **min_confidence**: Filter by minimum prediction confidence (0.0-1.0)
    
    Returns ML-generated risk zone predictions with confidence scores.
    """
    try:
        query = """
        SELECT 
            earthquake_id, predicted_risk_zone, prediction_confidence, 
            model_version, created_at
        FROM earthquake_predictions
        WHERE 1=1
        """
        
        params = {}
        
        if min_confidence is not None:
            query += " AND prediction_confidence >= :min_confidence"
            params['min_confidence'] = min_confidence
        
        query += " ORDER BY prediction_confidence DESC LIMIT :limit"
        params['limit'] = limit
        
        with get_db_connection() as conn:
            result = conn.execute(text(query), params)
            data = result.fetchall()
        
        predictions = []
        for row in data:
            predictions.append(MLPrediction(
                earthquake_id=str(row[0]),
                predicted_risk_zone=str(row[1]),
                prediction_confidence=safe_float(row[2]),
                model_version=str(row[3]),
                created_at=row[4] if row[4] else datetime.now()
            ))
        
        logger.info(f"✅ Retrieved {len(predictions)} ML predictions")
        return predictions
        
    except Exception as e:
        logger.error(f"❌ Failed to retrieve ML predictions: {e}")
        raise HTTPException(status_code=500, detail=f"ML predictions query failed: {str(e)}")

@app.get("/ml/model-metrics", response_model=List[ModelMetrics], summary="Get ML Model Performance")
async def get_model_metrics():
    """
    📊 **Get ML model performance metrics**
    
    Returns training metrics including accuracy, precision, recall, F1-score.
    """
    try:
        query = """
        SELECT 
            model_name, model_type, accuracy, precision_score, 
            recall_score, f1_score, training_samples
        FROM ml_model_metadata
        ORDER BY created_at DESC
        """
        
        with get_db_connection() as conn:
            result = conn.execute(text(query))
            data = result.fetchall()
        
        metrics = []
        for row in data:
            metrics.append(ModelMetrics(
                model_name=str(row[0]),
                model_type=str(row[1]),
                accuracy=safe_float(row[2]),
                precision_score=safe_float(row[3]),
                recall_score=safe_float(row[4]),
                f1_score=safe_float(row[5]),
                training_samples=safe_int(row[6])
            ))
        
        logger.info(f"✅ Retrieved {len(metrics)} model metrics")
        return metrics
        
    except Exception as e:
        logger.error(f"❌ Failed to retrieve model metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Model metrics query failed: {str(e)}")

@app.post("/ml/predict", summary="Predict Risk Zone for Coordinates")
async def predict_risk_zone(
    latitude: float = Query(..., description="Latitude coordinate"),
    longitude: float = Query(..., description="Longitude coordinate"),
    magnitude: float = Query(default=4.0, description="Earthquake magnitude"),
    depth: float = Query(default=50.0, description="Earthquake depth in km")
):
    """
    🎯 **Predict risk zone for given coordinates**
    
    - **latitude**: Geographic latitude
    - **longitude**: Geographic longitude  
    - **magnitude**: Expected earthquake magnitude (default: 4.0)
    - **depth**: Expected earthquake depth in km (default: 50.0)
    
    Returns predicted risk zone with confidence score.
    """
    try:
        import joblib
        import numpy as np
        import pandas as pd
        
        # Load model components
        model_path = '/opt/airflow/magnitudr/data/airflow_output/earthquake_risk_model.pkl'
        scaler_path = '/opt/airflow/magnitudr/data/airflow_output/earthquake_risk_model_scaler.pkl'
        encoder_path = '/opt/airflow/magnitudr/data/airflow_output/earthquake_risk_model_label_encoder.pkl'
        
        if not all(os.path.exists(p) for p in [model_path, scaler_path, encoder_path]):
            raise HTTPException(status_code=503, detail="ML model not available. Please train model first.")
        
        model = joblib.load(model_path)
        scaler = joblib.load(scaler_path)
        label_encoder = joblib.load(encoder_path)
        
        # Prepare features (same as training)
        features = {
            'latitude': latitude,
            'longitude': longitude, 
            'magnitude': magnitude,
            'depth': depth,
            'spatial_density': 0.1,  # Default value
            'hazard_score': magnitude * 2,  # Estimated
            'distance_from_jakarta': np.sqrt((latitude + 6.2088) ** 2 + (longitude - 106.8456) ** 2),
            'distance_from_ring_of_fire': min(abs(latitude + 5), abs(longitude - 120)),
            'magnitude_depth_ratio': magnitude / (depth + 1),
            'energy_density': 0.1 * (magnitude ** 2),
            'shallow_earthquake': 1 if depth < 70 else 0,
            'high_magnitude': 1 if magnitude > 5.0 else 0,
            'lat_lon_interaction': latitude * longitude,
            'hazard_spatial_interaction': (magnitude * 2) * 0.1
        }
        
        # Convert to DataFrame and scale
        X = pd.DataFrame([features])
        X_scaled = scaler.transform(X)
        
        # Make prediction
        prediction = model.predict(X_scaled)[0]
        probabilities = model.predict_proba(X_scaled)[0]
        confidence = float(np.max(probabilities))
        
        # Decode prediction
        predicted_zone = label_encoder.inverse_transform([prediction])[0]
        
        return {
            "coordinates": {"latitude": latitude, "longitude": longitude},
            "input_parameters": {"magnitude": magnitude, "depth": depth},
            "predicted_risk_zone": predicted_zone,
            "prediction_confidence": round(confidence, 4),
            "model_version": "v1.0",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Risk zone prediction failed: {e}")
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
