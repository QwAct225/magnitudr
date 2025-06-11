from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import pandas as pd
import os
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:earthquake123@postgres:5432/magnitudr")
engine = create_engine(DATABASE_URL)

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
    allow_origins=["*"],  # In production, specify exact origins
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
    time: datetime
    place: str
    spatial_density: Optional[float] = None
    hazard_score: Optional[float] = None
    region: Optional[str] = None
    magnitude_category: Optional[str] = None
    depth_category: Optional[str] = None

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
    boundary_coordinates: str

class SystemStats(BaseModel):
    total_earthquakes: int
    total_clusters: int
    high_risk_zones: int
    last_update: datetime
    data_quality_score: float

# Database dependency
def get_db_connection():
    try:
        return engine.connect()
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

# API Endpoints
@app.get("/", summary="API Health Check")
async def root():
    """Health check endpoint with system information"""
    return {
        "message": "🌍 Magnitudr Earthquake Analysis API",
        "status": "operational",
        "version": "1.0.0",
        "documentation": "/docs",
        "endpoints": {
            "earthquakes": "/earthquakes",
            "clusters": "/clusters", 
            "hazard-zones": "/hazard-zones",
            "statistics": "/stats"
        }
    }

@app.get("/earthquakes", response_model=List[EarthquakeData], summary="Get Earthquake Data")
async def get_earthquakes(
    limit: int = Query(default=1000, le=10000, description="Maximum number of records"),
    min_magnitude: Optional[float] = Query(default=None, description="Minimum magnitude filter"),
    region: Optional[str] = Query(default=None, description="Region filter"),
    risk_zone: Optional[str] = Query(default=None, description="Risk zone filter")
):
    """
    🌍 **Get earthquake data with advanced filtering**
    
    - **limit**: Maximum records to return (default: 1000, max: 10000)
    - **min_magnitude**: Filter by minimum magnitude
    - **region**: Filter by Indonesian region (Java, Sumatra, Sulawesi, etc.)
    - **risk_zone**: Filter by risk assessment (Low, Moderate, High, Extreme)
    
    Returns processed earthquake data with spatial density and hazard scores.
    """
    try:
        # Build dynamic query
        query = """
        SELECT 
            e.id, e.magnitude, e.latitude, e.longitude, e.depth, e.time,
            e.place, e.spatial_density, e.hazard_score, e.region,
            e.magnitude_category, e.depth_category
        FROM earthquakes_processed e
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
        
        query += " ORDER BY e.time DESC LIMIT :limit"
        params['limit'] = limit
        
        with get_db_connection() as conn:
            result = conn.execute(text(query), params)
            data = result.fetchall()
            
        earthquakes = []
        for row in data:
            earthquakes.append(EarthquakeData(
                id=row[0],
                magnitude=row[1],
                latitude=row[2], 
                longitude=row[3],
                depth=row[4],
                time=row[5],
                place=row[6],
                spatial_density=row[7],
                hazard_score=row[8],
                region=row[9],
                magnitude_category=row[10],
                depth_category=row[11]
            ))
        
        logger.info(f"✅ Retrieved {len(earthquakes)} earthquake records")
        return earthquakes
        
    except Exception as e:
        logger.error(f"❌ Failed to retrieve earthquakes: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/clusters", response_model=List[ClusterData], summary="Get Cluster Analysis")
async def get_clusters(
    risk_zone: Optional[str] = Query(default=None, description="Filter by risk zone"),
    min_cluster_size: Optional[int] = Query(default=None, description="Minimum cluster size")
):
    """
    🔬 **Get DBSCAN clustering results**
    
    - **risk_zone**: Filter by risk assessment (Low, Moderate, High, Extreme)
    - **min_cluster_size**: Minimum number of events in cluster
    
    Returns spatial clusters with risk zone analysis and centroid coordinates.
    """
    try:
        query = """
        SELECT DISTINCT
            c.id, c.cluster_id, c.cluster_label, c.risk_zone,
            c.centroid_lat, c.centroid_lon, c.cluster_size, c.avg_magnitude
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
        
        query += " ORDER BY c.cluster_size DESC"
        
        with get_db_connection() as conn:
            result = conn.execute(text(query), params)
            data = result.fetchall()
        
        clusters = []
        for row in data:
            clusters.append(ClusterData(
                id=row[0],
                cluster_id=row[1],
                cluster_label=row[2],
                risk_zone=row[3],
                centroid_lat=row[4],
                centroid_lon=row[5],
                cluster_size=row[6],
                avg_magnitude=row[7]
            ))
        
        logger.info(f"✅ Retrieved {len(clusters)} cluster records")
        return clusters
        
    except Exception as e:
        logger.error(f"❌ Failed to retrieve clusters: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/hazard-zones", response_model=List[HazardZone], summary="Get Hazard Zones")
async def get_hazard_zones(
    risk_level: Optional[str] = Query(default=None, description="Filter by risk level")
):
    """
    🚨 **Get aggregated hazard zones for Indonesia**
    
    - **risk_level**: Filter by risk level (Low, Moderate, High, Extreme)
    
    Returns aggregated hazard zones with geographic boundaries and event statistics.
    """
    try:
        query = """
        SELECT 
            zone_id, risk_level, avg_magnitude, event_count,
            center_lat, center_lon, boundary_coordinates
        FROM hazard_zones
        WHERE 1=1
        """
        
        params = {}
        
        if risk_level:
            query += " AND risk_level = :risk_level"
            params['risk_level'] = risk_level
        
        query += " ORDER BY event_count DESC"
        
        with get_db_connection() as conn:
            result = conn.execute(text(query), params)
            data = result.fetchall()
        
        hazard_zones = []
        for row in data:
            hazard_zones.append(HazardZone(
                zone_id=row[0],
                risk_level=row[1],
                avg_magnitude=row[2],
                event_count=row[3],
                center_lat=row[4],
                center_lon=row[5],
                boundary_coordinates=row[6]
            ))
        
        logger.info(f"✅ Retrieved {len(hazard_zones)} hazard zones")
        return hazard_zones
        
    except Exception as e:
        logger.error(f"❌ Failed to retrieve hazard zones: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats", response_model=SystemStats, summary="Get System Statistics")
async def get_system_stats():
    """
    📊 **Get comprehensive system statistics**
    
    Returns overall system health and data quality metrics including:
    - Total earthquake records processed
    - Number of clusters identified  
    - High-risk zones count
    - Data quality assessment
    """
    try:
        with get_db_connection() as conn:
            # Get earthquake statistics
            earthquake_stats = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_earthquakes,
                    AVG(hazard_score) as avg_hazard_score
                FROM earthquakes_processed
            """)).fetchone()
            
            # Get cluster statistics
            cluster_stats = conn.execute(text("""
                SELECT 
                    COUNT(DISTINCT cluster_id) as total_clusters,
                    COUNT(*) as total_clustered_events
                FROM earthquake_clusters
            """)).fetchone()
            
            # Get high-risk zones count
            high_risk_count = conn.execute(text("""
                SELECT COUNT(*) as high_risk_zones
                FROM hazard_zones 
                WHERE risk_level IN ('High', 'Extreme')
            """)).fetchone()
            
            # Calculate data quality score
            data_quality = min(earthquake_stats[1] / 10.0, 1.0) if earthquake_stats[1] else 0.0
        
        stats = SystemStats(
            total_earthquakes=earthquake_stats[0] or 0,
            total_clusters=cluster_stats[0] or 0,
            high_risk_zones=high_risk_count[0] or 0,
            last_update=datetime.now(),
            data_quality_score=round(data_quality, 3)
        )
        
        logger.info(f"✅ Retrieved system statistics")
        return stats
        
    except Exception as e:
        logger.error(f"❌ Failed to retrieve statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/regions", summary="Get Available Regions")
async def get_regions():
    """
    🗺️ **Get list of available Indonesian regions**
    
    Returns all regions with earthquake data and their event counts.
    """
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
                    "region": row[0],
                    "event_count": row[1],
                    "avg_magnitude": round(row[2], 2),
                    "max_magnitude": row[3]
                })
        
        logger.info(f"✅ Retrieved {len(regions)} regions")
        return {"regions": regions}
        
    except Exception as e:
        logger.error(f"❌ Failed to retrieve regions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Enhanced data ingestion endpoint for 64MB requirement
@app.get("/data-volume", summary="Check Data Volume")
async def check_data_volume():
    """
    📈 **Check current data volume and ingestion status**
    
    Verifies if minimum 64MB data requirement is met.
    """
    try:
        with get_db_connection() as conn:
            # Get detailed data statistics
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as record_count,
                    MIN(time) as earliest_record,
                    MAX(time) as latest_record,
                    COUNT(DISTINCT region) as unique_regions,
                    AVG(magnitude) as avg_magnitude
                FROM earthquakes_processed
            """)).fetchone()
            
            # Estimate data size (rough calculation)
            estimated_size_mb = (result[0] * 500) / (1024 * 1024) if result[0] else 0  # ~500 bytes per record
            
        return {
            "record_count": result[0] or 0,
            "estimated_size_mb": round(estimated_size_mb, 2),
            "meets_64mb_requirement": estimated_size_mb >= 64,
            "earliest_record": result[1],
            "latest_record": result[2],
            "unique_regions": result[3] or 0,
            "avg_magnitude": round(result[4], 2) if result[4] else 0,
            "recommendation": "Increase time range in USGS ingestion if size < 64MB" if estimated_size_mb < 64 else "Data volume requirement satisfied"
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to check data volume: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
