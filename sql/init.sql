-- Production-ready database schema for earthquake analysis
-- PostGIS extension (optional - only if needed for spatial operations)
CREATE EXTENSION IF NOT EXISTS postgis;

-- Table 1: Processed Earthquake Data (after ETL)
CREATE TABLE IF NOT EXISTS earthquakes_processed (
    id VARCHAR(100) PRIMARY KEY,
    magnitude DECIMAL(4,2),
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7), 
    depth DECIMAL(8,3),
    time TIMESTAMP WITH TIME ZONE,
    place TEXT,
    spatial_density DECIMAL(10,6),
    hazard_score DECIMAL(6,3),
    region VARCHAR(50),
    magnitude_category VARCHAR(20),
    depth_category VARCHAR(20),
    -- --- TAMBAHAN KOLOM BARU DI SINI ---
    cluster_id INTEGER,         -- Kolom untuk ID kluster dari DBSCAN
    risk_zone VARCHAR(20),      -- Kolom untuk zona risiko
    -- ---------------------------------
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 2: DBSCAN Cluster Results
CREATE TABLE IF NOT EXISTS earthquake_clusters (
    id VARCHAR(100) REFERENCES earthquakes_processed(id),
    cluster_id INTEGER,
    cluster_label VARCHAR(50),
    risk_zone VARCHAR(20),
    centroid_lat DECIMAL(10,7),
    centroid_lon DECIMAL(10,7),
    cluster_size INTEGER,
    avg_magnitude DECIMAL(4,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, cluster_id)
);

-- Table 3: Aggregated Hazard Zones
CREATE TABLE IF NOT EXISTS hazard_zones (
    zone_id SERIAL PRIMARY KEY,
    risk_level VARCHAR(20),
    avg_magnitude DECIMAL(4,2),
    event_count INTEGER,
    boundary_coordinates JSON,
    center_lat DECIMAL(10,7),
    center_lon DECIMAL(10,7),
    area_km2 DECIMAL(10,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 4: ML Model Predictions (diubah namanya di DAG ML Training)
CREATE TABLE IF NOT EXISTS earthquake_predictions ( -- Ini mungkin earthquake_risk_classifications sekarang
    earthquake_id VARCHAR(100) REFERENCES earthquakes_processed(id),
    predicted_risk_zone VARCHAR(20),
    prediction_confidence DECIMAL(6,4),
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (earthquake_id, model_version)
);

-- Table 5: ML Model Metadata
CREATE TABLE IF NOT EXISTS ml_model_metadata (
    model_name VARCHAR(100),
    model_type VARCHAR(50),
    accuracy DECIMAL(6,4),
    precision_score DECIMAL(6,4),
    recall_score DECIMAL(6,4),
    f1_score DECIMAL(6,4),
    training_samples INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (model_name, created_at)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_earthquakes_location ON earthquakes_processed(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_earthquakes_magnitude ON earthquakes_processed(magnitude);
CREATE INDEX IF NOT EXISTS idx_earthquakes_time ON earthquakes_processed(time);
CREATE INDEX IF NOT EXISTS idx_clusters_zone ON earthquake_clusters(risk_zone);
CREATE INDEX IF NOT EXISTS idx_hazard_risk ON hazard_zones(risk_level);
CREATE INDEX IF NOT EXISTS idx_predictions_confidence ON earthquake_predictions(prediction_confidence);
CREATE INDEX IF NOT EXISTS idx_model_metadata_performance ON ml_model_metadata(f1_score);

-- Create a view for easy dashboard queries
CREATE OR REPLACE VIEW earthquake_summary AS
SELECT 
    e.id,
    e.magnitude,
    e.latitude,
    e.longitude,
    e.depth,
    e.region,
    e.hazard_score,
    c.risk_zone, -- Ini akan join dari earthquake_clusters, bukan kolom e.risk_zone
    e.risk_zone AS processed_risk_zone_initial, -- Tambahkan ini jika ingin melihat nilai initial di e
    e.cluster_id AS processed_cluster_id,
    c.cluster_label,
    p.predicted_risk_zone,
    p.prediction_confidence
FROM earthquakes_processed e
LEFT JOIN earthquake_clusters c ON e.id = c.id
LEFT JOIN earthquake_predictions p ON e.id = p.earthquake_id;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;

-- Clear any existing demo data (production safety)
DELETE FROM earthquake_clusters WHERE id LIKE 'demo_%';
DELETE FROM earthquake_predictions WHERE earthquake_id LIKE 'demo_%';
DELETE FROM hazard_zones WHERE risk_level = 'Demo';
DELETE FROM earthquakes_processed WHERE id LIKE 'demo_%';