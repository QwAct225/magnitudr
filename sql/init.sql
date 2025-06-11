-- Create database schema for earthquake analysis
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

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_earthquakes_location ON earthquakes_processed(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_earthquakes_magnitude ON earthquakes_processed(magnitude);
CREATE INDEX IF NOT EXISTS idx_earthquakes_time ON earthquakes_processed(time);
CREATE INDEX IF NOT EXISTS idx_clusters_zone ON earthquake_clusters(risk_zone);
CREATE INDEX IF NOT EXISTS idx_hazard_risk ON hazard_zones(risk_level);

-- Sample data for testing
INSERT INTO earthquakes_processed (id, magnitude, latitude, longitude, depth, time, place, spatial_density, hazard_score, region, magnitude_category, depth_category)
VALUES 
    ('demo_001', 5.2, -7.5, 110.2, 45.5, '2025-06-01 10:00:00+07', 'Central Java Demo', 0.15, 7.8, 'Java', 'Moderate', 'Shallow'),
    ('demo_002', 6.1, -0.5, 100.2, 15.2, '2025-06-01 11:00:00+07', 'Sumatra Demo', 0.23, 9.2, 'Sumatra', 'Strong', 'Very Shallow')
ON CONFLICT (id) DO NOTHING;
