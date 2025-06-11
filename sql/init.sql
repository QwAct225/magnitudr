-- Create database schema for earthquake analysis
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
    ('demo_002', 6.1, -0.5, 100.2, 15.2, '2025-06-01 11:00:00+07', 'Sumatra Demo', 0.23, 9.2, 'Sumatra', 'Strong', 'Very Shallow'),
    ('demo_003', 4.8, -2.5, 128.3, 85.2, '2025-06-01 12:00:00+07', 'Sulawesi Demo', 0.12, 6.5, 'Sulawesi', 'Light', 'Intermediate'),
    ('demo_004', 5.8, -8.1, 115.2, 25.8, '2025-06-01 13:00:00+07', 'Bali Demo', 0.18, 8.4, 'Other', 'Moderate', 'Very Shallow'),
    ('demo_005', 4.2, 3.5, 98.7, 65.5, '2025-06-01 14:00:00+07', 'North Sumatra Demo', 0.08, 5.2, 'Sumatra', 'Light', 'Shallow')
ON CONFLICT (id) DO NOTHING;

-- Insert sample cluster data
INSERT INTO earthquake_clusters (id, cluster_id, cluster_label, risk_zone, centroid_lat, centroid_lon, cluster_size, avg_magnitude)
VALUES
    ('demo_001', 1, 'Cluster_1_High', 'High', -7.5, 110.2, 15, 5.2),
    ('demo_002', 2, 'Cluster_2_Extreme', 'Extreme', -0.5, 100.2, 8, 6.1),
    ('demo_003', 3, 'Cluster_3_Moderate', 'Moderate', -2.5, 128.3, 12, 4.8),
    ('demo_004', 1, 'Cluster_1_High', 'High', -8.1, 115.2, 15, 5.8),
    ('demo_005', 4, 'Cluster_4_Low', 'Low', 3.5, 98.7, 20, 4.2)
ON CONFLICT (id, cluster_id) DO NOTHING;

-- Insert sample hazard zones
INSERT INTO hazard_zones (risk_level, avg_magnitude, event_count, center_lat, center_lon, boundary_coordinates, area_km2)
VALUES
    ('Extreme', 6.1, 8, -0.5, 100.2, '{"type": "circle", "radius": 50}', 7854.0),
    ('High', 5.5, 30, -7.8, 112.7, '{"type": "circle", "radius": 75}', 17671.0),
    ('Moderate', 4.8, 45, -2.5, 128.3, '{"type": "circle", "radius": 60}', 11310.0),
    ('Low', 4.2, 85, 3.5, 98.7, '{"type": "circle", "radius": 40}', 5027.0)
ON CONFLICT DO NOTHING;

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
    c.risk_zone,
    c.cluster_id,
    c.cluster_label
FROM earthquakes_processed e
LEFT JOIN earthquake_clusters c ON e.id = c.id;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
