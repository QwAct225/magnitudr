
                SELECT 
                    id, magnitude, place, 
                    CAST(latitude AS numeric) as latitude,
                    CAST(longitude AS numeric) as longitude,
                    depth, geographic_zone, risk_score
                FROM earthquakes_spark_processed
                WHERE magnitude IS NOT NULL
                ORDER BY magnitude DESC
                LIMIT 10
            