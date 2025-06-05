
                SELECT 
                    geographic_zone,
                    COUNT(*) as earthquake_count,
                    ROUND(AVG(magnitude)::numeric, 2) as avg_magnitude,
                    ROUND(MAX(magnitude)::numeric, 2) as max_magnitude,
                    ROUND(AVG(risk_score)::numeric, 3) as avg_risk_score
                FROM earthquakes_spark_processed
                WHERE geographic_zone IS NOT NULL
                GROUP BY geographic_zone
                ORDER BY earthquake_count DESC
            