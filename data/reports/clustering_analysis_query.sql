
                SELECT
                    COALESCE(earthquake_cluster, -1) AS earthquake_cluster,
                    COUNT(*) AS cluster_size,
                    ROUND(AVG(magnitude)::numeric, 2) AS avg_magnitude,
                    ROUND(AVG(depth)::numeric, 1) AS avg_depth,
                    ROUND(AVG(risk_score)::numeric, 3) AS avg_risk,
                    geographic_zone,
                    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS percentage
                FROM earthquakes_spark_processed
                GROUP BY earthquake_cluster, geographic_zone
                ORDER BY cluster_size DESC
            