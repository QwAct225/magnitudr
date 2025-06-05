
                SELECT 
                    depth_category_detailed,
                    COUNT(*) as count,
                    ROUND(AVG(magnitude)::numeric, 2) as avg_magnitude,
                    ROUND(AVG(depth)::numeric, 1) as avg_depth,
                    ROUND(AVG(risk_score)::numeric, 3) as avg_risk_score
                FROM earthquakes_spark_processed
                WHERE depth_category_detailed IS NOT NULL
                GROUP BY depth_category_detailed
                ORDER BY avg_depth
            