
                SELECT 
                    geographic_zone,
                    magnitude_category,
                    COUNT(*) as count,
                    ROUND(AVG(risk_score)::numeric, 3) as avg_risk_score,
                    ROUND(AVG(population_impact_estimate)::numeric, 0) as avg_population_impact
                FROM earthquakes_spark_processed
                WHERE risk_score > 0.7
                GROUP BY geographic_zone, magnitude_category
                ORDER BY avg_risk_score DESC
            