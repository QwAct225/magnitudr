
                SELECT
                    year,
                    month,
                    COUNT(*) AS monthly_count,
                    ROUND(AVG(magnitude)::numeric, 2) AS avg_magnitude,
                    COUNT(*) FILTER (WHERE magnitude >= 5.0) AS major_earthquakes
                FROM earthquakes_spark_processed
                WHERE year IS NOT NULL AND month IS NOT NULL
                GROUP BY year, month
                ORDER BY year, month;
            