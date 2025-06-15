-- Migration script to rename earthquake_predictions to earthquake_risk_classifications
-- Run this to fix misleading table name

-- Step 1: Rename the table
ALTER TABLE earthquake_predictions RENAME TO earthquake_risk_classifications;

-- Step 2: Rename columns for clarity (optional but recommended)
ALTER TABLE earthquake_risk_classifications 
    RENAME COLUMN predicted_risk_zone TO classified_risk_zone;

ALTER TABLE earthquake_risk_classifications 
    RENAME COLUMN prediction_confidence TO classification_confidence;

-- Step 3: Update any indexes or constraints if needed
-- (None needed for our current schema)

-- Step 4: Verify the migration
SELECT 
    table_name, 
    column_name, 
    data_type 
FROM information_schema.columns 
WHERE table_name = 'earthquake_risk_classifications'
ORDER BY ordinal_position;

-- Expected output should show:
-- earthquake_risk_classifications | earthquake_id | character varying
-- earthquake_risk_classifications | classified_risk_zone | character varying  
-- earthquake_risk_classifications | classification_confidence | numeric
-- earthquake_risk_classifications | model_version | character varying
-- earthquake_risk_classifications | created_at | timestamp without time zone
