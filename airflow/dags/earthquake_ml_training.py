from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add custom operators to path
sys.path.append('/opt/airflow/operators')
from ml_classification_operator import EarthquakeMLClassificationOperator

default_args = {
    'owner': 'magnitudr-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'earthquake_ml_training',
    default_args=default_args,
    description='🤖 Machine Learning training for earthquake risk zone classification',
    schedule_interval=None,  # Triggered manually or by master DAG
    max_active_runs=1,
    tags=['earthquake', 'ml', 'classification', 'training']
)

# Task 1: ML Model Training with K-Fold CV
task_ml_training = EarthquakeMLClassificationOperator(
    task_id='train_risk_classification_model',
    db_connection='postgresql://postgres:earthquake123@postgres:5432/magnitudr',
    model_output_path='/opt/airflow/magnitudr/data/airflow_output/earthquake_risk_model.pkl',
    test_size=0.2,
    cv_folds=5,
    random_state=42,
    dag=dag
)

def store_model_results_to_db(**context):
    """Store ML model results and predictions to database"""
    import pandas as pd
    import numpy as np
    import joblib
    import json
    from sqlalchemy import create_engine, text
    import logging
    
    try:
        # Load model and results
        model_path = '/opt/airflow/magnitudr/data/airflow_output/earthquake_risk_model.pkl'
        metrics_path = '/opt/airflow/magnitudr/data/airflow_output/earthquake_risk_model_metrics.json'
        
        if not os.path.exists(model_path):
            raise FileNotFoundError("Model file not found")
        
        # Load metrics
        with open(metrics_path, 'r') as f:
            metrics = json.load(f)
        
        # Store model metadata to database
        engine = create_engine('postgresql://postgres:earthquake123@postgres:5432/magnitudr')
        
        # Create model metadata table if not exists
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ml_model_metadata (
                    model_id SERIAL PRIMARY KEY,
                    model_name VARCHAR(100),
                    model_type VARCHAR(50),
                    accuracy DECIMAL(6,4),
                    precision_score DECIMAL(6,4),
                    recall_score DECIMAL(6,4),
                    f1_score DECIMAL(6,4),
                    training_samples INTEGER,
                    model_path TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Insert model metadata
            conn.execute(text("""
                INSERT INTO ml_model_metadata 
                (model_name, model_type, accuracy, precision_score, recall_score, f1_score, training_samples, model_path)
                VALUES 
                (:model_name, :model_type, :accuracy, :precision, :recall, :f1, :samples, :path)
            """), {
                'model_name': 'earthquake_risk_classifier',
                'model_type': 'RandomForestClassifier',
                'accuracy': metrics['accuracy'],
                'precision': metrics['precision'],
                'recall': metrics['recall'],
                'f1': metrics['f1_score'],
                'samples': int(metrics.get('training_data_size', 0)),
                'path': model_path
            })
        
        logging.info("✅ Model metadata stored to database")
        logging.info(f"📊 Model Performance: Accuracy={metrics['accuracy']:.4f}, F1={metrics['f1_score']:.4f}")
        
        return metrics['accuracy']
        
    except Exception as e:
        logging.error(f"❌ Failed to store model results: {e}")
        raise

task_store_results = PythonOperator(
    task_id='store_model_results',
    python_callable=store_model_results_to_db,
    dag=dag
)

def generate_ml_predictions(**context):
    """Generate predictions for all earthquake locations"""
    import pandas as pd
    import numpy as np
    import joblib
    from sqlalchemy import create_engine, text
    import logging
    
    try:
        # Load trained model and preprocessors
        model = joblib.load('/opt/airflow/magnitudr/data/airflow_output/earthquake_risk_model.pkl')
        scaler = joblib.load('/opt/airflow/magnitudr/data/airflow_output/earthquake_risk_model_scaler.pkl')
        label_encoder = joblib.load('/opt/airflow/magnitudr/data/airflow_output/earthquake_risk_model_label_encoder.pkl')
        
        # Load data for prediction
        engine = create_engine('postgresql://postgres:earthquake123@postgres:5432/magnitudr')
        
        query = """
        SELECT id, latitude, longitude, magnitude, depth, spatial_density, hazard_score
        FROM earthquakes_processed
        """
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        # Feature engineering (same as training)
        df_features = df[['latitude', 'longitude', 'magnitude', 'depth', 'spatial_density', 'hazard_score']].copy()
        
        # Add engineered features
        df_features['distance_from_jakarta'] = np.sqrt(
            (df_features['latitude'] + 6.2088) ** 2 + 
            (df_features['longitude'] - 106.8456) ** 2
        )
        
        df_features['distance_from_ring_of_fire'] = np.minimum(
            np.abs(df_features['latitude'] + 5),
            np.abs(df_features['longitude'] - 120)
        )
        
        df_features['magnitude_depth_ratio'] = df_features['magnitude'] / (df_features['depth'] + 1)
        df_features['energy_density'] = df_features['spatial_density'] * (df_features['magnitude'] ** 2)
        df_features['shallow_earthquake'] = (df_features['depth'] < 70).astype(int)
        df_features['high_magnitude'] = (df_features['magnitude'] > 5.0).astype(int)
        df_features['lat_lon_interaction'] = df_features['latitude'] * df_features['longitude']
        df_features['hazard_spatial_interaction'] = df_features['hazard_score'] * df_features['spatial_density']
        
        # Handle missing values
        df_features = df_features.fillna(df_features.median())
        
        # Scale features
        X_scaled = scaler.transform(df_features)
        
        # Make predictions
        predictions = model.predict(X_scaled)
        prediction_probs = model.predict_proba(X_scaled)
        
        # Decode predictions
        predicted_risk_zones = label_encoder.inverse_transform(predictions)
        max_probabilities = np.max(prediction_probs, axis=1)
        
        # Create predictions dataframe
        df_predictions = pd.DataFrame({
            'earthquake_id': df['id'],
            'predicted_risk_zone': predicted_risk_zones,
            'prediction_confidence': max_probabilities,
            'model_version': 'v1.0'
        })
        
        # Store predictions to database
        with engine.begin() as conn:
            # Create predictions table if not exists
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS earthquake_predictions (
                    prediction_id SERIAL PRIMARY KEY,
                    earthquake_id VARCHAR(100),
                    predicted_risk_zone VARCHAR(20),
                    prediction_confidence DECIMAL(6,4),
                    model_version VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (earthquake_id) REFERENCES earthquakes_processed(id)
                )
            """))
            
            # Clear existing predictions
            conn.execute(text("DELETE FROM earthquake_predictions"))
        
        # Insert new predictions
        df_predictions.to_sql('earthquake_predictions', engine, if_exists='append', index=False)
        
        logging.info(f"✅ Generated {len(df_predictions)} predictions")
        logging.info(f"📊 Risk zone distribution: {pd.Series(predicted_risk_zones).value_counts().to_dict()}")
        
        return len(df_predictions)
        
    except Exception as e:
        logging.error(f"❌ Failed to generate predictions: {e}")
        raise

task_generate_predictions = PythonOperator(
    task_id='generate_ml_predictions',
    python_callable=generate_ml_predictions,
    dag=dag
)

# Dependencies
task_ml_training >> task_store_results >> task_generate_predictions
