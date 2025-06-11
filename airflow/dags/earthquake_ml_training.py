from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add custom operators to path
sys.path.append('/opt/airflow/operators')

default_args = {
    'owner': 'magnitudr-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 8),  # Sunday
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'catchup': False
}

dag = DAG(
    'earthquake_weekly_ml_training',
    default_args=default_args,
    description='🤖 Weekly ML training with regularization - prevents overfitting',
    schedule_interval='0 2 * * 0',  # Weekly Sunday 02:00 WIB (19:00 UTC Saturday)
    max_active_runs=1,
    tags=['earthquake', 'ml', 'weekly', 'training']
)

def train_regularized_model(**context):
    """Weekly ML training with regularization to prevent overfitting"""
    import pandas as pd
    import numpy as np
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import StratifiedKFold, GridSearchCV, train_test_split
    from sklearn.preprocessing import StandardScaler, LabelEncoder
    from sklearn.metrics import classification_report, confusion_matrix, f1_score, precision_score, recall_score, accuracy_score
    from sqlalchemy import create_engine, text
    import joblib
    import json
    import logging
    from pathlib import Path
    
    logging.info("🤖 Starting Weekly ML Training with Regularization...")
    
    try:
        # Load training data
        engine = create_engine('postgresql://postgres:earthquake123@postgres:5432/magnitudr')
        
        query = """
        SELECT 
            e.latitude, e.longitude, e.magnitude, e.depth,
            e.spatial_density, e.hazard_score,
            c.risk_zone, c.cluster_size, c.avg_magnitude
        FROM earthquakes_processed e
        INNER JOIN earthquake_clusters c ON e.id = c.id
        WHERE c.risk_zone IS NOT NULL
        """
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        logging.info(f"📊 Loaded {len(df)} training samples")
        
        # Feature engineering
        feature_cols = ['latitude', 'longitude', 'magnitude', 'depth', 'spatial_density', 'hazard_score']
        X = df[feature_cols].copy()
        
        # Add engineered features
        X['distance_from_jakarta'] = np.sqrt((X['latitude'] + 6.2088) ** 2 + (X['longitude'] - 106.8456) ** 2)
        X['magnitude_depth_ratio'] = X['magnitude'] / (X['depth'] + 1)
        X['energy_density'] = X['spatial_density'] * (X['magnitude'] ** 2)
        X['shallow_earthquake'] = (X['depth'] < 70).astype(int)
        
        # Handle missing values
        X = X.fillna(X.median())
        
        # Prepare labels
        y = df['risk_zone'].copy()
        label_encoder = LabelEncoder()
        y_encoded = label_encoder.fit_transform(y)
        
        # Log class distribution
        unique, counts = np.unique(y, return_counts=True)
        class_dist = dict(zip(unique, counts))
        logging.info(f"🏷️ Class distribution: {class_dist}")
        
        # Train-test split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y_encoded, test_size=0.2, random_state=42, stratify=y_encoded
        )
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # REGULARIZED Parameter Grid (Prevent Overfitting)
        param_grid = {
            'n_estimators': [50, 100],                    # Reduced complexity
            'max_depth': [5, 10, 15],                     # Limited depth
            'min_samples_split': [10, 20],                # Higher minimum
            'min_samples_leaf': [5, 10],                  # Higher minimum  
            'max_features': ['sqrt'],                     # Feature selection
            'class_weight': ['balanced']                  # Handle imbalance
        }
        
        # Reduced CV for faster training
        kfold = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)
        
        # Grid Search with regularization
        rf = RandomForestClassifier(random_state=42)
        grid_search = GridSearchCV(
            estimator=rf,
            param_grid=param_grid,
            cv=kfold,
            scoring='f1_weighted',
            n_jobs=1,  # Single job for container
            verbose=1
        )
        
        logging.info(f"🔍 Training {len(param_grid['n_estimators']) * len(param_grid['max_depth']) * len(param_grid['min_samples_split']) * len(param_grid['min_samples_leaf']) * len(param_grid['max_features']) * len(param_grid['class_weight'])} models with 3-fold CV...")
        
        # Fit grid search
        grid_search.fit(X_train_scaled, y_train)
        
        logging.info(f"🏆 Best parameters: {grid_search.best_params_}")
        logging.info(f"🏆 Best CV score: {grid_search.best_score_:.4f}")
        
        # Final model evaluation
        best_model = grid_search.best_estimator_
        y_pred_train = best_model.predict(X_train_scaled)
        y_pred_test = best_model.predict(X_test_scaled)
        
        # Calculate metrics
        train_accuracy = accuracy_score(y_train, y_pred_train)
        test_accuracy = accuracy_score(y_test, y_pred_test)
        test_precision = precision_score(y_test, y_pred_test, average='weighted')
        test_recall = recall_score(y_test, y_pred_test, average='weighted')
        test_f1 = f1_score(y_test, y_pred_test, average='weighted')
        
        # Overfitting check
        overfitting_gap = train_accuracy - test_accuracy
        
        logging.info(f"📊 Regularized Model Performance:")
        logging.info(f"   • Train Accuracy: {train_accuracy:.4f}")
        logging.info(f"   • Test Accuracy: {test_accuracy:.4f}")
        logging.info(f"   • Overfitting Gap: {overfitting_gap:.4f}")
        logging.info(f"   • Precision: {test_precision:.4f}")
        logging.info(f"   • Recall: {test_recall:.4f}")
        logging.info(f"   • F1-Score: {test_f1:.4f}")
        
        # Expected range: 91-96% as requested
        if 0.91 <= test_accuracy <= 0.96:
            logging.info("✅ Model performance within expected range (91-96%)")
        else:
            logging.warning(f"⚠️ Model performance outside expected range: {test_accuracy:.4f}")
        
        # Save model and components
        output_dir = Path("/opt/airflow/magnitudr/data/airflow_output")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        joblib.dump(best_model, output_dir / "earthquake_risk_model_regularized.pkl")
        joblib.dump(scaler, output_dir / "earthquake_risk_model_regularized_scaler.pkl")
        joblib.dump(label_encoder, output_dir / "earthquake_risk_model_regularized_label_encoder.pkl")
        
        # Save metrics
        metrics = {
            'model_type': 'RandomForestClassifier_Regularized',
            'train_accuracy': train_accuracy,
            'test_accuracy': test_accuracy,
            'overfitting_gap': overfitting_gap,
            'precision': test_precision,
            'recall': test_recall,
            'f1_score': test_f1,
            'best_params': grid_search.best_params_,
            'class_distribution': class_dist,
            'training_samples': len(X_train),
            'test_samples': len(X_test),
            'features_count': len(X.columns),
            'training_timestamp': datetime.now().isoformat()
        }
        
        with open(output_dir / "earthquake_risk_model_regularized_metrics.json", 'w') as f:
            json.dump(metrics, f, indent=2)
        
        logging.info("✅ Regularized model training completed successfully")
        return test_accuracy
        
    except Exception as e:
        logging.error(f"❌ ML training failed: {e}")
        raise

task_regularized_ml = PythonOperator(
    task_id='train_regularized_model',
    python_callable=train_regularized_model,
    dag=dag
)

# Single task for weekly execution
task_regularized_ml
