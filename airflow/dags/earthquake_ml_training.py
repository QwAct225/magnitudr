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
    description='🤖 Weekly ML training with baseline comparison and prediction storage',
    schedule_interval='0 2 * * 0',  # Weekly Sunday 02:00 WIB (19:00 UTC Saturday)
    max_active_runs=1,
    tags=['earthquake', 'ml', 'weekly', 'training', 'comparison']
)

def train_models_with_comparison(**context):
    """Train RandomForest + Logistic Regression with comparison and prediction storage"""
    import pandas as pd
    import numpy as np
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import StratifiedKFold, GridSearchCV, train_test_split
    from sklearn.preprocessing import StandardScaler, LabelEncoder
    from sklearn.metrics import classification_report, confusion_matrix, f1_score, precision_score, recall_score, accuracy_score, roc_auc_score
    from sqlalchemy import create_engine, text
    import joblib
    import json
    import logging
    from pathlib import Path
    import time
    
    logging.info("🤖 Starting Weekly ML Training with Model Comparison...")
    
    try:
        # Load training data
        engine = create_engine('postgresql://postgres:earthquake123@postgres:5432/magnitudr')
        
        query = """
        SELECT 
            e.id, e.latitude, e.longitude, e.magnitude, e.depth,
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
        earthquake_ids = df['id'].copy()  # Store for predictions
        
        # Add engineered features
        X['distance_from_jakarta'] = np.sqrt((X['latitude'] + 6.2088) ** 2 + (X['longitude'] - 106.8456) ** 2)
        X['distance_from_ring_of_fire'] = np.minimum(
            np.abs(X['latitude'] + 5), 
            np.abs(X['longitude'] - 120)
        )
        X['magnitude_depth_ratio'] = X['magnitude'] / (X['depth'] + 1)
        X['energy_density'] = X['spatial_density'] * (X['magnitude'] ** 2)
        X['shallow_earthquake'] = (X['depth'] < 70).astype(int)
        X['high_magnitude'] = (X['magnitude'] > 5.0).astype(int)
        X['lat_lon_interaction'] = X['latitude'] * X['longitude']
        X['hazard_spatial_interaction'] = X['hazard_score'] * X['spatial_density']
        
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
        X_train, X_test, y_train, y_test, ids_train, ids_test = train_test_split(
            X, y_encoded, earthquake_ids, test_size=0.2, random_state=42, stratify=y_encoded
        )
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # REGULARIZED RANDOM FOREST
        logging.info("🌲 Training Regularized Random Forest...")
        
        rf_param_grid = {
            'n_estimators': [30, 50],
            'max_depth': [3, 5, 8],
            'min_samples_split': [15, 25],
            'min_samples_leaf': [8, 12],
            'max_features': ['sqrt', 'log2'],
            'class_weight': ['balanced'],
            'min_impurity_decrease': [0.001, 0.01]
        }
        
        # Reduced CV for more regularization
        kfold = StratifiedKFold(n_splits=2, shuffle=True, random_state=42)
        
        rf = RandomForestClassifier(random_state=42)
        rf_grid_search = GridSearchCV(
            estimator=rf,
            param_grid=rf_param_grid,
            cv=kfold,
            scoring='f1_weighted',
            n_jobs=1,
            verbose=1
        )
        
        # Add noise to training data to increase difficulty
        noise_scale = 0.01  # 1% noise
        X_train_noisy = X_train_scaled.copy()
        for i in range(X_train_scaled.shape[1]):
            noise = np.random.normal(0, noise_scale, X_train_scaled.shape[0])
            X_train_noisy[:, i] += noise
        
        logging.info(f"🔄 Added {noise_scale*100}% noise to training data for regularization")
        
        rf_grid_search.fit(X_train_noisy, y_train)  # Train on noisy data
        best_rf = rf_grid_search.best_estimator_
        
        # REGULARIZED LOGISTIC REGRESSION
        logging.info("📊 Training Regularized Logistic Regression (Fast)...")
        
        lr_param_grid = {
            'C': [0.01, 0.1, 1.0],
            'penalty': ['l1', 'l2'],
            'solver': ['liblinear'],
            'class_weight': ['balanced'],
            'max_iter': [1000],
            'tol': [1e-4]
        }
        
        lr = LogisticRegression(random_state=42)
        lr_grid_search = GridSearchCV(
            estimator=lr,
            param_grid=lr_param_grid,
            cv=kfold,
            scoring='f1_weighted',
            n_jobs=1,
            verbose=1
        )
        
        # Train LR on same noisy data for fair comparison
        lr_grid_search.fit(X_train_noisy, y_train)
        best_lr = lr_grid_search.best_estimator_
        
        # MODEL EVALUATION & COMPARISON
        logging.info("📈 Evaluating and comparing models...")
        
        models = {
            'RandomForest': best_rf,
            'LogisticRegression': best_lr
        }
        
        model_comparison = {}
        
        for model_name, model in models.items():
            # Predictions
            y_pred_train = model.predict(X_train_scaled)
            y_pred_test = model.predict(X_test_scaled)
            y_pred_test_proba = model.predict_proba(X_test_scaled) if hasattr(model, 'predict_proba') else None
            
            # Calculate metrics
            train_accuracy = accuracy_score(y_train, y_pred_train)
            test_accuracy = accuracy_score(y_test, y_pred_test)
            test_precision = precision_score(y_test, y_pred_test, average='weighted')
            test_recall = recall_score(y_test, y_pred_test, average='weighted')
            test_f1 = f1_score(y_test, y_pred_test, average='weighted')
            
            # AUC score (multiclass)
            try:
                test_auc = roc_auc_score(y_test, y_pred_test_proba, multi_class='ovr', average='weighted') if y_pred_test_proba is not None else 0
            except:
                test_auc = 0
            
            overfitting_gap = train_accuracy - test_accuracy
            
            metrics = {
                'model_type': model_name,
                'train_accuracy': train_accuracy,
                'test_accuracy': test_accuracy,
                'overfitting_gap': overfitting_gap,
                'precision': test_precision,
                'recall': test_recall,
                'f1_score': test_f1,
                'auc_score': test_auc,
                'best_params': rf_grid_search.best_params_ if model_name.startswith('RandomForest') else lr_grid_search.best_params_,
                'training_samples': len(X_train),
                'test_samples': len(X_test)
            }
            
            model_comparison[model_name] = metrics
            
            logging.info(f"📊 {model_name} Performance:")
            logging.info(f"   • Train Accuracy: {train_accuracy:.4f}")
            logging.info(f"   • Test Accuracy: {test_accuracy:.4f}")
            logging.info(f"   • Overfitting Gap: {overfitting_gap:.4f}")
            logging.info(f"   • Precision: {test_precision:.4f}")
            logging.info(f"   • Recall: {test_recall:.4f}")
            logging.info(f"   • F1-Score: {test_f1:.4f}")
            logging.info(f"   • AUC Score: {test_auc:.4f}")
        
        # === DETERMINE BEST MODEL ===
        best_model_name = max(model_comparison.keys(), key=lambda x: model_comparison[x]['f1_score'])
        best_model = models[best_model_name]
        
        logging.info(f"🏆 Best Model: {best_model_name} (F1: {model_comparison[best_model_name]['f1_score']:.4f})")
        
        # GENERATE PREDICTIONS FOR ALL DATA
        logging.info("Generating predictions for all earthquake data...")
        
        # Get all processed earthquake data
        all_query = """
        SELECT id, latitude, longitude, magnitude, depth, spatial_density, hazard_score
        FROM earthquakes_processed
        """
        
        with engine.connect() as conn:
            df_all = pd.read_sql(all_query, conn)
        
        # Apply same feature engineering
        X_all = df_all[feature_cols].copy()
        X_all['distance_from_jakarta'] = np.sqrt((X_all['latitude'] + 6.2088) ** 2 + (X_all['longitude'] - 106.8456) ** 2)
        X_all['distance_from_ring_of_fire'] = np.minimum(np.abs(X_all['latitude'] + 5), np.abs(X_all['longitude'] - 120))
        X_all['magnitude_depth_ratio'] = X_all['magnitude'] / (X_all['depth'] + 1)
        X_all['energy_density'] = X_all['spatial_density'] * (X_all['magnitude'] ** 2)
        X_all['shallow_earthquake'] = (X_all['depth'] < 70).astype(int)
        X_all['high_magnitude'] = (X_all['magnitude'] > 5.0).astype(int)
        X_all['lat_lon_interaction'] = X_all['latitude'] * X_all['longitude']
        X_all['hazard_spatial_interaction'] = X_all['hazard_score'] * X_all['spatial_density']
        X_all = X_all.fillna(X_all.median())
        
        # Scale and predict
        X_all_scaled = scaler.transform(X_all)
        predictions = best_model.predict(X_all_scaled)
        prediction_probabilities = best_model.predict_proba(X_all_scaled)
        prediction_confidence = np.max(prediction_probabilities, axis=1)
        
        # Decode predictions
        predicted_risk_zones = label_encoder.inverse_transform(predictions)
        
        # === SAVE MODELS AND RESULTS ===
        output_dir = Path("/opt/airflow/magnitudr/data/airflow_output")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save models with neutral names
        joblib.dump(best_rf, output_dir / "earthquake_risk_model_randomforest.pkl")
        joblib.dump(best_lr, output_dir / "earthquake_risk_model_logistic.pkl")
        joblib.dump(best_model, output_dir / "earthquake_risk_model_best.pkl")
        joblib.dump(scaler, output_dir / "earthquake_risk_model_scaler.pkl")
        joblib.dump(label_encoder, output_dir / "earthquake_risk_model_label_encoder.pkl")
        
        # Convert numpy types to native Python types for JSON serialization
        def convert_numpy_types(obj):
            """Convert numpy types to native Python types"""
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, dict):
                return {key: convert_numpy_types(value) for key, value in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy_types(item) for item in obj]
            return obj
        
        # Save comparison results with numpy type conversion
        comparison_report = {
            'comparison_timestamp': datetime.now().isoformat(),
            'model_comparison': convert_numpy_types(model_comparison),
            'best_model': best_model_name,
            'class_distribution': convert_numpy_types(class_dist),
            'feature_count': int(len(X.columns)),
            'total_predictions_generated': int(len(predictions)),
            'recommendation': f"Use {best_model_name} for production predictions"
        }
        
        with open(output_dir / "model_comparison_report.json", 'w') as f:
            json.dump(comparison_report, f, indent=2)
        
        # STORE PREDICTIONS TO DATABASE
        logging.info("Storing predictions to database...")
        
        # Create predictions DataFrame
        predictions_df = pd.DataFrame({
            'earthquake_id': df_all['id'],
            'predicted_risk_zone': predicted_risk_zones,
            'prediction_confidence': prediction_confidence,
            'model_version': "RF_v1.0" if "RandomForest" in best_model_name else "LR_v1.0",
            'created_at': datetime.now()
        })
        
        # Clear existing predictions and insert new ones
        with engine.begin() as conn:
            # Create table if not exists
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS earthquake_predictions (
                    earthquake_id VARCHAR(100),
                    predicted_risk_zone VARCHAR(20),
                    prediction_confidence DECIMAL(6,4),
                    model_version VARCHAR(50),
                    created_at TIMESTAMP,
                    PRIMARY KEY (earthquake_id, model_version)
                )
            """))
            
            # Clear old predictions
            conn.execute(text("DELETE FROM earthquake_predictions"))
            
        # Insert new predictions
        predictions_df.to_sql('earthquake_predictions', engine, if_exists='append', index=False)
        
        # Store model metadata
        with engine.begin() as conn:
            # Create metadata table if not exists
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ml_model_metadata (
                    model_name VARCHAR(100),
                    model_type VARCHAR(50),
                    accuracy DECIMAL(6,4),
                    precision_score DECIMAL(6,4),
                    recall_score DECIMAL(6,4),
                    f1_score DECIMAL(6,4),
                    training_samples INTEGER,
                    created_at TIMESTAMP,
                    PRIMARY KEY (model_name, created_at)
                )
            """))
            
            # Clear old metadata
            conn.execute(text("DELETE FROM ml_model_metadata"))
        
        # Insert model metadata
        for model_name, metrics in model_comparison.items():
            metadata_df = pd.DataFrame([{
                'model_name': model_name,
                'model_type': 'RandomForest' if 'RandomForest' in model_name else 'LogisticRegression',
                'accuracy': metrics['test_accuracy'],
                'precision_score': metrics['precision'],
                'recall_score': metrics['recall'],
                'f1_score': metrics['f1_score'],
                'training_samples': metrics['training_samples'],
                'created_at': datetime.now()
            }])
            metadata_df.to_sql('ml_model_metadata', engine, if_exists='append', index=False)
        
        logging.info(f"ML Training with comparison completed successfully")
        logging.info(f"Best Model: {best_model_name}")
        logging.info(f"Predictions stored: {len(predictions_df):,}")
        logging.info(f"Models saved: RandomForest + LogisticRegression + Best")
        
        return model_comparison[best_model_name]['test_accuracy']
        
    except Exception as e:
        logging.error(f"❌ ML training with comparison failed: {e}")
        raise

task_train_models = PythonOperator(
    task_id='train_models_with_comparison',
    python_callable=train_models_with_comparison,
    dag=dag
)

def generate_ml_comparison_visualization(**context):
    """Generate visualization comparing model performance"""
    import matplotlib.pyplot as plt
    import json
    import logging
    from pathlib import Path
    
    try:
        logging.info("Generating ML model comparison visualization...")
        
        # Load comparison results
        output_dir = Path("/opt/airflow/magnitudr/data/airflow_output")
        with open(output_dir / "model_comparison_report.json", 'r') as f:
            comparison_data = json.load(f)
        
        model_comparison = comparison_data['model_comparison']
        
        # Create comparison chart
        models = list(model_comparison.keys())
        metrics = ['test_accuracy', 'precision', 'recall', 'f1_score']
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('ML Model Comparison: RandomForest vs LogisticRegression (Both Regularized)', fontsize=16)
        
        for i, metric in enumerate(metrics):
            ax = axes[i//2, i%2]
            values = [model_comparison[model][metric] for model in models]
            colors = ['#2E8B57', '#FF6B35']  # Forest green for RF, Orange for LR
            
            bars = ax.bar(models, values, color=colors, alpha=0.8)
            ax.set_title(f'{metric.replace("_", " ").title()}', fontsize=12)
            ax.set_ylabel('Score')
            ax.set_ylim(0, 1)
            
            # Add value labels on bars
            for bar, value in zip(bars, values):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                       f'{value:.3f}', ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        
        # Save plot
        viz_dir = Path("/opt/airflow/magnitudr/data/plots")
        viz_dir.mkdir(parents=True, exist_ok=True)
        plt.savefig(viz_dir / "ml_model_comparison.png", dpi=300, bbox_inches='tight')
        plt.close()
        
        # Create summary table
        summary_text = f"""
        🤖 ML MODEL COMPARISON SUMMARY (BOTH REGULARIZED)
        ================================================
        
        Best Model: {comparison_data['best_model']}
        Total Predictions: {comparison_data['total_predictions_generated']:,}
        
        RandomForest Performance:
        - Accuracy: {model_comparison['RandomForest']['test_accuracy']:.4f}
        - F1-Score: {model_comparison['RandomForest']['f1_score']:.4f}
        - Overfitting: {model_comparison['RandomForest']['overfitting_gap']:.4f}
        
        LogisticRegression Performance:
        - Accuracy: {model_comparison['LogisticRegression']['test_accuracy']:.4f}
        - F1-Score: {model_comparison['LogisticRegression']['f1_score']:.4f}
        - Overfitting: {model_comparison['LogisticRegression']['overfitting_gap']:.4f}
        
        Note: Both models use aggressive regularization for fair comparison
        Recommendation: {comparison_data['recommendation']}
        """
        
        with open(viz_dir / "ml_comparison_summary.txt", 'w') as f:
            f.write(summary_text)
        
        logging.info("✅ ML comparison visualization generated")
        return True
        
    except Exception as e:
        logging.error(f"❌ ML visualization failed: {e}")
        raise

task_generate_viz = PythonOperator(
    task_id='generate_ml_comparison_visualization',
    python_callable=generate_ml_comparison_visualization,
    dag=dag
)

# Dependencies
task_train_models >> task_generate_viz
