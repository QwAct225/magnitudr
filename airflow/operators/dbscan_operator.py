from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
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
import psycopg2 
import psycopg2.extras # <--- TAMBAHAN: Untuk execute_batch
from sklearn.dummy import DummyClassifier # <--- TAMBAHAN: Untuk DummyClassifier

# Add custom operators to path (pastikan jalur ini sudah benar)
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
    schedule_interval='0 2 * * 0',  # Weekly Sunday 02:00 UTC
    tags=['earthquake', 'ml', 'training', 'weekly']
)

def train_models_with_comparison(**context):
    """Train RandomForest + Logistic Regression with comparison and prediction storage"""
    logging.info("🤖 Starting Weekly ML Training with Model Comparison...")
    
    pg_conn = None 
    # --- INISIALISASI VARIABEL DI AWAL FUNGSI ---
    models = {} 
    best_rf = None 
    best_lr = None 
    best_model_name = "Undefined" 
    best_model = None 
    scaler = StandardScaler() 
    label_encoder = LabelEncoder()
    class_dist = {} 
    total_predictions_generated = 0 
    predicted_risk_zones = [] 
    model_comparison = {} 
    # -------------------------------------------

    try:
        db_connection_str = 'postgresql://postgres:earthquake123@postgres:5432/magnitudr'
        engine = create_engine(db_connection_str) # Engine tetap digunakan untuk CREATE TABLE IF NOT EXISTS
        pg_conn = psycopg2.connect(db_connection_str) # pg_conn digunakan untuk pd.read_sql dan execute_values

        query = """
        SELECT 
            e.id, e.latitude, e.longitude, e.magnitude, e.depth,
            e.spatial_density, e.hazard_score,
            c.risk_zone, c.cluster_size, c.avg_magnitude
        FROM earthquakes_processed e
        INNER JOIN earthquake_clusters c ON e.id = c.id
        WHERE c.risk_zone IS NOT NULL
        """
        
        df = pd.read_sql(query, pg_conn) # Menggunakan pg_conn untuk read_sql
        
        logging.info(f"📊 Loaded {len(df)} training samples")
        
        if df.empty:
            logging.warning("⚠️ Loaded an empty DataFrame. Cannot perform ML training.")
            model_comparison = {'Status': {'message': 'No data loaded for training'}} 
            output_dir = Path("/opt/airflow/magnitudr/data/airflow_output") 
            output_dir.mkdir(parents=True, exist_ok=True)
            comparison_report = {
                'comparison_timestamp': datetime.now().isoformat(),
                'model_comparison': model_comparison,
                'best_model': 'N/A',
                'class_distribution': {},
                'feature_count': 0,
                'total_predictions_generated': 0,
                'recommendation': 'No data for training'
            }
            with open(output_dir / "model_comparison_report.json", 'w') as f:
                json.dump(comparison_report, f, indent=2)
            return 0 

        feature_cols = ['latitude', 'longitude', 'magnitude', 'depth', 'spatial_density', 'hazard_score']
        if not all(col in df.columns for col in feature_cols):
            logging.error(f"❌ Missing one or more required feature columns in loaded data: {feature_cols}")
            raise ValueError("Required feature columns are missing.")

        X = df[feature_cols].copy()
        earthquake_ids = df['id'].copy() 
        
        X['distance_from_jakarta'] = np.sqrt((X['latitude'] + 6.2088) ** 2 + (X['longitude'] - 106.8456) ** 2)
        X['distance_from_ring_of_fire'] = np.minimum(np.abs(X['latitude'] + 5), np.abs(X['longitude'] - 120))
        X['magnitude_depth_ratio'] = X['magnitude'] / (X['depth'] + 1)
        X['energy_density'] = X['spatial_density'] * (X['magnitude'] ** 2)
        X['shallow_earthquake'] = (X['depth'] < 70).astype(int)
        X['high_magnitude'] = (X['magnitude'] > 5.0).astype(int)
        X['lat_lon_interaction'] = X['latitude'] * X['longitude']
        X['hazard_spatial_interaction'] = X['hazard_score'] * X['spatial_density']
        X = X.fillna(X.median())
        
        y = df['risk_zone'].copy()
        
        unique_classes_in_y = y.nunique() 
        
        unique, counts = np.unique(y, return_counts=True)
        class_dist = dict(zip(unique, counts)) 
        logging.info(f"🏷️ Class distribution: {y.value_counts().to_dict()}") # Baris ini sudah dikoreksi

        if unique_classes_in_y < 2:
            logging.warning(f"⚠️ Insufficient classes for full ML training: Found only {unique_classes_in_y} unique class(es) in 'risk_zone' column (Unique: {y.unique()}).")
            logging.warning("Proceeding with DummyClassifier to generate initial model files for cold start.")
            
            label_encoder.fit(y.unique())
            y_encoded = label_encoder.transform(y)

            scaler.fit(X) 
            X_train_scaled = scaler.transform(X) 
            y_train = y_encoded 

            best_model_name = "DummyClassifier" 
            best_model = DummyClassifier(strategy="most_frequent", random_state=42)
            best_model.fit(X_train_scaled, y_train)
            
            dummy_metrics = {
                'model_type': 'DummyClassifier', 'train_accuracy': 1.0, 'test_accuracy': 1.0,
                'overfitting_gap': 0.0, 'precision': 1.0, 'recall': 1.0, 'f1_score': 1.0,
                'auc_score': 0.5, 'best_params': {'strategy': 'most_frequent'},
                'training_samples': len(X), 'test_samples': 0 
            }
            model_comparison = {'DummyClassifier': dummy_metrics} 
            
            logging.info("Skipping RandomForest and LogisticRegression training due to single class data.")
        else: # Lanjutkan dengan pelatihan model penuh jika ada setidaknya 2 kelas
            label_encoder.fit(y.unique())
            y_encoded = label_encoder.fit_transform(y)

            X_train, X_test, y_train, y_test, ids_train, ids_test = train_test_split(
                X, y_encoded, earthquake_ids, test_size=0.2, random_state=42, stratify=y_encoded
            )
            
            if np.unique(y_train).size < 2:
                logging.error(f"❌ After train-test split, y_train still contains only {np.unique(y_train).size} class(es).")
                raise ValueError("Training data contains insufficient classes after splitting. Adjust split strategy or input data.")

            scaler.fit(X_train)
            X_train_scaled = scaler.transform(X_train)
            X_test_scaled = scaler.transform(X_test)
            
            # === MODEL 1: REGULARIZED RANDOM FOREST ===
            logging.info("🌲 Training Regularized Random Forest...")
            rf_param_grid = {
                'n_estimators': [30, 50], 'max_depth': [3, 5, 8], 'min_samples_split': [15, 25],
                'min_samples_leaf': [8, 12], 'max_features': ['sqrt', 'log2'], 'class_weight': ['balanced'],
                'min_impurity_decrease': [0.001, 0.01]
            }
            kfold = StratifiedKFold(n_splits=2, shuffle=True, random_state=42)
            rf = RandomForestClassifier(random_state=42)
            rf_grid_search = GridSearchCV(
                estimator=rf, param_grid=rf_param_grid, cv=kfold, scoring='f1_weighted', n_jobs=1, verbose=1
            )
            noise_scale = 0.01
            X_train_noisy = X_train_scaled.copy()
            for i in range(X_train_scaled.shape[1]):
                noise = np.random.normal(0, noise_scale, X_train_noisy.shape[0]) 
                X_train_noisy[:, i] += noise
            logging.info(f"🔄 Added {noise_scale*100}% noise to training data for regularization")
            rf_grid_search.fit(X_train_noisy, y_train)
            best_rf = rf_grid_search.best_estimator_
            
            # === MODEL 2: REGULARIZED LOGISTIC REGRESSION (FAST VERSION) ===
            logging.info("📊 Training Regularized Logistic Regression (Fast)...")
            lr_param_grid = {
                'C': [0.01, 0.1, 1.0], 'penalty': ['l1', 'l2'], 'solver': ['liblinear'],
                'class_weight': ['balanced'], 'max_iter': [1000], 'tol': [1e-4]
            }
            lr = LogisticRegression(random_state=42)
            lr_grid_search = GridSearchCV(
                estimator=lr, param_grid=lr_param_grid, cv=kfold, scoring='f1_weighted', n_jobs=1, verbose=1
            )
            lr_grid_search.fit(X_train_noisy, y_train)
            best_lr = lr_grid_search.best_estimator_
            
            # === MODEL EVALUATION & COMPARISON ===
            logging.info("📈 Evaluating and comparing models...")
            models = {'RandomForest': best_rf, 'LogisticRegression': best_lr} 
            
            for model_name, model in models.items():
                y_pred_train = model.predict(X_train_scaled)
                y_pred_test = model.predict(X_test_scaled)
                y_pred_test_proba = model.predict_proba(X_test_scaled) if hasattr(model, 'predict_proba') else None
                train_accuracy = accuracy_score(y_train, y_pred_train)
                test_accuracy = accuracy_score(y_test, y_pred_test)
                test_precision = precision_score(y_test, y_pred_test, average='weighted')
                test_recall = recall_score(y_test, y_pred_test, average='weighted')
                test_f1 = f1_score(y_test, y_pred_test, average='weighted')
                try:
                    test_auc = roc_auc_score(y_test, y_pred_test_proba, multi_class='ovr', average='weighted') if y_pred_test_proba is not None else 0
                except ValueError:
                    test_auc = 0
                overfitting_gap = train_accuracy - test_accuracy
                metrics = {
                    'model_type': model_name, 'train_accuracy': train_accuracy, 'test_accuracy': test_accuracy,
                    'overfitting_gap': overfitting_gap, 'precision': test_precision, 'recall': test_recall,
                    'f1_score': test_f1, 'auc_score': test_auc,
                    'best_params': rf_grid_search.best_params_ if model_name.startswith('RandomForest') and hasattr(rf_grid_search, 'best_params_') else \
                                   lr_grid_search.best_params_ if model_name.startswith('LogisticRegression') and hasattr(lr_grid_search, 'best_params_') else {},
                    'training_samples': len(X_train), 'test_samples': len(X_test)
                }
                model_comparison[model_name] = metrics 
                logging.info(f"📊 {model_name} Performance:")
                logging.info(f"    • Train Accuracy: {train_accuracy:.4f}")
                logging.info(f"    • Test Accuracy: {test_accuracy:.4f}")
                logging.info(f"    • Overfitting Gap: {overfitting_gap:.4f}")
                logging.info(f"    • Precision: {test_precision:.4f}")
                logging.info(f"    • Recall: {test_recall:.4f}")
                logging.info(f"    • F1-Score: {test_f1:.4f}")
                logging.info(f"    • AUC Score: {test_auc:.4f}")

            if not model_comparison: 
                logging.error("❌ No models were successfully trained or compared. Cannot determine best model.")
                raise RuntimeError("Model training or comparison failed for all models.")
            best_model_name = max(model_comparison.keys(), key=lambda x: model_comparison[x]['f1_score'])
            best_model = models[best_model_name] 
            logging.info(f"🏆 Best Model: {best_model_name} (F1: {model_comparison[best_model_name]['f1_score']:.4f})")

        # === GENERATE PREDICTIONS FOR ALL DATA === 
        logging.info("🔮 Generating predictions for all earthquake data...")
        all_query = """
        SELECT id, latitude, longitude, magnitude, depth, spatial_density, hazard_score
        FROM earthquakes_processed
        """
        df_all = pd.read_sql(all_query, pg_conn) 
        
        if df_all.empty:
            logging.warning("⚠️ No data loaded for final predictions. Skipping prediction storage.")
            predictions_df = pd.DataFrame(columns=['earthquake_id', 'classified_risk_zone', 'classification_confidence', 'model_version', 'created_at']) 
            total_predictions_generated = 0
            predicted_risk_zones = [] 
        else:
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
            
            X_all_scaled = scaler.transform(X_all) 
            predictions = best_model.predict(X_all_scaled) 
            prediction_probabilities = best_model.predict_proba(X_all_scaled)
            prediction_confidence = np.max(prediction_probabilities, axis=1)
            
            predicted_risk_zones = label_encoder.inverse_transform(predictions) 

            predictions_df = pd.DataFrame({
                'earthquake_id': df_all['id'],
                'classified_risk_zone': predicted_risk_zones, 
                'classification_confidence': prediction_confidence, 
                'model_version': best_model_name, 
                'created_at': datetime.now()
            })
            total_predictions_generated = len(predictions_df)
        
        # === SAVE MODELS AND RESULTS === 
        output_dir = Path("/opt/airflow/magnitudr/data/airflow_output")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        if best_rf is not None: 
            joblib.dump(best_rf, output_dir / "earthquake_risk_model_randomforest.pkl")
        if best_lr is not None: 
            joblib.dump(best_lr, output_dir / "earthquake_risk_model_logistic.pkl")
        joblib.dump(best_model, output_dir / "earthquake_risk_model_best.pkl")
        joblib.dump(scaler, output_dir / "earthquake_risk_model_scaler.pkl")
        joblib.dump(label_encoder, output_dir / "earthquake_risk_model_label_encoder.pkl")
        
        def convert_numpy_types(obj):
            if isinstance(obj, np.integer): return int(obj)
            elif isinstance(obj, np.floating): return float(obj)
            elif isinstance(obj, np.ndarray): return obj.tolist()
            elif isinstance(obj, dict): return {key: convert_numpy_types(value) for key, value in obj.items()}
            elif isinstance(obj, list): return [convert_numpy_types(item) for item in obj]
            return obj
        
        comparison_report = {
            'comparison_timestamp': datetime.now().isoformat(),
            'model_comparison': convert_numpy_types(model_comparison), 
            'best_model': best_model_name,
            'class_distribution': convert_numpy_types(class_dist) if class_dist else {'Unknown': y.nunique()}, 
            'feature_count': int(len(X.columns)),
            'total_predictions_generated': int(total_predictions_generated),
            'recommendation': f"Use {best_model_name} for production predictions"
        }
        
        with open(output_dir / "model_comparison_report.json", 'w') as f:
            json.dump(comparison_report, f, indent=2)
        
        # === STORE PREDICTIONS TO DATABASE === 
        logging.info("💾 Storing predictions to database...")
        
        if not predictions_df.empty: 
            with pg_conn.cursor() as cursor: 
                # Bersihkan tabel sebelum menyisipkan (seperti yang dilakukan pd.to_sql dengan if_exists='replace')
                cursor.execute("DELETE FROM earthquake_risk_classifications")
                
                # Siapkan query INSERT
                insert_query = """
                    INSERT INTO earthquake_risk_classifications (
                        earthquake_id, classified_risk_zone, classification_confidence, model_version, created_at
                    ) VALUES (%s, %s, %s, %s, %s);
                """
                # Siapkan data dalam format list of tuples
                # Pastikan kolom sesuai urutan di INSERT query
                data_to_insert = [
                    (row.earthquake_id, row.classified_risk_zone, row.classification_confidence,
                     row.model_version, row.created_at) # Pastikan row.created_at adalah datetime object
                    for row in predictions_df.itertuples(index=False)
                ]
                
                psycopg2.extras.execute_batch(cursor, insert_query, data_to_insert, page_size=1000)
                logging.info(f"✅ Inserted {len(data_to_insert)} predictions into earthquake_risk_classifications.")

        else:
            logging.warning("⚠️ No predictions generated to store in database.")
        
        # Store model metadata (tetap ml_model_metadata)
        # Memastikan model_comparison tidak kosong sebelum mencoba iterasi
        if model_comparison: 
            with pg_conn.cursor() as cursor: 
                cursor.execute("DELETE FROM ml_model_metadata") 

                insert_metadata_query = """
                    INSERT INTO ml_model_metadata (
                        model_name, model_type, accuracy, precision_score, recall_score, f1_score, training_samples, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
                # Siapkan data dalam format list of tuples
                metadata_to_insert = [
                    (model_name, metrics['model_type'], metrics['test_accuracy'],
                     metrics['precision'], metrics['recall'], metrics['f1_score'],
                     metrics['training_samples'], datetime.now()) 
                    for model_name, metrics in model_comparison.items()
                ]
                psycopg2.extras.execute_batch(cursor, insert_metadata_query, metadata_to_insert, page_size=1000)
                logging.info(f"✅ Inserted {len(metadata_to_insert)} model metadata records into ml_model_metadata.")
        else:
            logging.warning("⚠️ No model metadata generated to store.")

        pg_conn.commit() 

        logging.info(f"✅ ML Training with comparison completed successfully")
        logging.info(f"🏆 Best Model: {best_model_name}")
        logging.info(f"📊 Predictions stored: {total_predictions_generated:,}")
        logging.info(f"💾 Models saved: RandomForest + LogisticRegression + Best")
        
        return model_comparison[best_model_name]['test_accuracy']
        
    except Exception as e:
        logging.error(f"❌ ML training with comparison failed: {e}", exc_info=True)
        if pg_conn:
            pg_conn.rollback()
        raise
    finally:
        if pg_conn: 
            pg_conn.close()