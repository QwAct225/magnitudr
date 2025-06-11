from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
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
import os
from pathlib import Path

class EarthquakeMLClassificationOperator(BaseOperator):
    """
    ML Classification Operator for Earthquake Risk Zone Prediction
    
    Purpose: Train model to classify geographic coordinates into risk zones
    Method: Use DBSCAN cluster results as training labels
    Model: Random Forest Classifier with K-Fold CV and Hyperparameter Tuning
    """
    
    @apply_defaults
    def __init__(
        self,
        db_connection: str,
        model_output_path: str,
        test_size: float = 0.2,
        cv_folds: int = 5,
        random_state: int = 42,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.db_connection = db_connection
        self.model_output_path = model_output_path
        self.test_size = test_size
        self.cv_folds = cv_folds
        self.random_state = random_state
    
    def execute(self, context):
        logging.info("🤖 Starting Earthquake Risk Zone ML Classification...")
        
        try:
            # Step 1: Load and prepare data
            df_features, df_labels = self._load_training_data()
            
            # Step 2: Feature engineering
            X_processed = self._engineer_features(df_features)
            y_processed = self._prepare_labels(df_labels)
            
            # Step 3: Train-test split
            X_train, X_test, y_train, y_test = train_test_split(
                X_processed, y_processed, 
                test_size=self.test_size, 
                random_state=self.random_state,
                stratify=y_processed  # Maintain class distribution
            )
            
            # Step 4: K-Fold Cross Validation with Hyperparameter Tuning
            best_model = self._hyperparameter_tuning(X_train, y_train)
            
            # Step 5: Final model training and evaluation
            model_metrics = self._train_and_evaluate(best_model, X_train, X_test, y_train, y_test)
            
            # Step 6: Save model and results
            self._save_model_and_results(best_model, model_metrics, X_test, y_test)
            
            logging.info("✅ ML Classification pipeline completed successfully")
            return model_metrics['accuracy']
            
        except Exception as e:
            logging.error(f"❌ ML Classification failed: {e}")
            raise
    
    def _load_training_data(self):
        """Load training data from PostgreSQL"""
        logging.info("📊 Loading training data from database...")
        
        engine = create_engine(self.db_connection)
        
        # Load earthquake data with cluster assignments (features + labels)
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
        
        # Separate features and labels
        feature_cols = ['latitude', 'longitude', 'magnitude', 'depth', 'spatial_density', 'hazard_score']
        label_col = 'risk_zone'
        
        df_features = df[feature_cols].copy()
        df_labels = df[label_col].copy()
        
        return df_features, df_labels
    
    def _engineer_features(self, df):
        """Advanced feature engineering for seismic risk classification"""
        logging.info("🔧 Engineering earthquake risk features...")
        
        # Create copy for processing
        df_processed = df.copy()
        
        # Geographic features (distance from major fault lines/cities)
        df_processed['distance_from_jakarta'] = np.sqrt(
            (df_processed['latitude'] + 6.2088) ** 2 + 
            (df_processed['longitude'] - 106.8456) ** 2
        )
        
        df_processed['distance_from_ring_of_fire'] = np.minimum(
            np.abs(df_processed['latitude'] + 5),  # Approximate ring of fire latitude
            np.abs(df_processed['longitude'] - 120)  # Approximate ring of fire longitude
        )
        
        # Seismic features
        df_processed['magnitude_depth_ratio'] = df_processed['magnitude'] / (df_processed['depth'] + 1)
        df_processed['energy_density'] = df_processed['spatial_density'] * (df_processed['magnitude'] ** 2)
        df_processed['shallow_earthquake'] = (df_processed['depth'] < 70).astype(int)
        df_processed['high_magnitude'] = (df_processed['magnitude'] > 5.0).astype(int)
        
        # Interaction features
        df_processed['lat_lon_interaction'] = df_processed['latitude'] * df_processed['longitude']
        df_processed['hazard_spatial_interaction'] = df_processed['hazard_score'] * df_processed['spatial_density']
        
        # Handle missing values
        df_processed = df_processed.fillna(df_processed.median())
        
        logging.info(f"🔧 Features engineered: {len(df_processed.columns)} total features")
        return df_processed
    
    def _prepare_labels(self, labels):
        """Prepare and encode risk zone labels"""
        logging.info("🏷️ Preparing risk zone labels...")
        
        # Encode risk zones to numeric
        label_encoder = LabelEncoder()
        encoded_labels = label_encoder.fit_transform(labels)
        
        # Save label encoder for later use
        os.makedirs(os.path.dirname(self.model_output_path), exist_ok=True)
        joblib.dump(label_encoder, self.model_output_path.replace('.pkl', '_label_encoder.pkl'))
        
        # Log class distribution
        unique, counts = np.unique(labels, return_counts=True)
        class_dist = dict(zip(unique, counts))
        logging.info(f"🏷️ Class distribution: {class_dist}")
        
        return encoded_labels
    
    def _hyperparameter_tuning(self, X_train, y_train):
        """K-Fold Cross Validation with Hyperparameter Tuning"""
        logging.info("🔍 Starting K-Fold CV with hyperparameter tuning...")
        
        # Define parameter grid for Random Forest
        param_grid = {
            'n_estimators': [100, 200, 300],
            'max_depth': [10, 20, None],
            'min_samples_split': [2, 5, 10],
            'min_samples_leaf': [1, 2, 4],
            'max_features': ['sqrt', 'log2', None]
        }
        
        # Initialize Random Forest
        rf = RandomForestClassifier(random_state=self.random_state)
        
        # K-Fold Cross Validation
        kfold = StratifiedKFold(n_splits=self.cv_folds, shuffle=True, random_state=self.random_state)
        
        # Grid Search with CV
        grid_search = GridSearchCV(
            estimator=rf,
            param_grid=param_grid,
            cv=kfold,
            scoring='f1_weighted',  # Use weighted F1 for imbalanced classes
            n_jobs=-1,
            verbose=1
        )
        
        # Fit grid search
        grid_search.fit(X_train, y_train)
        
        logging.info(f"🏆 Best parameters: {grid_search.best_params_}")
        logging.info(f"🏆 Best CV score: {grid_search.best_score_:.4f}")
        
        return grid_search.best_estimator_
    
    def _train_and_evaluate(self, model, X_train, X_test, y_train, y_test):
        """Train final model and comprehensive evaluation"""
        logging.info("📈 Training final model and evaluating...")
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Save scaler
        joblib.dump(scaler, self.model_output_path.replace('.pkl', '_scaler.pkl'))
        
        # Train final model
        model.fit(X_train_scaled, y_train)
        
        # Predictions
        y_pred_train = model.predict(X_train_scaled)
        y_pred_test = model.predict(X_test_scaled)
        
        # Comprehensive evaluation metrics
        metrics = {
            'accuracy': accuracy_score(y_test, y_pred_test),
            'precision': precision_score(y_test, y_pred_test, average='weighted'),
            'recall': recall_score(y_test, y_pred_test, average='weighted'),
            'f1_score': f1_score(y_test, y_pred_test, average='weighted'),
            'train_accuracy': accuracy_score(y_train, y_pred_train),
            'confusion_matrix': confusion_matrix(y_test, y_pred_test).tolist(),
            'classification_report': classification_report(y_test, y_pred_test, output_dict=True)
        }
        
        # Feature importance
        feature_importance = dict(zip(X_train.columns, model.feature_importances_))
        metrics['feature_importance'] = feature_importance
        
        # Log results
        logging.info(f"📊 Model Performance:")
        logging.info(f"   • Accuracy: {metrics['accuracy']:.4f}")
        logging.info(f"   • Precision: {metrics['precision']:.4f}")
        logging.info(f"   • Recall: {metrics['recall']:.4f}")
        logging.info(f"   • F1-Score: {metrics['f1_score']:.4f}")
        logging.info(f"   • Train Accuracy: {metrics['train_accuracy']:.4f}")
        
        return metrics
    
    def _save_model_and_results(self, model, metrics, X_test, y_test):
        """Save trained model and evaluation results"""
        logging.info("💾 Saving model and results...")
        
        # Save model
        os.makedirs(os.path.dirname(self.model_output_path), exist_ok=True)
        joblib.dump(model, self.model_output_path)
        
        # Save metrics
        metrics_path = self.model_output_path.replace('.pkl', '_metrics.json')
        with open(metrics_path, 'w') as f:
            json.dump(metrics, f, indent=2)
        
        # Save evaluation report
        report_path = self.model_output_path.replace('.pkl', '_evaluation_report.json')
        
        evaluation_report = {
            'model_type': 'RandomForestClassifier',
            'task': 'Earthquake Risk Zone Classification',
            'purpose': 'Classify geographic coordinates into seismic risk zones',
            'training_data_size': len(X_test) / (1 - self.test_size),  # Estimate total size
            'test_data_size': len(X_test),
            'cv_folds': self.cv_folds,
            'metrics': metrics,
            'model_interpretation': {
                'top_features': sorted(metrics['feature_importance'].items(), 
                                     key=lambda x: x[1], reverse=True)[:5],
                'model_purpose': 'Geographic risk zone classification for earthquake hazard assessment',
                'business_value': 'Enable automated risk assessment for new locations'
            },
            'training_timestamp': pd.Timestamp.now().isoformat()
        }
        
        with open(report_path, 'w') as f:
            json.dump(evaluation_report, f, indent=2)
        
        logging.info(f"✅ Model saved: {self.model_output_path}")
        logging.info(f"✅ Metrics saved: {metrics_path}")
        logging.info(f"✅ Report saved: {report_path}")
