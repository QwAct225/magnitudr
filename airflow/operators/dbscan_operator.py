from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
import psycopg2
from sqlalchemy import create_engine, text
import joblib
import logging
from operators.ml_classification_operator import EarthquakeMLClassificationOperator


import os
from pathlib import Path

class DBSCANClusterOperator(BaseOperator):
    """
    Hybrid DBSCAN operator - clustering with ML-based risk zone labeling
    """

    @apply_defaults
    def __init__(
            self,
            input_path: str,
            db_connection: str,
            eps: float = 0.1,
            min_samples: int = 5,
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.input_path = input_path
        self.db_connection = db_connection
        self.eps = eps
        self.min_samples = min_samples
        self.classifier = EarthquakeMLClassificationOperator(
            task_id='ml_classifier_for_dbscan',
            db_connection=self.db_connection,
            model_output_path='/opt/airflow/magnitudr/data/models/earthquake_model.pkl'
        )
    
    def execute(self, context):
        logging.info("🔬 Starting Hybrid DBSCAN clustering with ML labeling...")
        
        try:
            # Load processed data
            df = pd.read_csv(self.input_path)
            logging.info(f"📊 Loaded {len(df)} records for clustering")
            
            # Prepare features for clustering
            cluster_features = ['latitude', 'longitude', 'magnitude', 'depth']
            cluster_data = df[cluster_features].dropna()
            
            if len(cluster_data) < self.min_samples:
                logging.warning("⚠️ Insufficient data for clustering")
                return 0
            
            # Standardize features
            scaler = StandardScaler()
            scaled_features = scaler.fit_transform(cluster_data)
            
            # Apply DBSCAN clustering
            dbscan = DBSCAN(eps=self.eps, min_samples=self.min_samples)
            cluster_labels = dbscan.fit_predict(scaled_features)
            
            # Add cluster results to dataframe
            df.loc[cluster_data.index, 'cluster_id'] = cluster_labels
            
            # Analyze clusters with HYBRID ML labeling
            cluster_analysis = self._analyze_clusters_with_ml(df)
            
            # Store ONLY clustering results (not earthquake data)
            self._store_clustering_results(df, cluster_analysis)
            
            n_clusters = len(cluster_analysis)
            logging.info(f"✅ Hybrid DBSCAN completed: {n_clusters} clusters with ML labeling")
            
            return n_clusters
            
        except Exception as e:
            logging.error(f"❌ Hybrid DBSCAN clustering failed: {e}")
            raise
    
    def _analyze_clusters_with_ml(self, df):
        """Analyze clusters with ML model for risk zone labeling"""
        clusters = []
        
        # Load trained ML model for labeling
        ml_model_path = Path("/opt/airflow/magnitudr/data/airflow_output/earthquake_risk_model_best.pkl")
        scaler_path = Path("/opt/airflow/magnitudr/data/airflow_output/earthquake_risk_model_scaler.pkl")
        encoder_path = Path("/opt/airflow/magnitudr/data/airflow_output/earthquake_risk_model_label_encoder.pkl")
        
        use_ml_labeling = all(p.exists() for p in [ml_model_path, scaler_path, encoder_path])
        
        if use_ml_labeling:
            try:
                ml_model = joblib.load(ml_model_path)
                ml_scaler = joblib.load(scaler_path)
                label_encoder = joblib.load(encoder_path)
                logging.info("🤖 Using ML model for risk zone labeling")
            except Exception as e:
                logging.warning(f"⚠️ Failed to load ML model: {e}, using fallback logic")
                use_ml_labeling = False
        else:
            logging.info("📊 ML model not found, using fallback hazard score logic")
        
        # Filter out noise (cluster_id = -1)
        clustered_data = df[df['cluster_id'] != -1]
        
        for cluster_id in clustered_data['cluster_id'].unique():
            cluster_df = clustered_data[clustered_data['cluster_id'] == cluster_id]
            
            # Calculate cluster statistics
            cluster_stats = {
                'cluster_id': int(cluster_id),
                'size': len(cluster_df),
                'centroid_lat': cluster_df['latitude'].mean(),
                'centroid_lon': cluster_df['longitude'].mean(),
                'avg_magnitude': cluster_df['magnitude'].mean(),
                'max_magnitude': cluster_df['magnitude'].max(),
                'avg_depth': cluster_df['depth'].mean(),
                'avg_hazard_score': cluster_df['hazard_score'].mean() if 'hazard_score' in cluster_df.columns else 5.0,
                'dominant_region': cluster_df['region'].mode().iloc[0] if 'region' in cluster_df.columns and not cluster_df['region'].mode().empty else 'Unknown'
            }
            
            # HYBRID APPROACH: Use ML model for risk zone labeling
            if use_ml_labeling:
                try:
                    # Prepare features for ML prediction (same as training)
                    ml_features = self._prepare_ml_features(cluster_stats)
                    ml_features_scaled = ml_scaler.transform([ml_features])
                    
                    # Get ML prediction
                    risk_prediction = ml_model.predict(ml_features_scaled)[0]
                    risk_zone = label_encoder.inverse_transform([risk_prediction])[0]
                    
                    # Get prediction confidence
                    prediction_proba = ml_model.predict_proba(ml_features_scaled)[0]
                    confidence = np.max(prediction_proba)
                    
                    logging.info(f"🤖 Cluster {cluster_id}: ML predicted {risk_zone} (confidence: {confidence:.3f})")
                    
                except Exception as e:
                    logging.warning(f"⚠️ ML prediction failed for cluster {cluster_id}: {e}, using fallback")
                    risk_zone = self._fallback_risk_labeling(cluster_stats['avg_hazard_score'])
            else:
                # Fallback: Use hazard score logic
                risk_zone = self._fallback_risk_labeling(cluster_stats['avg_hazard_score'])
            
            cluster_stats['risk_zone'] = risk_zone
            cluster_stats['cluster_label'] = f"Cluster_{cluster_id}_{risk_zone}"
            
            clusters.append(cluster_stats)
        
        return clusters
    
    def _prepare_ml_features(self, cluster_stats):
        """Prepare features for ML model prediction (same as training features)"""
        # Use cluster centroid and statistics as features
        features = [
            cluster_stats['centroid_lat'],           # latitude
            cluster_stats['centroid_lon'],           # longitude  
            cluster_stats['avg_magnitude'],          # magnitude
            cluster_stats['avg_depth'],              # depth
            0.1,                                     # spatial_density (estimated)
            cluster_stats['avg_hazard_score'],       # hazard_score
            # Engineered features (same as training)
            np.sqrt((cluster_stats['centroid_lat'] + 6.2088) ** 2 + (cluster_stats['centroid_lon'] - 106.8456) ** 2),  # distance_from_jakarta
            min(abs(cluster_stats['centroid_lat'] + 5), abs(cluster_stats['centroid_lon'] - 120)),  # distance_from_ring_of_fire
            cluster_stats['avg_magnitude'] / (cluster_stats['avg_depth'] + 1),  # magnitude_depth_ratio
            0.1 * (cluster_stats['avg_magnitude'] ** 2),  # energy_density
            1 if cluster_stats['avg_depth'] < 70 else 0,  # shallow_earthquake
            1 if cluster_stats['avg_magnitude'] > 5.0 else 0,  # high_magnitude
            cluster_stats['centroid_lat'] * cluster_stats['centroid_lon'],  # lat_lon_interaction
            cluster_stats['avg_hazard_score'] * 0.1  # hazard_spatial_interaction
        ]
        
        return features
    
    def _fallback_risk_labeling(self, avg_hazard_score):
        """Fallback risk labeling using hazard score"""
        if avg_hazard_score >= 8:
            return 'Extreme'
        elif avg_hazard_score >= 6:
            return 'High'
        elif avg_hazard_score >= 4:
            return 'Moderate'
        else:
            return 'Low'

    def _store_clustering_results(self, df, cluster_analysis):
        logging.info("📊 Storing clustering results and hazard zones to database...")

        conn = None
        try:
            conn = psycopg2.connect(self.db_connection)

            cluster_df = cluster_analysis.copy()
            hazard_df = self._create_hazard_zones(cluster_analysis)

            cluster_df_final = cluster_df.replace({np.nan: None, pd.NaT: None})
            hazard_df_final = hazard_df.replace({np.nan: None, pd.NaT: None})

            with conn.cursor() as cursor:
                logging.info("Clearing existing clustering and hazard data...")
                cursor.execute("DELETE FROM earthquake_clusters")
                cursor.execute("DELETE FROM hazard_zones")
                logging.info("✅ Cleared existing data.")

                if not cluster_df_final.empty:
                    logging.info(f"Inserting {len(cluster_df_final)} records into earthquake_clusters...")
                    cluster_insert_query = f"""
                        INSERT INTO earthquake_clusters ({', '.join(cluster_df_final.columns)})
                        VALUES %s
                    """
                    cluster_tuples = [tuple(row) for row in cluster_df_final.itertuples(index=False)]
                    psycopg2.extras.execute_values(cursor, cluster_insert_query, cluster_tuples, page_size=1000)
                    logging.info("✅ Cluster data inserted.")

                if not hazard_df_final.empty:
                    logging.info(f"Inserting {len(hazard_df_final)} records into hazard_zones...")
                    hazard_insert_query = f"""
                        INSERT INTO hazard_zones ({', '.join(hazard_df_final.columns)})
                        VALUES %s
                    """
                    hazard_tuples = [tuple(row) for row in hazard_df_final.itertuples(index=False)]
                    psycopg2.extras.execute_values(cursor, hazard_insert_query, hazard_tuples, page_size=1000)
                    logging.info("✅ Hazard zone data inserted.")

                conn.commit()
                logging.info("✅ Clustering results and hazard zones successfully stored and committed.")

        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(f"❌ Clustering results storage failed: {error}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
