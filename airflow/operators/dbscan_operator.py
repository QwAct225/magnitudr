import pandas as pd
import numpy as np
import logging
import joblib
from sklearn.cluster import DBSCAN
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2
import psycopg2.extras

from operators.ml_classification_operator import EarthquakeMLClassificationOperator


class DBSCANClusterOperator(BaseOperator):
    """
    Hybrid DBSCAN operator - final corrected version for clustering and ML labeling.
    """

    @apply_defaults
    def __init__(
            self,
            input_path: str,
            db_connection: str,
            model_path: str,
            scaler_path: str,
            label_encoder_path: str,
            eps: float = 0.1,
            min_samples: int = 5,
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.input_path = input_path
        self.db_connection = db_connection
        self.model_path = model_path
        self.scaler_path = scaler_path
        self.label_encoder_path = label_encoder_path
        self.eps = eps
        self.min_samples = min_samples

    def execute(self, context):
        """
        Main execution logic: Load -> Cluster -> Analyze -> Merge -> Store.
        """
        try:
            logging.info("🔬 Starting DBSCAN clustering process...")
            df_events = self._load_and_prepare_data()

            if df_events.empty:
                logging.warning("Skipping DBSCAN clustering due to empty input data.")
                return 0

            coordinates = df_events[['latitude', 'longitude']].values
            db = DBSCAN(eps=self.eps, min_samples=self.min_samples, algorithm='ball_tree', metric='haversine').fit(
                np.radians(coordinates))

            df_events['cluster_id'] = db.labels_
            logging.info(
                f"✅ DBSCAN clustering completed. Found {len(set(db.labels_)) - (1 if -1 in db.labels_ else 0)} clusters.")

            df_cluster_summary = self._analyze_and_predict_clusters(df_events)

            if df_cluster_summary.empty:
                logging.info("No clusters were formed. Skipping storage.")
                self._clear_target_tables()
                return 0

            df_merged = pd.merge(df_events, df_cluster_summary, on='cluster_id', how='left')
            df_final_to_store = df_merged[df_merged['cluster_id'] != -1].copy()

            if not df_final_to_store.empty:
                self._store_clustering_results(df_final_to_store)
            else:
                logging.info("No clustered events to store after filtering.")
                self._clear_target_tables()

            logging.info(f"✅ Hybrid DBSCAN clustering with ML labeling fully completed.")
            return len(df_cluster_summary)

        except Exception as e:
            logging.error(f"❌ Hybrid DBSCAN clustering failed: {e}", exc_info=True)
            raise

    def _load_and_prepare_data(self):
        logging.info(f"Loading data from {self.input_path}")
        df = pd.read_csv(self.input_path)
        required_cols = ['id', 'latitude', 'longitude', 'magnitude', 'depth']
        if not all(col in df.columns for col in required_cols):
            logging.error(f"Input CSV is missing one of the required columns: {required_cols}.")
            return pd.DataFrame()
        return df

    def _analyze_and_predict_clusters(self, df):
        logging.info("🔬 Analyzing clusters and predicting risk zones...")

        df_clustered_only = df[df['cluster_id'] != -1].copy()
        if df_clustered_only.empty:
            return pd.DataFrame()

        cluster_summary = df_clustered_only.groupby('cluster_id').agg(
            cluster_size=('magnitude', 'count'),
            avg_magnitude=('magnitude', 'mean'),
            centroid_lat=('latitude', 'mean'),
            centroid_lon=('longitude', 'mean')
        ).reset_index()

        try:
            model = joblib.load(self.model_path)
            scaler = joblib.load(self.scaler_path)
            label_encoder = joblib.load(self.label_encoder_path)
        except FileNotFoundError as e:
            logging.error(f"Model/Scaler/Encoder file not found: {e}. Cannot predict risk zones.")
            cluster_summary['risk_zone'] = 'Unknown'
            cluster_summary['cluster_label'] = cluster_summary['cluster_id'].apply(lambda x: f'Cluster-{x}')
            return cluster_summary

        features_to_predict = cluster_summary[['avg_magnitude']]
        features_scaled = scaler.transform(features_to_predict)

        predictions_encoded = model.predict(features_scaled)
        predictions_decoded = label_encoder.inverse_transform(predictions_encoded)

        cluster_summary['risk_zone'] = predictions_decoded
        # **PERBAIKAN UTAMA**: Membuat 'cluster_label' di sini, di tempat yang benar.
        cluster_summary['cluster_label'] = cluster_summary['cluster_id'].apply(lambda x: f'Cluster-{x}')
        logging.info("✅ ML-based risk zone labeling completed.")

        return cluster_summary

    def _clear_target_tables(self):
        """Helper function to clear tables."""
        conn = None
        try:
            conn = psycopg2.connect(self.db_connection)
            with conn.cursor() as cursor:
                logging.info("Clearing existing clustering and hazard data.")
                cursor.execute("DELETE FROM earthquake_clusters")
                cursor.execute("DELETE FROM hazard_zones")
                conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(f"❌ Failed to clear target tables: {error}")
            if conn: conn.rollback()
            raise
        finally:
            if conn: conn.close()

    def _store_clustering_results(self, df_to_store):
        logging.info(f"📊 Storing {len(df_to_store)} clustered events to database...")
        conn = None
        try:
            conn = psycopg2.connect(self.db_connection)

            schema_columns = [
                'id', 'cluster_id', 'cluster_label', 'risk_zone',
                'centroid_lat', 'centroid_lon', 'cluster_size', 'avg_magnitude'
            ]
            df_final = df_to_store[schema_columns].copy()
            df_final.replace({np.nan: None, pd.NaT: None}, inplace=True)

            with conn.cursor() as cursor:
                self._clear_target_tables()

                logging.info(f"Inserting {len(df_final)} records into earthquake_clusters...")
                insert_query = f"INSERT INTO earthquake_clusters ({', '.join(df_final.columns)}) VALUES %s"
                data_tuples = [tuple(row) for row in df_final.itertuples(index=False)]
                psycopg2.extras.execute_values(cursor, insert_query, data_tuples, page_size=1000)
                logging.info("✅ Cluster data inserted.")

                hazard_df = self._create_hazard_zones(df_final)
                if not hazard_df.empty:
                    logging.info(f"Inserting {len(hazard_df)} records into hazard_zones...")
                    hazard_insert_query = f"INSERT INTO hazard_zones ({', '.join(hazard_df.columns)}) VALUES %s"
                    hazard_tuples = [tuple(row) for row in hazard_df.itertuples(index=False)]
                    psycopg2.extras.execute_values(cursor, hazard_insert_query, hazard_tuples, page_size=1000)
                    logging.info("✅ Hazard zone data inserted.")

                conn.commit()
                logging.info("✅ Clustering results and hazard zones successfully stored and committed.")
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(f"❌ Clustering results storage failed: {error}", exc_info=True)
            if conn: conn.rollback()
            raise
        finally:
            if conn: conn.close()

    def _create_hazard_zones(self, df_final_clusters):
        if df_final_clusters.empty or 'risk_zone' not in df_final_clusters.columns:
            return pd.DataFrame()

        hazard_summary = df_final_clusters[df_final_clusters['risk_zone'].isin(['High', 'Extreme'])] \
            .groupby('risk_zone').agg(
            center_lat=('centroid_lat', 'mean'),
            center_lon=('centroid_lon', 'mean'),
            avg_magnitude=('avg_magnitude', 'mean'),
            event_count=('id', 'count')
        ).reset_index()

        if hazard_summary.empty:
            return pd.DataFrame()

        hazard_summary.rename(columns={'risk_zone': 'risk_level'}, inplace=True)
        hazard_summary['boundary_coordinates'] = None
        hazard_summary['area_km2'] = None

        return hazard_summary[
            ['risk_level', 'avg_magnitude', 'event_count', 'boundary_coordinates', 'center_lat', 'center_lon',
             'area_km2']]