from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import pandas as pd
import numpy as np
import joblib
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
import json
import os
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import sys

sys.path.append('/opt/airflow')

default_args = {
    'owner': 'magnitudr-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'catchup': False
}

DB_CONNECTION_STRING = 'postgresql://postgres:earthquake123@postgres:5432/magnitudr'
MODEL_DIR = Path("/opt/airflow/magnitudr/data/models")
VIZ_DIR = Path("/opt/airflow/magnitudr/data/plots")
MODEL_OUTPUT_PATH = str(MODEL_DIR / "earthquake_model.pkl")


def _load_training_data():
    logging.info("📊 Memuat data latih dari database...")
    conn = None
    try:
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        query = """
        SELECT 
            p.id, p.magnitude, p.depth, p.latitude, p.longitude, 
            p.spatial_density, p.hazard_score,
            c.risk_zone, c.cluster_size, c.avg_magnitude, c.max_magnitude
        FROM earthquakes_processed p
        JOIN earthquake_clusters c ON p.id = c.id
        WHERE c.risk_zone IS NOT NULL AND c.risk_zone != 'Unknown';
        """
        df = pd.read_sql(query, conn)
        logging.info(f"📊 Berhasil memuat {len(df)} sampel latih")
    except (Exception, psycopg2.DatabaseError) as e:
        logging.error(f"❌ Kueri database untuk data latih gagal: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()
    return df


def _engineer_features(df):
    logging.info("🔧 Melakukan rekayasa fitur risiko gempa...")
    df_processed = df.copy()

    df_processed['distance_from_jakarta'] = np.sqrt(
        (df_processed['latitude'] + 6.2088) ** 2 + (df_processed['longitude'] - 106.8456) ** 2)
    df_processed['magnitude_depth_ratio'] = df_processed['magnitude'] / (df_processed['depth'] + 1)
    df_processed['shallow_earthquake'] = (df_processed['depth'] < 70).astype(int)

    df_processed.fillna(df_processed.median(numeric_only=True), inplace=True)

    logging.info(f"🔧 Rekayasa fitur selesai: {len(df_processed.columns)} total fitur")
    return df_processed


def _train_and_evaluate_model(model, model_name, X_train, y_train, X_test, y_test):
    logging.info(f"--- Melatih dan mengevaluasi model: {model_name} ---")
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)

    metrics = {
        'test_accuracy': accuracy,
        'precision': report['weighted avg']['precision'],
        'recall': report['weighted avg']['recall'],
        'f1_score': report['weighted avg']['f1-score'],
    }

    logging.info(f"Model: {model_name}, Accuracy: {accuracy:.4f}")
    return model, metrics


def train_models_with_comparison(**context):
    logging.info("🤖 Memulai Pelatihan ML Mingguan dengan Perbandingan Model...")

    df = _load_training_data()
    if df.empty or len(df['risk_zone'].unique()) < 2:
        logging.warning("⚠️ Data atau kelas tidak cukup untuk melatih model. Melewati task.")
        return False

    engineered_df = _engineer_features(df)

    feature_cols = [
        'latitude', 'longitude', 'magnitude', 'depth', 'spatial_density',
        'hazard_score',
        'distance_from_jakarta', 'magnitude_depth_ratio', 'shallow_earthquake'
    ]
    target_col = 'risk_zone'

    X = engineered_df[feature_cols]
    y = engineered_df[target_col]

    le = LabelEncoder()
    y_encoded = le.fit_transform(y)

    X_train, X_test, y_train_encoded, y_test_encoded = train_test_split(
        X, y_encoded, test_size=0.2, random_state=42, stratify=y_encoded
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    models = {
        'RandomForest': RandomForestClassifier(
            random_state=42, n_estimators=100, max_depth=8,
            min_samples_leaf=3, class_weight='balanced'
        ),
        'LogisticRegression': LogisticRegression(
            random_state=42, solver='lbfgs', max_iter=1000,
            C=0.5, class_weight='balanced'
        )
    }

    model_comparison = {}
    rf_model, rf_metrics = _train_and_evaluate_model(models['RandomForest'], 'RandomForest', X_train_scaled,
                                                     y_train_encoded, X_test_scaled, y_test_encoded)
    lr_model, lr_metrics = _train_and_evaluate_model(models['LogisticRegression'], 'LogisticRegression', X_train_scaled,
                                                     y_train_encoded, X_test_scaled, y_test_encoded)

    model_comparison = {'RandomForest': rf_metrics, 'LogisticRegression': lr_metrics}

    best_model_name = max(model_comparison, key=lambda k: model_comparison[k]['f1_score'])
    best_model = rf_model if best_model_name == 'RandomForest' else lr_model
    best_model_metrics = model_comparison[best_model_name]

    logging.info(f"🏆 Model terbaik dipilih: {best_model_name}")

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(best_model, MODEL_OUTPUT_PATH)
    joblib.dump(scaler, MODEL_DIR / "earthquake_model_scaler.pkl")
    joblib.dump(le, MODEL_DIR / "earthquake_model_label_encoder.pkl")

    report_data = {
        'best_model': best_model_name,
        'model_comparison': model_comparison,
        'training_timestamp': datetime.now().isoformat()
    }

    with open(MODEL_DIR / "model_comparison_report.json", 'w') as f:
        json.dump(report_data, f, indent=2)

    conn_db = None
    try:
        conn_db = psycopg2.connect(DB_CONNECTION_STRING)
        with conn_db.cursor() as cursor:
            logging.info(f"💾 Menyimpan metadata model {best_model_name} ke database...")
            insert_query = """
                INSERT INTO ml_model_metadata (model_name, model_type, accuracy, precision_score, recall_score, f1_score, training_samples)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            model_type = 'RandomForest' if best_model_name == 'RandomForest' else 'LogisticRegression'
            record_to_insert = (
                best_model_name,
                model_type,
                best_model_metrics['test_accuracy'],
                best_model_metrics['precision'],
                best_model_metrics['recall'],
                best_model_metrics['f1_score'],
                len(X_train)
            )
            cursor.execute(insert_query, record_to_insert)
            conn_db.commit()
            logging.info("✅ Metadata model berhasil disimpan.")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"❌ Gagal menyimpan metadata model: {error}")
        if conn_db: conn_db.rollback()
    finally:
        if conn_db: conn_db.close()

    logging.info("✅ Pelatihan ML dengan perbandingan model selesai.")
    context['ti'].xcom_push(key='model_comparison_report', value=report_data)
    return True


def generate_ml_comparison_visualization(**context):
    try:
        report_data = context['ti'].xcom_pull(key='model_comparison_report', task_ids='train_models_with_comparison')
        if not report_data:
            logging.warning("Tidak ada laporan perbandingan model di XComs. Melewati visualisasi.")
            return False

        comparison_df = pd.DataFrame(report_data['model_comparison']).T.reset_index().rename(columns={'index': 'Model'})
        best_model_name = report_data['best_model']

        VIZ_DIR.mkdir(parents=True, exist_ok=True)

        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(10, 6))

        sns.barplot(x='Model', y='test_accuracy', data=comparison_df, ax=ax, palette=['#1f77b4', '#ff7f0e'])

        ax.set_title('Perbandingan Akurasi Model', fontsize=16)
        ax.set_ylabel('Akurasi', fontsize=12)
        ax.set_xlabel('Model', fontsize=12)
        ax.set_ylim(0.8, 1.0)

        plt.tight_layout()
        plt.savefig(VIZ_DIR / "ml_accuracy_comparison.png", dpi=300)
        plt.close()

        summary_text = f"""
        Ringkasan Perbandingan Model ML
        --------------------------------
        Timestamp: {report_data['training_timestamp']}
        Model Terbaik: {best_model_name}

        Metrik Performa:
        {comparison_df.to_string()}
        """

        with open(VIZ_DIR / "ml_comparison_summary.txt", 'w') as f:
            f.write(summary_text)

        logging.info("✅ Visualisasi perbandingan ML berhasil dibuat")
        return True
    except Exception as e:
        logging.error(f"❌ Visualisasi ML gagal: {e}")
        raise


with DAG(
        'earthquake_weekly_ml_training',
        default_args=default_args,
        description='Pipeline pelatihan ulang mingguan untuk klasifikasi zona risiko gempa',
        schedule_interval='0 0 * * 0',
        max_active_runs=1,
        tags=['earthquake', 'ml', 'training', 'weekly']
) as dag:
    task_train_models = PythonOperator(
        task_id='train_models_with_comparison',
        python_callable=train_models_with_comparison,
    )

    task_generate_viz = PythonOperator(
        task_id='generate_ml_comparison_visualization',
        python_callable=generate_ml_comparison_visualization,
    )

    task_train_models >> task_generate_viz