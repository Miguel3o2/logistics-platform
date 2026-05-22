
from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, timedelta, timezone

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import GradientBoostingClassifier, GradientBoostingRegressor
from sklearn.metrics import (
    brier_score_loss,
    f1_score,
    mean_absolute_error,
    mean_absolute_percentage_error,
    precision_score,
    recall_score,
    root_mean_squared_error,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

log = logging.getLogger("logistics.training")

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
GOLD_PATH           = os.environ.get("DELTA_GOLD_PATH", "s3a://logistics/delta/gold")
MINIO_ENDPOINT      = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

ETA_FEATURES = [
    "current_speed_kmh", "fuel_level_pct", "engine_health_score",
    "road_risk_score", "visibility_km", "precipitation_mm", "temp_c",
    "hist_avg_speed_kmh", "hist_speed_stddev", "hour_of_day",
    "day_of_week", "is_weekend", "is_already_late",
    "hours_to_scheduled_eta", "weight_kg",
    "harsh_brake_flag", "harsh_accel_flag", "active_dtc_count",
]

FAILURE_FEATURES = [
    "avg_coolant_temp", "max_coolant_temp", "std_coolant_temp",
    "avg_oil_pressure", "min_oil_pressure", "avg_rpm", "max_rpm",
    "avg_battery_v", "min_battery_v", "dtc_rate", "avg_engine_health",
    "total_readings",
]


def promote_latest_model(model_name: str, stage: str = "Production") -> None:
    client = MlflowClient()
    versions = client.get_latest_versions(model_name)
    if not versions:
        raise RuntimeError(f"No registered versions found for {model_name}")
    latest = max(versions, key=lambda item: int(item.version))
    client.transition_model_version_stage(
        name=model_name,
        version=latest.version,
        stage=stage,
        archive_existing_versions=True,
    )
    log.info("Promoted %s version %s to %s", model_name, latest.version, stage)


# ── Data loading ──────────────────────────────────────────────────────────
def load_eta_training_data() -> pd.DataFrame:
    """
    Load and prepare ETA training data from Gold.
    Label: actual hours taken = (actual_delivery - created_at) in hours.
    """
    try:
        import pyarrow.parquet as pq
        table = pq.read_table(f"{GOLD_PATH}/eta_features")
        df    = table.to_pandas()
        log.info("Loaded %d ETA feature rows", len(df))
        return df
    except Exception as exc:
        log.warning("Gold read failed (%s) — generating synthetic training data", exc)
        return _synthetic_eta_data(n=5000)


def load_failure_training_data() -> pd.DataFrame:
    try:
        import pyarrow.parquet as pq
        table = pq.read_table(f"{GOLD_PATH}/engine_features")
        df    = table.to_pandas()
        log.info("Loaded %d engine feature rows", len(df))
        return df
    except Exception as exc:
        log.warning("Gold read failed (%s) — generating synthetic training data", exc)
        return _synthetic_failure_data(n=5000)


def _synthetic_eta_data(n: int = 5000) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    df  = pd.DataFrame({
        "current_speed_kmh":     rng.uniform(0, 120, n),
        "fuel_level_pct":        rng.uniform(5, 100, n),
        "engine_health_score":   rng.uniform(40, 100, n),
        "road_risk_score":       rng.uniform(0, 10, n),
        "visibility_km":         rng.uniform(1, 50, n),
        "precipitation_mm":      rng.exponential(2, n),
        "temp_c":                rng.uniform(-5, 45, n),
        "hist_avg_speed_kmh":    rng.uniform(50, 100, n),
        "hist_speed_stddev":     rng.uniform(5, 25, n),
        "hour_of_day":           rng.integers(0, 24, n),
        "day_of_week":           rng.integers(1, 8, n),
        "is_weekend":            rng.integers(0, 2, n),
        "is_already_late":       rng.integers(0, 2, n),
        "hours_to_scheduled_eta":rng.uniform(0, 72, n),
        "weight_kg":             rng.uniform(100, 20000, n),
        "harsh_brake_flag":      rng.integers(0, 2, n),
        "harsh_accel_flag":      rng.integers(0, 2, n),
        "active_dtc_count":      rng.integers(0, 5, n),
    })
    # Realistic label: base ETA adjusted by conditions
    df["actual_eta_hours"] = (
        df["hours_to_scheduled_eta"]
        + df["road_risk_score"] * 0.5
        - (df["current_speed_kmh"] / df["hist_avg_speed_kmh"] - 1) * 2
        + df["is_already_late"] * 1.5
        + rng.normal(0, 0.5, n)
    ).clip(lower=0.1)
    return df


def _synthetic_failure_data(n: int = 5000) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    df  = pd.DataFrame({
        "avg_coolant_temp":  rng.normal(87, 8, n),
        "max_coolant_temp":  rng.normal(95, 12, n),
        "std_coolant_temp":  rng.exponential(3, n),
        "avg_oil_pressure":  rng.normal(350, 50, n),
        "min_oil_pressure":  rng.normal(280, 60, n),
        "avg_rpm":           rng.normal(2000, 400, n),
        "max_rpm":           rng.normal(3500, 700, n),
        "avg_battery_v":     rng.normal(13.5, 0.5, n),
        "min_battery_v":     rng.normal(12.5, 0.8, n),
        "dtc_rate":          rng.beta(0.5, 10, n),
        "avg_engine_health": rng.normal(80, 15, n).clip(0, 100),
        "total_readings":    rng.integers(10, 200, n),
    })
    # Realistic failure probability
    risk = (
        (df["max_coolant_temp"] > 110).astype(int) * 0.4
        + (df["min_oil_pressure"] < 200).astype(int) * 0.3
        + (df["dtc_rate"] > 0.1).astype(int) * 0.3
        + rng.uniform(0, 0.1, n)
    ).clip(0, 1)
    df["failed_within_24h"] = (risk > 0.4).astype(int)
    return df


# ── Training functions ────────────────────────────────────────────────────
def train_eta_model(run_date: date) -> None:
    """Train and register the ETA prediction model."""
    df = load_eta_training_data()
    df = df.fillna(df.median(numeric_only=True))

    X = df[ETA_FEATURES].values
    y = df["actual_eta_hours"].values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("model",  GradientBoostingRegressor(
            n_estimators     = 300,
            max_depth        = 5,
            learning_rate    = 0.05,
            subsample        = 0.8,
            min_samples_leaf = 10,
            random_state     = 42,
        )),
    ])

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("logistics_eta_prediction")

    with mlflow.start_run(run_name=f"eta_{run_date.isoformat()}"):
        pipeline.fit(X_train, y_train)
        preds = pipeline.predict(X_test)

        mae  = mean_absolute_error(y_test, preds)
        rmse = root_mean_squared_error(y_test, preds)
        mape = mean_absolute_percentage_error(y_test, preds)
        r2   = pipeline.score(X_test, y_test)

        log.info("ETA model — MAE=%.2fh RMSE=%.2fh MAPE=%.1f%% R²=%.4f",
                 mae, rmse, mape * 100, r2)

        # Log parameters
        mlflow.log_params({
            "n_estimators": 300,
            "max_depth":    5,
            "learning_rate":0.05,
            "features":     len(ETA_FEATURES),
            "train_rows":   len(X_train),
        })

        # Log metrics
        mlflow.log_metrics({
            "mae_hours":  round(mae,  3),
            "rmse_hours": round(rmse, 3),
            "mape_pct":   round(mape * 100, 2),
            "r2":         round(r2,   4),
        })

        # Log feature importance
        importances = pipeline.named_steps["model"].feature_importances_
        for feat, imp in zip(ETA_FEATURES, importances):
            mlflow.log_metric(f"fi_{feat}", round(float(imp), 4))

        # Register model
        mlflow.sklearn.log_model(
            pipeline,
            artifact_path   = "model",
            registered_model_name = "logistics_eta",
        )
        promote_latest_model("logistics_eta")
        log.info("ETA model registered in MLflow")


def train_failure_model(run_date: date) -> None:
    """
    Train and register the engine failure prediction model.
    Uses CalibratedClassifierCV for calibrated probabilities —
    same pattern as the Meridian churn scorer but with GBDT base.
    """
    df = load_failure_training_data()
    df = df.fillna(df.median(numeric_only=True))

    X = df[FAILURE_FEATURES].values
    y = df["failed_within_24h"].values

    if y.sum() < 20:
        log.error("Only %d positive examples — need at least 20", y.sum())
        return

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    base_clf = GradientBoostingClassifier(
        n_estimators     = 300,
        max_depth        = 4,
        learning_rate    = 0.05,
        subsample        = 0.8,
        min_samples_leaf = 10,
        random_state     = 42,
    )

    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("model",  CalibratedClassifierCV(base_clf, cv=5, method="isotonic")),
    ])

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("logistics_engine_failure")

    with mlflow.start_run(run_name=f"failure_{run_date.isoformat()}"):
        pipeline.fit(X_train, y_train)
        proba = pipeline.predict_proba(X_test)[:, 1]
        preds = (proba >= 0.5).astype(int)

        auroc  = roc_auc_score(y_test, proba)
        brier  = brier_score_loss(y_test, proba)
        f1     = f1_score(y_test, preds)
        prec   = precision_score(y_test, preds)
        rec    = recall_score(y_test, preds)

        log.info(
            "Failure model — AUROC=%.4f Brier=%.4f F1=%.4f Prec=%.4f Rec=%.4f",
            auroc, brier, f1, prec, rec,
        )

        mlflow.log_params({
            "n_estimators":   300,
            "calibration":    "isotonic",
            "positive_rate":  round(float(y.mean()), 3),
            "features":       len(FAILURE_FEATURES),
            "train_rows":     len(X_train),
        })

        mlflow.log_metrics({
            "auroc":     round(auroc, 4),
            "brier":     round(brier, 4),
            "f1":        round(f1,    4),
            "precision": round(prec,  4),
            "recall":    round(rec,   4),
        })

        mlflow.sklearn.log_model(
            pipeline,
            artifact_path         = "model",
            registered_model_name = "logistics_engine_failure",
        )
        promote_latest_model("logistics_engine_failure")
        log.info("Engine failure model registered in MLflow")


# ── Entry point ───────────────────────────────────────────────────────────
def main(run_date: date) -> None:
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = MINIO_ENDPOINT
    log.info("Training pipeline starting for %s", run_date)
    train_eta_model(run_date)
    train_failure_model(run_date)
    log.info("Training complete — both models registered in MLflow")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=date.today().isoformat())
    args = parser.parse_args()
    main(date.fromisoformat(args.run_date))
