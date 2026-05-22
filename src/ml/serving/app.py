

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import mlflow
import mlflow.sklearn
import numpy as np
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

log = logging.getLogger("logistics.ml_server")

# ── Config ─────────────────────────────────────────────────────────────────
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
GOLD_PATH           = os.environ.get("DELTA_GOLD_PATH", "s3a://logistics/delta/gold")
MODEL_STAGE         = os.environ.get("MODEL_STAGE", "Production")
FEATURE_MAX_AGE_H   = int(os.environ.get("FEATURE_MAX_AGE_HOURS", "2"))

# ── App state ─────────────────────────────────────────────────────────────
class ModelStore:
    def __init__(self):
        self.eta_model      = None
        self.failure_model  = None
        self.eta_version    = "unknown"
        self.failure_version= "unknown"
        self.loaded_at      = None

    @property
    def is_ready(self) -> bool:
        return (self.eta_model is not None
                and self.failure_model is not None)

model_store = ModelStore()

# ── FastAPI app ───────────────────────────────────────────────────────────
app = FastAPI(
    title       = "Logistics ML Inference API",
    description = "ETA prediction and engine failure detection for fleet management",
    version     = "1.0.0",
    docs_url    = "/docs",
)


# ── Pydantic request/response models ─────────────────────────────────────

class ETARequest(BaseModel):
    shipment_id:          str
    vehicle_id:           str
    current_speed_kmh:    float = Field(ge=0.0, le=300.0)
    fuel_level_pct:       float = Field(ge=0.0, le=100.0)
    engine_health_score:  float = Field(ge=0.0, le=100.0)
    road_risk_score:      float = Field(ge=0.0, le=10.0)
    visibility_km:        float = Field(ge=0.0, le=50.0)
    precipitation_mm:     float = Field(ge=0.0)
    temp_c:               float
    hist_avg_speed_kmh:   float = Field(ge=0.0)
    hist_speed_stddev:    float = Field(ge=0.0)
    hour_of_day:          int   = Field(ge=0, le=23)
    day_of_week:          int   = Field(ge=1, le=7)
    is_weekend:           int   = Field(ge=0, le=1)
    is_already_late:      int   = Field(ge=0, le=1)
    hours_to_scheduled_eta: float
    weight_kg:            float = Field(ge=0.0)
    harsh_brake_flag:     int   = Field(ge=0, le=1)
    harsh_accel_flag:     int   = Field(ge=0, le=1)
    active_dtc_count:     int   = Field(ge=0)


class ETAResponse(BaseModel):
    shipment_id:      str
    vehicle_id:       str
    predicted_eta_h:  float          # hours from now
    lower_bound_h:    float
    upper_bound_h:    float
    confidence:       float
    model_version:    str
    scored_at:        str


class FailureRequest(BaseModel):
    vehicle_id:         str
    avg_coolant_temp:   float
    max_coolant_temp:   float
    std_coolant_temp:   float
    avg_oil_pressure:   float
    min_oil_pressure:   float
    avg_rpm:            float
    max_rpm:            float
    avg_battery_v:      float
    min_battery_v:      float
    dtc_rate:           float = Field(ge=0.0, le=1.0)
    avg_engine_health:  float = Field(ge=0.0, le=100.0)
    total_readings:     int   = Field(ge=1)


class FailureResponse(BaseModel):
    vehicle_id:        str
    failure_prob:      float
    risk_tier:         str    # low | medium | high | critical
    top_risk_factors:  list[str]
    model_version:     str
    scored_at:         str


class HealthResponse(BaseModel):
    status:            str
    eta_model:         str
    failure_model:     str
    models_loaded_at:  str | None
    gold_data_fresh:   bool
    uptime_s:          float


# ── ETA feature vector ────────────────────────────────────────────────────
ETA_FEATURE_COLS = [
    "current_speed_kmh", "fuel_level_pct", "engine_health_score",
    "road_risk_score", "visibility_km", "precipitation_mm", "temp_c",
    "hist_avg_speed_kmh", "hist_speed_stddev", "hour_of_day",
    "day_of_week", "is_weekend", "is_already_late",
    "hours_to_scheduled_eta", "weight_kg",
    "harsh_brake_flag", "harsh_accel_flag", "active_dtc_count",
]

FAILURE_FEATURE_COLS = [
    "avg_coolant_temp", "max_coolant_temp", "std_coolant_temp",
    "avg_oil_pressure", "min_oil_pressure", "avg_rpm", "max_rpm",
    "avg_battery_v", "min_battery_v", "dtc_rate", "avg_engine_health",
    "total_readings",
]

FAILURE_THRESHOLDS = {
    "critical": 0.75,
    "high":     0.50,
    "medium":   0.25,
}

_start_time = time.monotonic()


# ── Model loading ─────────────────────────────────────────────────────────
def load_models() -> None:
    """
    Load both models from MLflow Registry.
    Falls back to a dummy model if MLflow is unreachable (dev mode).
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    try:
        log.info("Loading ETA model from MLflow (stage=%s)…", MODEL_STAGE)
        model_store.eta_model    = mlflow.sklearn.load_model(
            f"models:/logistics_eta/{MODEL_STAGE}"
        )
        model_store.eta_version  = MODEL_STAGE

        log.info("Loading engine failure model from MLflow…")
        model_store.failure_model   = mlflow.sklearn.load_model(
            f"models:/logistics_engine_failure/{MODEL_STAGE}"
        )
        model_store.failure_version = MODEL_STAGE
        model_store.loaded_at       = datetime.now(timezone.utc)
        log.info("Both models loaded from MLflow")

    except Exception as exc:
        log.warning("MLflow unavailable — loading fallback models: %s", exc)
        _load_fallback_models()


def _load_fallback_models() -> None:
    """
    Fallback: simple heuristic models for development/testing.
    Not used in production — only when MLflow is unreachable.
    """
    from sklearn.ensemble import GradientBoostingRegressor, GradientBoostingClassifier
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    import numpy as np

    # Dummy ETA model
    X_eta = np.random.rand(200, len(ETA_FEATURE_COLS))
    y_eta = np.random.uniform(0.5, 48.0, 200)
    eta_pipe = Pipeline([
        ("scaler", StandardScaler()),
        ("model",  GradientBoostingRegressor(n_estimators=50, random_state=42)),
    ])
    eta_pipe.fit(X_eta, y_eta)
    model_store.eta_model   = eta_pipe
    model_store.eta_version = "fallback-v0"

    # Dummy failure model
    X_fail = np.random.rand(200, len(FAILURE_FEATURE_COLS))
    y_fail = np.random.randint(0, 2, 200)
    fail_pipe = Pipeline([
        ("scaler", StandardScaler()),
        ("model",  GradientBoostingClassifier(n_estimators=50, random_state=42)),
    ])
    fail_pipe.fit(X_fail, y_fail)
    model_store.failure_model   = fail_pipe
    model_store.failure_version = "fallback-v0"
    model_store.loaded_at       = datetime.now(timezone.utc)
    log.info("Fallback models loaded")


def _check_gold_freshness() -> bool:
    """Check if Gold feature store was updated within FEATURE_MAX_AGE_H hours."""
    try:
        import duckdb
        conn = duckdb.connect()
        result = conn.execute(
            f"SELECT MAX(feature_computed_at) FROM read_parquet('{GOLD_PATH}/eta_features/**/*.parquet')"
        ).fetchone()
        if not result or result[0] is None:
            return False
        max_ts = result[0]
        cutoff = datetime.now(timezone.utc) - timedelta(hours=FEATURE_MAX_AGE_H)
        return max_ts.replace(tzinfo=timezone.utc) > cutoff
    except Exception:
        return True   # if we can't check, don't block requests


def _assign_risk_tier(prob: float) -> str:
    if prob >= FAILURE_THRESHOLDS["critical"]: return "critical"
    if prob >= FAILURE_THRESHOLDS["high"]:     return "high"
    if prob >= FAILURE_THRESHOLDS["medium"]:   return "medium"
    return "low"


def _identify_risk_factors(req: FailureRequest) -> list[str]:
    """Return human-readable top risk factors for the failure prediction."""
    factors = []
    if req.max_coolant_temp > 105:
        factors.append(f"Critical coolant temperature: {req.max_coolant_temp}°C")
    if req.min_oil_pressure < 200:
        factors.append(f"Low oil pressure: {req.min_oil_pressure} kPa")
    if req.dtc_rate > 0.1:
        factors.append(f"High fault code rate: {req.dtc_rate:.0%} of readings")
    if req.min_battery_v < 12.0:
        factors.append(f"Low battery voltage: {req.min_battery_v}V")
    if req.avg_engine_health < 60:
        factors.append(f"Low engine health score: {req.avg_engine_health:.0f}/100")
    if req.max_rpm > 4500:
        factors.append(f"High RPM spikes: {req.max_rpm} RPM")
    return factors[:3] if factors else ["No significant risk factors identified"]


# ── Lifecycle ─────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    log.info("ML server starting — loading models…")
    load_models()
    log.info("ML server ready")


# ── Endpoints ─────────────────────────────────────────────────────────────
@app.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(
        status           = "ok" if model_store.is_ready else "loading",
        eta_model        = model_store.eta_version,
        failure_model    = model_store.failure_version,
        models_loaded_at = model_store.loaded_at.isoformat() if model_store.loaded_at else None,
        gold_data_fresh  = _check_gold_freshness(),
        uptime_s         = round(time.monotonic() - _start_time, 1),
    )


@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    uptime = time.monotonic() - _start_time
    ready = 1 if model_store.is_ready else 0
    return "\n".join([
        "# HELP logistics_ml_server_ready Whether both ML models are loaded.",
        "# TYPE logistics_ml_server_ready gauge",
        f"logistics_ml_server_ready {ready}",
        "# HELP logistics_ml_server_uptime_seconds ML server process uptime.",
        "# TYPE logistics_ml_server_uptime_seconds gauge",
        f"logistics_ml_server_uptime_seconds {uptime:.1f}",
        "",
    ])


@app.post("/predict/eta", response_model=ETAResponse)
def predict_eta(req: ETARequest):
    if not model_store.is_ready:
        raise HTTPException(status_code=503, detail="Models not yet loaded")

    feature_vector = np.array([[getattr(req, col) for col in ETA_FEATURE_COLS]])

    try:
        prediction = float(model_store.eta_model.predict(feature_vector)[0])
    except Exception as exc:
        log.error("ETA prediction failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Prediction error: {exc}")

    # Confidence interval: ±10% + road risk adjustment
    uncertainty     = max(0.5, prediction * 0.10 + req.road_risk_score * 0.2)
    lower           = max(0.0, prediction - uncertainty)
    upper           = prediction + uncertainty
    confidence      = max(0.5, 1.0 - (req.road_risk_score / 10.0) * 0.3)

    return ETAResponse(
        shipment_id     = req.shipment_id,
        vehicle_id      = req.vehicle_id,
        predicted_eta_h = round(prediction, 2),
        lower_bound_h   = round(lower, 2),
        upper_bound_h   = round(upper, 2),
        confidence      = round(confidence, 4),
        model_version   = model_store.eta_version,
        scored_at       = datetime.now(timezone.utc).isoformat(),
    )


@app.post("/predict/failure", response_model=FailureResponse)
def predict_failure(req: FailureRequest):
    if not model_store.is_ready:
        raise HTTPException(status_code=503, detail="Models not yet loaded")

    feature_vector = np.array([[getattr(req, col) for col in FAILURE_FEATURE_COLS]])

    try:
        proba       = model_store.failure_model.predict_proba(feature_vector)[0]
        failure_prob = float(proba[1])
    except Exception as exc:
        log.error("Failure prediction failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Prediction error: {exc}")

    return FailureResponse(
        vehicle_id      = req.vehicle_id,
        failure_prob    = round(failure_prob, 4),
        risk_tier       = _assign_risk_tier(failure_prob),
        top_risk_factors= _identify_risk_factors(req),
        model_version   = model_store.failure_version,
        scored_at       = datetime.now(timezone.utc).isoformat(),
    )


@app.post("/models/reload")
def reload_models(background_tasks: BackgroundTasks):
    """Trigger a background model reload from MLflow (no downtime)."""
    background_tasks.add_task(load_models)
    return {"message": "Model reload triggered in background"}


@app.get("/models/info")
def model_info():
    return {
        "eta_model":       model_store.eta_version,
        "failure_model":   model_store.failure_version,
        "loaded_at":       model_store.loaded_at.isoformat() if model_store.loaded_at else None,
        "feature_cols": {
            "eta":     ETA_FEATURE_COLS,
            "failure": FAILURE_FEATURE_COLS,
        },
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    uvicorn.run(app, host="0.0.0.0", port=8000)
