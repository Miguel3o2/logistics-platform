"""
src/contracts/schemas.py
========================
Pydantic V2 data contracts for all three ingestion sources.

Every Kafka message is validated against these contracts at the producer boundary.
Failures route to the Dead Letter Queue topic with full error context.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Any, Literal, Union
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

SUPPORTED_VERSIONS: frozenset[str] = frozenset({"1.0", "1.1"})


class VehicleStatus(str, Enum):
    moving    = "MOVING"
    idle      = "IDLE"
    stopped   = "STOPPED"
    breakdown = "BREAKDOWN"


class ShipmentStatus(str, Enum):
    pending    = "PENDING"
    in_transit = "IN_TRANSIT"
    delivered  = "DELIVERED"
    delayed    = "DELAYED"
    returned   = "RETURNED"


class WeatherCondition(str, Enum):
    clear   = "CLEAR"
    rain    = "RAIN"
    snow    = "SNOW"
    fog     = "FOG"
    storm   = "STORM"
    extreme = "EXTREME"


class CDCOperation(str, Enum):
    create = "c"
    update = "u"
    delete = "d"
    read   = "r"


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Annotated[str, Field(pattern=r"^\d+\.\d+$")] = "1.0"
    received_at:    AwareDatetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    producer_id: str | None = None

    @field_validator("schema_version")
    @classmethod
    def version_supported(cls, v: str) -> str:
        if v not in SUPPORTED_VERSIONS:
            raise ValueError(f"Unsupported schema_version {v!r}")
        return v


class GPSCoordinates(BaseModel):
    model_config = ConfigDict(frozen=True)
    latitude:   float = Field(ge=-90.0,  le=90.0)
    longitude:  float = Field(ge=-180.0, le=180.0)
    altitude_m: float | None = None
    accuracy_m: float | None = Field(default=None, ge=0)


class EngineMetrics(BaseModel):
    model_config = ConfigDict(frozen=True)
    rpm:               int   = Field(ge=0,    le=10_000)
    coolant_temp_c:    float = Field(ge=-40.0, le=200.0)
    oil_pressure_kpa:  float = Field(ge=0.0,   le=1000.0)
    fuel_level_pct:    float = Field(ge=0.0,   le=100.0)
    battery_voltage_v: float = Field(ge=0.0,   le=30.0)
    dtc_codes:         list[str] = Field(default_factory=list)

    @field_validator("dtc_codes")
    @classmethod
    def validate_dtc_format(cls, codes: list[str]) -> list[str]:
        pattern = re.compile(r"^[BCPU][0-9A-F]{4}$", re.IGNORECASE)
        for code in codes:
            if not pattern.match(code):
                raise ValueError(f"Invalid DTC code format: {code!r}")
        return codes


class IoTTelemetryEvent(EventEnvelope):
    """High-frequency vehicle telematics. Topic: logistics.telemetry.raw"""
    event_type:    Literal["iot_telemetry"] = "iot_telemetry"
    vehicle_id:    str = Field(min_length=3, max_length=20)
    driver_id:     str | None = None
    fleet_id:      str
    timestamp:     AwareDatetime
    status:        VehicleStatus
    speed_kmh:     float = Field(ge=0.0, le=300.0)
    heading_deg:   float = Field(ge=0.0, lt=360.0)
    gps:           GPSCoordinates
    engine:        EngineMetrics
    harsh_brake:   bool = False
    harsh_accel:   bool = False
    geofence_exit: bool = False

    @model_validator(mode="after")
    def speed_status_consistency(self) -> "IoTTelemetryEvent":
        if self.status == VehicleStatus.stopped and self.speed_kmh > 2.0:
            raise ValueError(
                f"Status STOPPED but speed={self.speed_kmh} km/h"
            )
        return self


class DebeziumSource(BaseModel):
    model_config = ConfigDict(frozen=True)
    version:   str
    connector: str = "postgresql"
    name:      str
    ts_ms:     int
    snapshot:  str | None = None
    db:        str
    schema:    str
    table:     str
    txId:      int | None = None
    lsn:       int | None = None


class ShipmentCDCEvent(EventEnvelope):
    """CDC from logistics_ops.public.shipments. Topic: logistics.cdc.shipments"""
    event_type: Literal["shipment_cdc"] = "shipment_cdc"
    op:         CDCOperation
    ts_ms:      int
    source:     DebeziumSource
    before:     dict[str, Any] | None = None
    after:      dict[str, Any] | None = None

    @model_validator(mode="after")
    def payload_valid_for_operation(self) -> "ShipmentCDCEvent":
        if self.op == CDCOperation.create and self.after is None:
            raise ValueError("INSERT must have `after` payload")
        if self.op == CDCOperation.delete and self.before is None:
            raise ValueError("DELETE must have `before` payload")
        return self


class VehicleCDCEvent(EventEnvelope):
    """CDC from logistics_ops.public.vehicles. Topic: logistics.cdc.vehicles"""
    event_type: Literal["vehicle_cdc"] = "vehicle_cdc"
    op:         CDCOperation
    ts_ms:      int
    source:     DebeziumSource
    before:     dict[str, Any] | None = None
    after:      dict[str, Any] | None = None


class WeatherEvent(EventEnvelope):
    """External weather API enrichment. Topic: logistics.weather.raw"""
    event_type:       Literal["weather"] = "weather"
    corridor_id:      str
    lat:              float = Field(ge=-90.0,  le=90.0)
    lon:              float = Field(ge=-180.0, le=180.0)
    observed_at:      AwareDatetime
    condition:        WeatherCondition
    temp_c:           float = Field(ge=-80.0, le=60.0)
    wind_kph:         float = Field(ge=0.0,   le=400.0)
    visibility_km:    float = Field(ge=0.0,   le=50.0)
    precipitation_mm: float = Field(ge=0.0,   le=500.0)
    humidity_pct:     float = Field(ge=0.0,   le=100.0)
    road_risk_score:  float = Field(ge=0.0,   le=10.0)

    @model_validator(mode="after")
    def snow_requires_low_temp(self) -> "WeatherEvent":
        if self.condition == WeatherCondition.snow and self.temp_c > 5.0:
            raise ValueError(
                f"SNOW condition but temp={self.temp_c}°C — sensor error"
            )
        return self


class DLQEvent(BaseModel):
    """Dead Letter Queue envelope for any validation failure."""
    model_config = ConfigDict(extra="allow")
    failed_at:        AwareDatetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    source_topic:     str
    source_offset:    int | None = None
    source_partition: int | None = None
    raw_payload:      str
    error_type:       str
    error_detail:     str
    producer_id:      str | None = None


LogisticsEvent = Annotated[
    Union[
        IoTTelemetryEvent,
        ShipmentCDCEvent,
        VehicleCDCEvent,
        WeatherEvent,
    ],
    Field(discriminator="event_type"),
]


def parse_event(raw: dict[str, Any]) -> EventEnvelope:
    """
    Parse and validate any incoming event dict.
    Raises pydantic.ValidationError on failure — callers route to DLQ.
    """
    from pydantic import TypeAdapter
    adapter: TypeAdapter[EventEnvelope] = TypeAdapter(LogisticsEvent)  # type: ignore
    return adapter.validate_python(raw)
