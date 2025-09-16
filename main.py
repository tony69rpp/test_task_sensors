from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Literal, List
from datetime import datetime, timezone
import sqlite3
import numpy as np
import json
import logging
import time

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("analyzer.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# FastAPI
app = FastAPI(title="Async Sensor Data Analyzer API")

DB_FILE = "analyses.db"

# Модель вх дданных
class MeasurementData(BaseModel):
    device_id: str
    measurements: List[int] = Field(..., min_length=1)
    measurement_type: Literal["temperature", "voltage", "current"]
    timestamp: datetime

    # Проверка времени на ошибку
    @field_validator("timestamp")
    @classmethod
    def check_timestamp(cls, v: datetime):
        now = datetime.now(timezone.utc)
        if v > now:
            raise ValueError("Timestamp из будущего")
        return v

    # Допустимые значения вх данных
    @model_validator(mode="after")
    def validate_measurements(self):
        m_type = self.measurement_type
        v = self.measurements

        if m_type == "temperature":
            invalid = [x for x in v if not (-50 <= x <= 150)]
            if invalid:
                raise ValueError(f"Температура вне диапазона: {invalid}")

        elif m_type == "voltage":
            invalid = [x for x in v if not (0 <= x <= 500)]
            if invalid:
                raise ValueError(f"Напряжение вне диапазона: {invalid}")

        elif m_type == "current":
            invalid = [x for x in v if not (0 <= x <= 200)]
            if invalid:
                raise ValueError(f"Ток вне диапазона: {invalid}")

        return self

# ИНИЦ БД
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS analyses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id TEXT,
        status TEXT,
        progres TEXT,
        error_message TEXT,
        resuts TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    """)
    conn.commit()
    conn.close()

init_db()

# Анализ
def analyze_measurements(data: MeasurementData):
    arr = np.array(data.measurements, dtype=float)
    mean = float(np.mean(arr))
    std = float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0
    min_val = float(np.min(arr))
    max_val = float(np.max(arr))

    # Выбросы >2σ
    if std > 0:
        outliers = [float(x) for x in arr if abs(x - mean) > 2 * std]
    else:
        outliers = []

    # Стабильность
    stability = float(std / mean) if mean != 0 else None

    return {
        "mean": mean,
        "std": std,
        "min": min_val,
        "max": max_val,
        "outliers": outliers,
        "stability": stability
    }


#  Background worker
def process_analysis(analysis_id: int, data: MeasurementData):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    logger.info(f"Processing analysis_id={analysis_id} device={data.device_id}")

    # Started параметр
    cursor.execute("UPDATE analyses SET progres=? WHERE id=?", ("started", analysis_id))
    conn.commit()

    try:
        # calc stats
        results = analyze_measurements(data)
        status = "completed"
        progres = "finished"
        error_message = None
    except Exception as e:
        results = None
        status = "failed"
        progres = "error"
        error_message = str(e)

    # save record to db
    cursor.execute("""
        UPDATE analyses
        SET status=?, progres=?, error_message=?, resuts=?, updated_at=?
        WHERE id=?
    """, (status, progres, error_message, json.dumps(results), datetime.now(timezone.utc).isoformat(), analysis_id))
    conn.commit()
    conn.close()
    logger.info(f"Analysis {analysis_id} completed with status={status}")

# Endpoint приема
@app.post("/analyze")
async def create_analysis(data: MeasurementData, background_tasks: BackgroundTasks):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    now = datetime.now(timezone.utc)

    cursor.execute("""
        INSERT INTO analyses (device_id, status, progres, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
    """, (data.device_id, "analyzing", "queued", now.isoformat(), now.isoformat()))
    analysis_id = cursor.lastrowid
    conn.commit()
    conn.close()

    logger.info(f"Queued analysis {analysis_id} for device {data.device_id}")

    # start background worker
    background_tasks.add_task(process_analysis, analysis_id, data)

    return {"analysis_id": analysis_id, "status": "analyzing"}

# Endpoint получения результатов
@app.get("/results/{analysis_id}")
async def get_results(analysis_id: int):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT status, progres, error_message, resuts FROM analyses WHERE id=?", (analysis_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Analysis not found")
    status, progres, error_message, resuts = row
    results_obj = json.loads(resuts) if resuts else None
    return {
        "analysis_id": analysis_id,
        "status": status,
        "progres": progres,
        "error_message": error_message,
        "resuts": results_obj
    }

# Root endpoint проверка серв
@app.get("/")
async def read_root():
    return {
        "message": "Async Sensor Data Analizer API",
        "endpoints": {
            "analyze": "POST /analyze - send data for analisis",
            "results": "GET /results/{id} - get analisis resuts"
        }
    }

# Запуск епты
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
