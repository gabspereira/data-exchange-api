from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes.demand import router as demand_router

app = FastAPI(
    title="Demand Planning — Data Exchange API",
    description=(
        "REST API for exchanging demand planning data between the Databricks "
        "ELT pipeline and downstream applications (ML forecasting, BI tools, "
        "third-party integrations).\n\n"
        "**Pipeline:** SAP ERP CSV → Bronze (Delta) → Silver (Delta) → Gold: demand_rpt → API"
    ),
    version="1.0.0",
    contact={
        "name": "Gabriel Pereira",
        "url": "https://www.linkedin.com/in/gabrielpereira-dev",
    },
    license_info={"name": "MIT"},
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

app.include_router(demand_router, prefix="/api/v1")


@app.get("/health", tags=["Health"])
def health() -> dict:
    """Liveness probe."""
    return {"status": "ok"}
