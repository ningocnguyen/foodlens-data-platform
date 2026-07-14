"""Extract Open Food Facts records into the Bronze layer"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import time
from typing import Any

from src.api_client import OpenFoodFactsClient
from src.config import Settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExtractionResult:
    run_id: str
    ingestion_date: str
    product_path: Path
    metadata_path: Path
    record_count: int


def create_run_id() -> str:
    """Create a unique UTC pipeline-run identifier"""

    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def extract_products(settings: Settings) -> ExtractionResult:
    """Extract API records and preserve them as Bronze JSON"""

    started_at = datetime.now(timezone.utc)
    run_id = create_run_id()
    ingestion_date = started_at.date().isoformat()

    output_directory = (
        Path(settings.bronze_root)
        / f"ingestion_date={ingestion_date}"
        / f"run_id={run_id}"
    )
    output_directory.mkdir(parents=True, exist_ok=True)

    client = OpenFoodFactsClient(
        base_url=settings.base_url,
        user_agent=settings.user_agent,
    )

    all_products: list[dict[str, Any]] = []

    for page in range(1, settings.max_pages + 1):
        products = client.fetch_category_page(
            category=settings.category,
            page=page,
            page_size=settings.page_size,
        )

        all_products.extend(products)

        if page < settings.max_pages:
            time.sleep(6)

    product_path = output_directory / "products.json"
    metadata_path = output_directory / "metadata.json"

    product_path.write_text(
        json.dumps(
            all_products,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    completed_at = datetime.now(timezone.utc)

    metadata = {
        "run_id": run_id,
        "source": "open_food_facts",
        "category": settings.category,
        "page_size": settings.page_size,
        "pages_requested": settings.max_pages,
        "record_count": len(all_products),
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": round(
            (completed_at - started_at).total_seconds(),
            2,
        ),
        "product_path": str(product_path),
    }

    metadata_path.write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    logger.info(
        "Bronze extraction complete: records=%s path=%s",
        len(all_products),
        product_path,
    )

    return ExtractionResult(
        run_id=run_id,
        ingestion_date=ingestion_date,
        product_path=product_path,
        metadata_path=metadata_path,
        record_count=len(all_products),
    )