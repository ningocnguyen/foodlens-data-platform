"""Central configuration for the FoodLens pipeline."""

from dataclasses import dataclass
import os

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    base_url: str
    category: str
    page_size: int
    max_pages: int
    user_agent: str
    bronze_root: str
    silver_root: str
    gold_root: str
    quarantine_root: str
    report_root: str


def get_settings() -> Settings:
    """Load and validate pipeline settings."""

    user_agent = os.getenv("FOODLENS_USER_AGENT")

    if not user_agent:
        raise ValueError(
            "FOODLENS_USER_AGENT is missing. "
            "Create a .env file from .env.example."
        )

    return Settings(
        base_url=os.getenv(
            "OPEN_FOOD_FACTS_BASE_URL",
            "https://world.openfoodfacts.org",
        ),
        category=os.getenv(
            "OPEN_FOOD_FACTS_CATEGORY",
            "chocolates",
        ),
        page_size=int(
            os.getenv("OPEN_FOOD_FACTS_PAGE_SIZE", "50")
        ),
        max_pages=int(
            os.getenv("OPEN_FOOD_FACTS_MAX_PAGES", "2")
        ),
        user_agent=user_agent,
        bronze_root=os.getenv("BRONZE_ROOT", "data/bronze"),
        silver_root=os.getenv("SILVER_ROOT", "data/silver"),
        gold_root=os.getenv("GOLD_ROOT", "data/gold"),
        quarantine_root=os.getenv(
            "QUARANTINE_ROOT",
            "data/quarantine",
        ),
        report_root=os.getenv("REPORT_ROOT", "reports"),
    )