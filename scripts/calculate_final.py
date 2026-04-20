#!/usr/bin/env python3
"""Merge API ridership and baseline data into final processed outputs.

Reads monthly files from:
    - data/api/ridership
    - data/api/baseline

Writes merged files to:
    - data/api/processed

Outputs include:
    - baseline: baseline ridership value for matching month/day_group/geography
    - baseline_comparison: ridership / baseline (ratio, rounded to 4 decimals)
"""

from __future__ import annotations

print(">>> CALCULATE_FINAL VERSION TEST <<<")

from dataclasses import dataclass
import logging
from pathlib import Path
import sys
from typing import Dict, Optional, Sequence, Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.utils.socrata import repo_root
from scripts.utils.runtime import setup_script_logging

DAY_GROUP_ORDER: Tuple[str, ...] = ("total", "weekday", "weekend")
ALLOWED_DAY_GROUPS = set(DAY_GROUP_ORDER)


@dataclass(frozen=True)
class LevelConfig:
    """Configuration for a geographic aggregation level."""

    geo_key: Optional[str]
    ridership_required: Tuple[str, ...]
    baseline_required: Tuple[str, ...]
    ridership_unique_keys: Tuple[str, ...]
    baseline_unique_keys: Tuple[str, ...]
    output_columns: Tuple[str, ...]
    sort_columns: Tuple[str, ...]


LEVELS: Dict[str, LevelConfig] = {
    "station": LevelConfig(
        geo_key="complex_id",
        ridership_required=(
            "complex_id",
            "year",
            "month",
            "period",
            "day_group",
            "ridership",
            "omny_pct",
        ),
        baseline_required=("complex_id", "month", "day_group", "ridership"),
        ridership_unique_keys=("complex_id", "year", "month", "day_group"),
        baseline_unique_keys=("complex_id", "month", "day_group"),
        output_columns=(
            "complex_id",
            "year",
            "month",
            "period",
            "day_group",
            "ridership",
            "baseline",
            "baseline_comparison",
            "omny_pct",
        ),
        sort_columns=("year", "month", "complex_id", "day_group"),
    ),
    "puma": LevelConfig(
        geo_key="puma",
        ridership_required=(
            "puma",
            "year",
            "month",
            "period",
            "day_group",
            "ridership",
            "omny_pct",
        ),
        baseline_required=("puma", "month", "day_group", "ridership"),
        ridership_unique_keys=("puma", "year", "month", "day_group"),
        baseline_unique_keys=("puma", "month", "day_group"),
        output_columns=(
            "puma",
            "year",
            "month",
            "period",
            "day_group",
            "ridership",
            "baseline",
            "baseline_comparison",
            "omny_pct",
        ),
        sort_columns=("year", "month", "puma", "day_group"),
    ),
    "nyc": LevelConfig(
        geo_key=None,
        ridership_required=("year", "month", "period", "day_group", "ridership", "omny_pct"),
        baseline_required=("month", "day_group", "ridership"),
        ridership_unique_keys=("year", "month", "day_group"),
        baseline_unique_keys=("month", "day_group"),
        output_columns=(
            "year",
            "month",
            "period",
            "day_group",
            "ridership",
            "baseline",
            "baseline_comparison",
            "omny_pct",
        ),
        sort_columns=("year", "month", "day_group"),
    ),
}


def load_csv_required(
    path: Path, required_columns: Sequence[str], dataset_label: str
) -> pd.DataFrame:
    """Load a CSV and validate required columns."""
    if not path.exists():
        raise FileNotFoundError(f"Missing {dataset_label} file: {path}")

    df = pd.read_csv(path)
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(
            f"{dataset_label} missing required columns: {missing}. "
            f"Found: {list(df.columns)}"
        )
    return df


def validate_day_groups(df: pd.DataFrame, dataset_label: str) -> None:
    """Validate day_group values for the dataset."""
    if "day_group" not in df.columns:
        raise ValueError(f"{dataset_label} is missing required 'day_group' column")

    if df["day_group"].isna().any():
        raise ValueError(f"{dataset_label} contains null day_group values")

    values = set(df["day_group"].astype(str).unique())
    invalid = values - ALLOWED_DAY_GROUPS
    if invalid:
        raise ValueError(
            f"{dataset_label} has invalid day_group values: {sorted(invalid)}. "
            f"Allowed: {sorted(ALLOWED_DAY_GROUPS)}"
        )

    missing = ALLOWED_DAY_GROUPS - values
    if missing:
        raise ValueError(
            f"{dataset_label} is missing day_group values: {sorted(missing)}"
        )


def validate_unique(df: pd.DataFrame, keys: Sequence[str], dataset_label: str) -> None:
    """Ensure the given key columns uniquely identify each row."""
    duplicates = df[df.duplicated(list(keys), keep=False)]
    if duplicates.empty:
        return

    sample = duplicates.loc[:, list(keys)].head(10).to_dict(orient="records")
    raise ValueError(
        f"{dataset_label} has duplicate keys for {list(keys)}. "
        f"Sample duplicates: {sample}"
    )



def merge_level(level: str, config: LevelConfig, data_root: Path, logger: logging.Logger):
    """Load, validate, merge, and format one level output."""

    ridership_path = (
        data_root
        / "api"
        / "ridership"
        / f"monthly_ridership_{level}.csv"
        if level != "station"
        else data_root / "api" / "ridership" / "monthly_ridership_station.csv"
    )

    baseline_path = (
        data_root
        / "api"
        / "baseline"
        / f"monthly_baseline_{level}.csv"
    )

    logger.info("DEBUG ridership_path = %s", ridership_path)
    logger.info("DEBUG baseline_path  = %s", baseline_path)

    ridership_df = load_csv_required(
        ridership_path, config.ridership_required, f"{level} ridership"
    )
    baseline_df = load_csv_required(
        baseline_path, config.baseline_required, f"{level} baseline"
    )

    logger.info(
        "   Loaded %s ridership rows and %s baseline rows for %s.",
        f"{len(ridership_df):,}",
        f"{len(baseline_df):,}",
        level,
    )

    validate_day_groups(ridership_df, f"{level} ridership")
    validate_day_groups(baseline_df, f"{level} baseline")

    validate_unique(ridership_df, config.ridership_unique_keys, f"{level} ridership")
    validate_unique(baseline_df, config.baseline_unique_keys, f"{level} baseline")

    baseline_df = baseline_df.rename(columns={"ridership": "baseline"})

    merge_keys = ["month", "day_group"]
    if config.geo_key:
        merge_keys = [config.geo_key] + merge_keys

    merged_df = ridership_df.merge(
        baseline_df[merge_keys + ["baseline"]],
        on=merge_keys,
        how="left",
        validate="many_to_one",
    )

    merged_df["baseline_comparison"] = pd.NA
    valid = merged_df["baseline"].notna() & (merged_df["baseline"] > 0)
    merged_df.loc[valid, "baseline_comparison"] = (
        merged_df.loc[valid, "ridership"] / merged_df.loc[valid, "baseline"]
    ).round(4)

    merged_df["day_group"] = pd.Categorical(
        merged_df["day_group"], categories=DAY_GROUP_ORDER, ordered=True
    )
    merged_df = merged_df.sort_values(list(config.sort_columns), ignore_index=True)
    merged_df["day_group"] = merged_df["day_group"].astype(str)

    return merged_df.loc[:, list(config.output_columns)].copy()


def save_output(df: pd.DataFrame, path: Path, base_dir: Path, logger: logging.Logger) -> str:
    """Save one output dataframe and return a plain-language status."""
    path.parent.mkdir(parents=True, exist_ok=True)
    file_existed = path.exists()
    previous_count: int | None = None
    if file_existed:
        try:
            previous_count = len(pd.read_csv(path))
        except Exception:
            previous_count = None

    df.to_csv(path, index=False)
    current_count = len(df)

    if previous_count is None:
        status = "Created new output file." if not file_existed else "Updated"
    elif previous_count == current_count:
        status = "Up to date (row count unchanged)."
    else:
        status = f"Updated ({previous_count:,} → {current_count:,} rows)."

    logger.info("  ✅ %s (%s)", path.relative_to(base_dir), status)
    return status



def main() -> None:
    project_root = repo_root()
    data_root = project_root / "data"

    logger, _ = setup_script_logging(
        base_dir=project_root,
        logger_name=__name__,
        log_filename="calculate_final_api.log",
        fmt="%(message)s",
    )


    logger.info("\n🚀 API final merge: ridership + baseline")
    logger.info("   Output directory: data/api/processed\n")

    output_dir = data_root / "api" / "processed"
    output_stats: list[tuple[str, str, int]] = []
    level_labels = {
        "station": "🚉 Station",
        "puma": "🗺️ PUMA",
        "nyc": "🏙️ NYC",
    }

    try:
        for level, config in LEVELS.items():
            logger.info("%s", level_labels[level])
            output_df = merge_level(level, config, data_root, logger)
            logger.info("DEBUG merge_level base_dir param = %s", data_root)
            output_path = output_dir / f"monthly_ridership_{level}.csv"
            status = save_output(output_df, output_path, data_root, logger)
            output_stats.append((level_labels[level], status, len(output_df)))
            logger.info("")

        logger.info("📊 Step 3 summary")
        logger.info("-" * 60)
        overall_up_to_date = all(
            "Up to date" in status for _, status, _ in output_stats
        )
        logger.info("   Status: %s", "Up to date" if overall_up_to_date else "Updated")
        for level_name, status, row_count in output_stats:
            logger.info("   %s: %s (%s rows)", level_name, status, f"{row_count:,}")
        logger.info("✅ Step 3 complete.")
    except Exception as exc:
        logger.info("❌ Failed: %s", str(exc))
        raise


if __name__ == "__main__":
    main()
