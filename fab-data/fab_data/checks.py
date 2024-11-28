
from typing import Dict, Generator, List, Any
from dagster import (
    asset_check, AssetCheckResult
)

from .assets import ingest_raw_fab_data
from pathlib import Path
import daft


@asset_check(asset=ingest_raw_fab_data, blocking=True, description="Check for temperature and defects data quality metrics")
def ingest_data_quality(ingest_raw_fab_data) -> Generator[AssetCheckResult, Any, None]:

    for filename in ingest_raw_fab_data:
        file_path = Path("../raw_data") / filename
        if not file_path.exists():
            raise FileNotFoundError(f"Input file not found: {file_path}")

        df: daft.DataFrame = daft.read_json(str(file_path))
        df_temp: daft.DataFrame = df.select('temperature_c').where("temperature_c > 299")
        temp_row_cnt: Dict[str, List[Any]] = df_temp.count().to_pydict()

        df_defects: daft.DataFrame = df.select('defects_detected').where("defects_detected > 4")
        defects_row_cnt: Dict[str, List[Any]] = df_defects.count().to_pydict()

        metadata: Dict[str, int] = {"defects_errors": int(
            defects_row_cnt['count'][0]), "temperature_errors": int(temp_row_cnt['count'][0])}

        yield AssetCheckResult(
            passed=bool(temp_row_cnt['count'][0] < 20),
            metadata=metadata,
        )
        yield AssetCheckResult(
            passed=bool(defects_row_cnt['count'][0] < 820),
            metadata=metadata,
        )
