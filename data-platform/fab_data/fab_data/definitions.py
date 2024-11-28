from dagster import Definitions, load_assets_from_modules

from . import assets
from .raw_sensor import raw_data_sensor
from .resources import IcebergCatalogResource
from .checks import ingest_data_quality
# from fab_dbt.fab_dbt.assets import fab_dbt_asset_group
iceberg = IcebergCatalogResource()
all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={"iceberg_catalog": iceberg},
    sensors=[raw_data_sensor],
    asset_checks=[ingest_data_quality]
)
