from dagster import Definitions, load_assets_from_modules

from dagster_modal_demo import assets
from dagster import PipesSubprocessClient

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={"pipes_subprocess_client": PipesSubprocessClient()},
)
