import shutil

from pathlib import Path
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    PipesSubprocessClient,
    asset,
)


# TODO - introduce `ModalProject`


@asset
def subprocess_asset(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
) -> MaterializeResult:
    path = Path(__file__).parent.parent / "modal" / "hello_world.py"
    python = shutil.which("python")
    if not python:
        raise Exception("No suitible Python command found")
    return pipes_subprocess_client.run(
        command=[python, str(path)], context=context
    ).get_materialize_result()
