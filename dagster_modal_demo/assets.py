import shutil

from pathlib import Path
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    PipesSubprocessClient,
    StaticPartitionsDefinition,
    asset,
)


# TODO - introduce `ModalProject`

oceans_partitions_def = StaticPartitionsDefinition(
    ["arctic", "atlantic", "indian", "pacific", "southern"]
)


@asset(partitions_def=oceans_partitions_def)
def subprocess_asset(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
) -> MaterializeResult:
    path = Path(__file__).parent.parent / "modal" / "hello_world.py"
    return pipes_subprocess_client.run(
        command=["modal", "run", str(path)],
        context=context,
    ).get_materialize_result()
