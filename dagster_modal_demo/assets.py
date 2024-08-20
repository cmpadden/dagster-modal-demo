from pathlib import Path
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    PipesSubprocessClient,
    StaticPartitionsDefinition,
    asset,
)



# TODO - introduce `ModalProject`
# TODO - create custom PipesSubprocessClient -- ModalSubprocessClient

colors_partitions_def = StaticPartitionsDefinition(["red", "yellow", "blue"])


@asset(partitions_def=colors_partitions_def, compute_kind="modal")
def subprocess_asset(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
) -> MaterializeResult:
    path = Path(__file__).parent.parent / "modal" / "hello_world.py"
    return pipes_subprocess_client.run(
        command=["modal", "run", str(path)],
        context=context,
    ).get_materialize_result()
