from dagster import Definitions

from pathlib import Path
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    StaticPartitionsDefinition,
    asset,
)
from dagster_modal_demo.dagster_modal.resources import ModalClient


colors_partitions_def = StaticPartitionsDefinition(["red", "yellow", "blue"])


@asset(partitions_def=colors_partitions_def, compute_kind="modal")
def modal_hello_world(
    context: AssetExecutionContext, modal: ModalClient
) -> MaterializeResult:
    return modal.run(
        func_ref="hello_world.py",
        context=context,
    ).get_materialize_result()


defs = Definitions(
    assets=[modal_hello_world],
    resources={
        "modal": ModalClient(project_directory=Path(__file__).parent.parent / "modal")
    },
)
