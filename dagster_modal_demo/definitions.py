from pathlib import Path

import dagster as dg
from dagster import Definitions

from dagster_modal_demo.dagster_modal.resources import ModalClient

colors_partitions_def = dg.StaticPartitionsDefinition(["red", "yellow", "blue"])


@dg.asset(partitions_def=colors_partitions_def, compute_kind="modal")
def modal_hello_world(
    context: dg.AssetExecutionContext, modal: ModalClient
) -> dg.MaterializeResult:
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


# sensor to poll for new podcasts
