from dagster import Definitions

from dagster import PipesSubprocessClient
from pathlib import Path
from typing import Mapping, Optional, Union
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    OpExecutionContext,
    StaticPartitionsDefinition,
    asset,
)
from dagster._annotations import public
from dagster._core.pipes.client import PipesClientCompletedInvocation
from dagster_pipes import PipesExtras


####################################################################################################
#                                             Resource                                             #
####################################################################################################


class ModalClient(PipesSubprocessClient):
    def __init__(
        self,
        modal_project_dir: Optional[Union[str, Path]] = None,
        env: Optional[Mapping[str, str]] = None,
    ):
        if isinstance(modal_project_dir, Path):
            modal_project_dir = str(modal_project_dir)
        super().__init__(env=env, cwd=modal_project_dir)

    # TODO - will this abstraction allow me to implement a `deploy` method, or other `modal`
    # subcommands?

    @public
    def run(
        self,
        *,
        relative_script_path: str,
        context: OpExecutionContext,
        extras: Optional[PipesExtras] = None,
        env: Optional[Mapping[str, str]] = None,
    ) -> PipesClientCompletedInvocation:
        return super().run(
            command=["modal", "run", relative_script_path],
            context=context,
            extras=extras,
            env=env,
        )


####################################################################################################
#                                              Assets                                              #
####################################################################################################

colors_partitions_def = StaticPartitionsDefinition(["red", "yellow", "blue"])


@asset(partitions_def=colors_partitions_def, compute_kind="modal")
def modal_hello_world(
    context: AssetExecutionContext, modal: ModalClient
) -> MaterializeResult:
    return modal.run(
        relative_script_path="hello_world.py",
        context=context,
    ).get_materialize_result()


####################################################################################################
#                                           Definitions                                            #
#####################################################################################################

defs = Definitions(
    assets=[modal_hello_world],
    resources={"modal": ModalClient(modal_project_dir=Path(__file__).parent.parent / "modal")},
)
