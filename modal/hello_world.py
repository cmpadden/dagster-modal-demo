"""
USAGE

    modal run modal/hello_world.py

"""

import modal


app = modal.App("example-hello-world")


@app.function()
def f(identifier):
    print(identifier)


@app.local_entrypoint()
def main():
    from dagster_pipes import PipesContext, open_dagster_pipes

    with open_dagster_pipes():
        context = PipesContext.get()
        context.log.info(f"Processing static partition {context.partition_key}")
        f.remote(context.partition_key)
