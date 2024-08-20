"""
USAGE

    modal run modal/hello_world.py

"""

import sys

import modal


app = modal.App("example-hello-world")

image = modal.Image.debian_slim(python_version="3.11").pip_install("dagster-pipes")



@app.function(image=image)
def f(identifier):
    print(identifier)


@app.local_entrypoint()
def main():
    from dagster_pipes import PipesContext, open_dagster_pipes

    with open_dagster_pipes():
        context = PipesContext.get()
        context.log.info(f"Processing static partition {context.partition_key}")
        f.remote(context.partition_key)
