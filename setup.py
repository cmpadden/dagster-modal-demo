from setuptools import find_packages, setup

setup(
    name="dagster_modal_demo",
    packages=find_packages(exclude=["dagster_modal_demo_tests"]),
    install_requires=[
        "dagster",
        "dagster-aws",
        "dagster-cloud",
        "feedparser",
        "openai",
        "tiktoken",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "ruff"]},
)
