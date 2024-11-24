from setuptools import find_packages, setup

setup(
    name="fab_data",
    packages=find_packages(exclude=["fab_data_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
