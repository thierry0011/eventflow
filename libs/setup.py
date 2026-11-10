"""
Packaging the shared kernel.

Installed with `pip install -e /libs` inside each service image. In production
this becomes a versioned wheel on a private index so services can upgrade
independently — the whole point being that `eventflow-common==1.3.0` in orders
and `1.4.0` in inventory must both work. That is why the envelope only ever
gains OPTIONAL fields.
"""

from setuptools import find_packages, setup

setup(
    name="eventflow-common",
    version="1.0.0",
    packages=find_packages(include=["eventflow_common", "eventflow_common.*"]),
    python_requires=">=3.11",
    install_requires=[],  # deliberately none: each service pins its own brokers
)
