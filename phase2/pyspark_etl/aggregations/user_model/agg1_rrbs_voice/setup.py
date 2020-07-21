from setuptools import setup, find_packages

from glob import glob

setup(
    name="lycaetl_agg1_rrbs_voice",
    version="1.0.0",
    author="Cloudwick Technologies UK",
    author_email="bhavin.tandel@cloudwick.com",
    description="Aggregation level 1 of RRBS voice",
    url="https://code.cloudwick.com/emea/customers/lycamobile/lycamobile-etl-movements",
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=2.7'
)
