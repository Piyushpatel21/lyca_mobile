from setuptools import setup, find_packages
from glob import glob


with open("README.md", "r+") as f:
    long_description = f.read()

setup(
    name="lycaetl_aggregation",
    version="1.0.0",
    author="Cloudwick Technologies UK",
    author_email="bhavin.tandel@cloudwick.com",
    description="Build lyca etl aggregation pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://code.cloudwick.com/emea/customers/lycamobile/lycamobile-etl-movements/",
    packages=find_packages(where='code/pythonlib/main/src'),
    package_dir={'': 'code/pythonlib/main/src'},
    data_files=['Pipfile',
                ('config', glob('RRBS/code/config/*.json'))],
    include_data_files=True,
    zip_safe=True,
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    project_urls={
        "Bug Tracker": "",
        "Source Code": "https://code.cloudwick.com/emea/customers/lycamobile/lycamobile-etl-movements/",
    },
    python_requires='>=2.7'
)