from setuptools import setup, find_packages
from pipenv.project import Project
from pipenv.utils import convert_deps_to_pip


pr = Project(chdir=False)
pFile = pr.parsed_pipfile
requirements = convert_deps_to_pip(pFile['packages'], r=False)


with open("README.md", "r+") as f:
    long_description = f.read()

setup(
    name="lycaetl",
    author="Cloudwick Technologies UK",
    author_email="bhavin.tandel@cloudwick.com",
    description="Build lyca etl pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://code.cloudwick.com/emea/customers/lycamobile/lycamobile-etl-movements",
    packages=find_packages(exclude=["docs", "tests", ".gitignore", "README.md", "Makefile"]),
    data_files=['Pipfile'],
    include_data_files=True,
    zip_safe=True,
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    project_urls={
        "Bug Tracker": "",
        "Source Code": "https://code.cloudwick.com/emea/customers/lycamobile/lycamobile-etl-movements",
    },
    python_requires='>=2.7'
)
