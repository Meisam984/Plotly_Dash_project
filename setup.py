# Import required libraries
from setuptools import setup, find_packages
from typing import List

HYPHEN_E_DOT = "-e ."


# function to fetch requirements from requirements.txt
def fetch_requirements(file_path: str) -> List[str]:
    with open(file=file_path, mode="r") as f:
        req_list = f.readlines()
        req_list = [req.replace("\n", "") for req in req_list]
        if HYPHEN_E_DOT in req_list:
            req_list.remove(HYPHEN_E_DOT)

    return req_list


setup(
    name="Plotly_Dash_project",
    version="0.0.1",
    description="A web app based on Streamlit to visualize components, using Plotly Dash",
    author="Meisam Rezaei",
    author_email="Meysam.or.us@gmail.com",
    url="https://github.com/Meisam984/Plotly_Dash_project.git",
    packages=find_packages(),
    install_requires=fetch_requirements("requirements.txt")
)