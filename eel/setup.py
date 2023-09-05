from setuptools import setup, find_packages

setup(
    name="eel",
    version="0.001",
    packages=find_packages(),
    install_requires=["pandas", "sqlalchemy", "openpyxl"],
)
