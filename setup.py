from setuptools import setup, find_packages

setup(
    name="cyberdelta",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "aiohttp>=3.9.0",
        "websockets>=12.0",
        "numpy>=1.26.0",
        "PyYAML>=6.0",
        "python-dotenv>=1.0.0",
    ],
)
