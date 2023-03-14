from setuptools import setup, find_packages

setup(
    name="myproject",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "numpy",
        "pandas"
    ],
    entry_points={
        "console_scripts": [
            "myproject=myproject.main:main"
        ]
    },
     # include the following line to generate a wheel distribution
    setup_requires=['wheel'],
)
