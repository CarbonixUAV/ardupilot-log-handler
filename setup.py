from setuptools import setup, find_packages

setup(
    name="ardupilot-log-handler",
    version="1.0",
    description="A Python library for processing ArduPilot logs",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/CarbonixUAV/ardupilot-log-handler",
    author="Lokesh",
    author_email="loki077@gmail.com",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "pymavlink>=2.4.0",
        "pandas>=1.3.0",
        "pyarrow>=5.0.0"
    ],
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            "aploghandler=ardupilot_log_handler.ap_log_handler:main",
        ]
    },
)
