from setuptools import setup, find_packages

# Read long description from README.md
with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="ubunye-engine",
    version="0.1.0",
    author="Ubunye AI Team",
    author_email=["uaie@gmail.com"],
    maintainer=['Thabang Mashinini-Sekgoto'],
    maintainer_email=['thaangline@gmail.com'],
    description="Config-first, Spark-native ETL + ML framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ubunye-ai-ecosystems/ubunye-engine",
    packages=find_packages(exclude=("tests", "docs", "examples")),
    python_requires=">=3.9",
    install_requires=[
        "pydantic>=1.10,<3",
        "pyyaml>=6.0",
        "typer>=0.9.0",
    ],
    extras_require={
        "dev": ["pytest", "black", "ruff"],
        "spark": ["pyspark>=3.3,<4"],
        "ml": ["scikit-learn", "torch", "mlflow"],
    },
    entry_points={
        "console_scripts": [
            "ubunye=ubunye.cli.main:app",
        ],
        "ubunye.readers": [
            "hive=ubunye.plugins.readers.hive:HiveReader",
            "jdbc=ubunye.plugins.readers.jdbc:JdbcReader",
            "unity=ubunye.plugins.readers.unity:UnityTableReader",
        ],
        "ubunye.writers": [
            "s3=ubunye.plugins.writers.s3:S3Writer",
            "jdbc=ubunye.plugins.writers.jdbc:JdbcWriter",
            "unity=ubunye.plugins.writers.unity:UnityTableWriter",
        ],
        "ubunye.transforms": [
            "noop=ubunye.plugins.transforms.noop:NoOpTransform",
            # ML transforms can go here (train_sklearn, score_torch, etc.)
        ],
        "ubunye.ml": [
            "sklearn=ubunye.plugins.ml.sklearn:SklearnModel",
            "sparkml=ubunye.plugins.ml.pysparkml:SparkMLModel",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    include_package_data=True,
    zip_safe=False,
)
