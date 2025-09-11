# Ubunye Engine

<p align="center">
  <img src="branding/logo/tfilters-logo.jpeg?" alt="tfilterspy logo"/>
</p>

Ubunye Engine is a **Spark-native, config-first ETL/ML framework** with a modular plugin system.
It lets you define **inputs/outputs** in YAML, write **Spark transformations** in `feature_class.py`,
and run **locally, on-prem (YARN/K8s), or on Databricks**.

## Install (dev)
```bash
pip install -e .
```

## Quickstart
```bash
# Scaffold
ubunye init -u fraud_detection -p claims -t claim_etl

# Run
ubunye run -u fraud_detection -p claims -t claim_etl --profile dev
```

See `DEV_README.md` for development details.
