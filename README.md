

<p align="center">
  <img src="docs/assets/ubunye-logo-white-v2.png?" alt="tfilterspy logo"/>
</p>

# Ubunye Engine
---

**Ubunye Engine** is a **Spark-based framework** for running **machine learning and ETL pipelines** at scale.

* You set up your **inputs and outputs** in a simple YAML file.
* You write your **data transformations** in `transformations.py`.
* You can run it **locally, on-prem (YARN/K8s), or on cloud(Databricks, aws)** without worrying about setup details.

Because everything is **config-driven**, data scientists can easily use the **same code in production and in notebooks**, making development and deployment seamless.

---


## Install (dev)
```bash
pip install -e .
```

## Quickstart
```bash
# Scaffold
ubunye init -d ./pipelines -u fraud_detection -p ingestion -t claim_etl

# Run
ubunye run -d ./pipelines -u fraud_detection -p ingestion -t claim_etl --profile dev
```

See `DEV_README.md` for development details.
