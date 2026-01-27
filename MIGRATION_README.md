# 🌍 Ubunye Engine - Migration Guide

Welcome to **Ubunye Engine**, a modular, Spark-first ETL and ML framework evolving from the legacy **Analytics Engine**. This guide outlines a low-risk migration path, preserving public touchpoints while modernizing the architecture with plugins, Pydantic schemas, and orchestration support.

## Migration Principles

- **Stable Touchpoints**: Retain config + `transformations.py` shapes during transition.
- **Implementation Shift**: Move Readers/Writers/Transforms to plugins; centralize task lifecycle in `core/`.
- **Compatibility Shim**: Use `ubunye.compat.analytics_engine_shim` to run existing projects during migration.

## 1) Top-Level Repo Layout
