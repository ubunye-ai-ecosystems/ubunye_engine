"""Typed config schema using Pydantic v2."""
from __future__ import annotations
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field


class EngineProfile(BaseModel):
    spark_conf: Dict[str, str] = Field(default_factory=dict)


class EngineConfig(BaseModel):
    spark_conf: Dict[str, str] = Field(default_factory=dict)
    profiles: Dict[str, EngineProfile] = Field(default_factory=dict)


class IOConfig(BaseModel):
    format: str
    db_name: Optional[str] = None
    tbl_name: Optional[str] = None
    sql: Optional[str] = None
    path: Optional[str] = None
    mode: Optional[str] = None
    options: Dict[str, Any] = Field(default_factory=dict)


class TransformConfig(BaseModel):
    type: str = "noop"
    params: Dict[str, Any] = Field(default_factory=dict)


class TaskConfig(BaseModel):
    MODEL: str
    VERSION: str
    ENGINE: EngineConfig = EngineConfig()
    CONFIG: Dict[str, Any]

    def merged_spark_conf(self, profile: str | None = None) -> Dict[str, str]:
        conf = dict(self.ENGINE.spark_conf)
        if profile and profile in self.ENGINE.profiles:
            conf.update(self.ENGINE.profiles[profile].spark_conf)
        return conf
