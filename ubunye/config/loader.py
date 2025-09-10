"""YAML + Jinja config loader with Pydantic validation."""
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, Optional
import os
import yaml
from jinja2 import Environment, FileSystemLoader
from .schema import TaskConfig


def load_config(path: str, variables: Optional[Dict[str, Any]] = None) -> TaskConfig:
    """Render a YAML config with Jinja and validate with Pydantic.

    Parameters
    ----------
    path: str
        Path to the YAML (or Jinja-templated YAML) file.
    variables: dict, optional
        Extra variables available to the Jinja template.
    """
    p = Path(path)
    env = Environment(loader=FileSystemLoader(str(p.parent)))
    # Provide environment vars inside Jinja as `env` and date helpers
    env.globals.update(env=os.environ)
    template = env.get_template(p.name)
    rendered = template.render(**(variables or {}))
    raw = yaml.safe_load(rendered)
    return TaskConfig.model_validate(raw)
