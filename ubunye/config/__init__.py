from ubunye.core.errors import ConfigFieldError, ConfigTemplateError

from .loader import load_config
from .schema import UbunyeConfig

# Backward-compatible alias — code that previously imported TaskConfig as the
# top-level model will continue to work.
TaskConfig = UbunyeConfig

__all__ = [
    "ConfigFieldError",
    "ConfigTemplateError",
    "load_config",
    "UbunyeConfig",
    "TaskConfig",
]
