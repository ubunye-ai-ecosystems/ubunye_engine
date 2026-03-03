from .loader import load_config as load_config
from .schema import UbunyeConfig

# Backward-compatible alias — code that previously imported TaskConfig as the
# top-level model will continue to work.
TaskConfig = UbunyeConfig
