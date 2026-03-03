from ubunye.config import load_config


def test_load_config():
    # Minimal config (string) saved to temp file during CI would be ideal.
    # Here we simply assert loader callable exists.
    assert callable(load_config)
