from ubunye.core.runtime import Registry


def test_registry_loads():
    reg = Registry.from_entrypoints()
    assert isinstance(reg.readers, dict)
