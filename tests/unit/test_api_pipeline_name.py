"""run_pipeline / job_health / the MCP health tool take pipeline=; package= is deprecated."""

import warnings

import pytest

import ubunye
from ubunye import api


class _Stop(Exception):
    pass


@pytest.fixture
def loaded(monkeypatch):
    """Paths run_pipeline loads configs from; stops before any backend starts."""
    seen = []

    def fake_load_config(path, **_):
        seen.append(path)
        raise _Stop

    monkeypatch.setattr(api, "load_config", fake_load_config)
    return seen


def _ends_with(path, *parts):
    return path.replace("\\", "/").endswith("/".join(parts))


def test_run_pipeline_takes_pipeline(tmp_path, loaded):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(_Stop):
            ubunye.run_pipeline(str(tmp_path), "uc", pipeline="pl", tasks=["t"])
    assert _ends_with(loaded[0], "uc", "pl", "t")


def test_run_pipeline_positional_order_is_kept(tmp_path, loaded):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(_Stop):
            ubunye.run_pipeline(str(tmp_path), "uc", "pl", ["t"])
    assert _ends_with(loaded[0], "uc", "pl", "t")


def test_run_pipeline_package_still_works_with_a_warning(tmp_path, loaded):
    with pytest.warns(DeprecationWarning, match="renamed pipeline="):
        with pytest.raises(_Stop):
            ubunye.run_pipeline(str(tmp_path), "uc", package="pl", tasks=["t"])
    assert _ends_with(loaded[0], "uc", "pl", "t")


def test_run_pipeline_both_names_is_a_type_error(tmp_path, loaded):
    with pytest.raises(TypeError, match="both pipeline= and package="):
        ubunye.run_pipeline(str(tmp_path), "uc", pipeline="a", package="b", tasks=["t"])
    assert loaded == []


def test_run_pipeline_needs_a_pipeline_and_tasks(tmp_path, loaded):
    with pytest.raises(TypeError, match="'pipeline'"):
        ubunye.run_pipeline(str(tmp_path), "uc", tasks=["t"])
    with pytest.raises(TypeError, match="'tasks'"):
        ubunye.run_pipeline(str(tmp_path), "uc", "pl")


@pytest.fixture
def narrowed(monkeypatch):
    from ubunye.lineage import health

    seen = []

    def fake_tasks_for(store, usecase_dir, usecase, pipeline):
        seen.append(pipeline)
        return []

    monkeypatch.setattr(health, "tasks_for", fake_tasks_for)
    return seen


def test_job_health_takes_pipeline(tmp_path, narrowed):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(ValueError, match="Say which tasks"):
            api.job_health(tmp_path, usecase="uc", pipeline="pl")
    assert narrowed == ["pl"]


def test_job_health_package_still_works_with_a_warning(tmp_path, narrowed):
    with pytest.warns(DeprecationWarning):
        with pytest.raises(ValueError, match="Say which tasks"):
            api.job_health(tmp_path, usecase="uc", package="pl")
    assert narrowed == ["pl"]


def test_job_health_both_names_is_a_type_error(tmp_path, narrowed):
    with pytest.raises(TypeError):
        api.job_health(tmp_path, usecase="uc", pipeline="a", package="b")


def test_mcp_health_tool_takes_pipeline_and_old_package(tmp_path, narrowed):
    from ubunye.mcp_server import _Tools as Tools

    tools = Tools(tmp_path)
    for kwargs in ({"pipeline": "pl"}, {"package": "pl"}):
        with pytest.raises(ValueError, match="say which tasks"):
            tools.health(usecase="uc", **kwargs)
    assert narrowed == ["pl", "pl"]
    with pytest.raises(ValueError, match="pipeline only"):
        tools.health(usecase="uc", pipeline="a", package="b")
