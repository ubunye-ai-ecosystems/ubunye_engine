"""`ubunye prove` options with a fixed set of values are usage errors when wrong (F-092).

`skip --status nonsense` and `observe --cost-basis bogus` printed a raw ValueError
traceback; `observe --kind bogus` was kept silently.
"""

import pytest
from typer.testing import CliRunner

from ubunye.cli.main import app

try:  # Click before 8.2 mixes stderr into stdout unless asked not to
    runner = CliRunner(mix_stderr=False)
except TypeError:  # Click 8.2+ always keeps them apart
    runner = CliRunner()


@pytest.fixture
def record(tmp_path):
    path = tmp_path / "record.json"
    path.write_text('{"run_id": "abc", "status": "success"}', encoding="utf-8")
    return path


def _observe(record, out, *extra):
    return ["prove", "observe", "--workload", "w", "--env", "e", "--record", str(record)] + [
        "-o",
        str(out),
        *extra,
    ]


@pytest.mark.parametrize(
    "args, choices",
    [
        (["--kind", "bogus"], ["local", "cloud", "managed"]),
        (["--cost-basis", "bogus"], ["actual", "provider_estimate", "unknown"]),
    ],
)
def test_observe_rejects_an_unknown_choice(record, tmp_path, args, choices):
    result = runner.invoke(app, _observe(record, tmp_path / "ev", *args))
    assert result.exit_code == 2, result.output
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "Traceback" not in result.stderr
    for name in choices:
        assert name in result.stderr
    assert not (tmp_path / "ev").exists()


def test_skip_rejects_an_unknown_status(tmp_path):
    out = tmp_path / "ev"
    args = ["prove", "skip", "--workload", "w", "--env", "e", "--reason", "r", "-o", str(out)]
    result = runner.invoke(app, args + ["--status", "nonsense"])
    assert result.exit_code == 2, result.output
    assert isinstance(result.exception, SystemExit)
    for name in ("not_run", "unsupported", "failed_to_launch"):
        assert name in result.stderr
    assert not out.exists()


def test_skip_and_observe_still_take_the_valid_choices(record, tmp_path):
    out = tmp_path / "ev"
    args = ["prove", "skip", "--workload", "w", "--env", "e", "--reason", "r", "-o", str(out)]
    assert runner.invoke(app, args + ["--status", "unsupported"]).exit_code == 0
    result = runner.invoke(app, _observe(record, out, "--kind", "cloud", "--cost-basis", "actual"))
    assert result.exit_code == 0, result.output
