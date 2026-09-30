"""Record once, replay anywhere: model calls answered from a file, for nothing.

``record`` makes live calls and keeps each answer under the request's key;
``replay`` answers from the file and makes no call at all; a request with no
recorded answer fails, it never falls through to a live call.
"""

from __future__ import annotations

import json

import pytest

import ubunye
from ubunye import llm
from ubunye.core.errors import LLMError
from ubunye.lineage.storage import FileSystemLineageStore

from .test_llm_port import _task


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in ("UBUNYE_LLM_MODE", "UBUNYE_LLM_STORE", "ANTHROPIC_API_KEY"):
        monkeypatch.delenv(name, raising=False)


def _port(provider, store, mode):
    return llm.port(
        "anthropic", model="c", api_key="k", base_url=provider.url, mode=mode, store=str(store)
    )


def test_record_then_replay_gives_the_same_answers_with_no_calls(provider, tmp_path):
    store = tmp_path / "replay.jsonl"
    recorded = _port(provider, store, "record").complete_many(["a", "b", "c"])
    assert len(provider.requests) == 3
    replayed = _port(provider, store, "replay").complete_many(["a", "b", "c"])
    assert len(provider.requests) == 3  # nothing more was sent
    assert [r.text for r in replayed] == [r.text for r in recorded]
    assert [r.input_tokens for r in replayed] == [r.input_tokens for r in recorded]


def test_the_store_keeps_answers_but_never_prompts(provider, tmp_path):
    store = tmp_path / "replay.jsonl"
    _port(provider, store, "record").complete("a secret prompt")
    text = store.read_text(encoding="utf-8")
    assert "a secret prompt" not in text.replace("echo: a secret prompt", "")
    (line,) = [json.loads(x) for x in text.splitlines()]
    assert line["key"].startswith("sha256:")
    assert line["response"]["text"] == "echo: a secret prompt"
    assert set(line) == {
        "key",
        "occurrence",
        "session",
        "backend",
        "model",
        "recorded_at",
        "response",
    }


def test_a_replay_miss_fails_closed_and_names_the_key(provider, tmp_path):
    store = tmp_path / "replay.jsonl"
    _port(provider, store, "record").complete("a")
    port = _port(provider, store, "replay")
    with llm.recording() as calls:
        with pytest.raises(LLMError, match="No recorded answer") as err:
            port.complete("never recorded")
    assert "UBUNYE_LLM_MODE=record" in str(err.value)
    assert len(provider.requests) == 1  # the miss did not call out
    (call,) = calls
    assert call["status"] == "error" and call["source"] == "replay"
    assert call["request_key"] in str(err.value)


def test_replay_with_no_store_at_all_fails_closed(provider, tmp_path):
    with pytest.raises(LLMError, match="No recorded answer"):
        _port(provider, tmp_path / "missing.jsonl", "replay").complete("a")
    assert provider.requests == []


def test_replay_needs_no_key(provider, tmp_path, monkeypatch):
    store = tmp_path / "replay.jsonl"
    _port(provider, store, "record").complete("a")
    monkeypatch.setenv("UBUNYE_LLM_MODE", "replay")
    port = llm.port("anthropic", model="c", base_url=provider.url, store=str(store))
    assert port.complete("a").text == "echo: a"


def test_a_changed_request_is_a_miss(provider, tmp_path):
    store = tmp_path / "replay.jsonl"
    _port(provider, store, "record").complete("a", temperature=0)
    with pytest.raises(LLMError, match="No recorded answer"):
        _port(provider, store, "replay").complete("a", temperature=0.7)


def test_recording_again_keeps_the_latest_answer(provider, tmp_path):
    store = tmp_path / "replay.jsonl"
    provider.answer({"content": [{"type": "text", "text": "first"}], "usage": {}, "model": "c"})
    _port(provider, store, "record").complete("a")
    _port(provider, store, "record").complete("a")  # echo this time
    assert _port(provider, store, "replay").complete("a").text == "echo: a"


def test_the_mode_comes_from_the_environment(provider, tmp_path, monkeypatch):
    store = tmp_path / "replay.jsonl"
    monkeypatch.setenv("UBUNYE_LLM_STORE", str(store))
    monkeypatch.setenv("UBUNYE_LLM_MODE", "record")
    llm.port("anthropic", model="c", api_key="k", base_url=provider.url).complete("a")
    assert store.exists()


def test_an_unknown_mode_is_refused(provider):
    with pytest.raises(LLMError, match="mode"):
        llm.port("anthropic", model="c", api_key="k", base_url=provider.url, mode="later")


def test_calls_say_where_the_answer_came_from(provider, tmp_path):
    store = tmp_path / "replay.jsonl"
    with llm.recording() as calls:
        _port(provider, store, "record").complete("a")
        _port(provider, store, "replay").complete("a")
        _port(provider, store, "live").complete("a")
    assert [c["source"] for c in calls] == ["record", "replay", "live"]
    assert calls[1]["attempts"] == 0


# --- through the engine: the store sits with the task --------------------------------


def test_a_task_records_next_to_itself_and_replays_offline(provider, tmp_path, monkeypatch):
    import pandas as pd

    task = _task(tmp_path, provider.url)
    lineage = tmp_path / "lineage"
    monkeypatch.setenv("UBUNYE_LLM_MODE", "record")
    ubunye.run_task(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))
    assert (task / "llm-replay.jsonl").exists()
    sent = len(provider.requests)

    monkeypatch.setenv("UBUNYE_LLM_MODE", "replay")
    ubunye.run_task(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))
    assert len(provider.requests) == sent  # the second run called nothing
    assert list(pd.read_parquet(tmp_path / "out")["label"]) == ["echo: good", "echo: bad"]

    runs = FileSystemLineageStore(str(lineage)).list_runs("uc/pkg/label")
    by_source = sorted(",".join(sorted({c["source"] for c in r.llm_calls})) for r in runs)
    assert by_source == ["record", "replay"]
    # Same data from the live and the replayed run.
    hashes = {r.outputs[0].data_hash for r in runs}
    assert len(hashes) == 1


def test_the_store_is_found_by_its_spelling_not_by_asking_the_filesystem(tmp_path, monkeypatch):
    """One file, one store, whatever the disk is doing.

    The cache was keyed by Path.resolve(), which on Windows asks the filesystem and
    can spell one folder two ways (8.3 short name or not), for example while another
    thread has the file open. One task's answers then split across two stores and
    replay missed half of them.
    """
    from pathlib import Path

    from ubunye.llm import replay

    def no_disk(self, *a, **k):
        raise AssertionError("store_for must not resolve paths")

    monkeypatch.setattr(Path, "resolve", no_disk)
    task = tmp_path / "uc" / "pkg" / "t"
    first = replay.store_for(None, str(task))
    task.mkdir(parents=True)
    assert replay.store_for(None, str(task)) is first
    assert replay.store_for(None, str(task) + "/") is first


# --- a repeated request: replay is the recorded run, call for call -----------------


def _says(provider, *texts):
    """Queue answers, as a model that answers the same prompt differently each time."""
    for text in texts:
        provider.answer(
            {
                "type": "message",
                "model": "c",
                "content": [{"type": "text", "text": text}],
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 3, "output_tokens": 1},
            }
        )


def test_repeated_prompts_replay_answer_for_answer(provider, tmp_path):
    # Found on real data: two identical reviews, a model that labelled them
    # differently, and a replay that gave both the same label.
    store = tmp_path / "replay.jsonl"
    prompts = ["same", "same", "other", "same"]
    _says(provider, "A", "B", "C", "D")
    with llm.recording():
        recorded = [r.text for r in _port(provider, store, "record").complete_many(prompts)]
    assert len(set(recorded)) == 4  # four different answers, in some order
    with llm.recording():
        replayed = [r.text for r in _port(provider, store, "replay").complete_many(prompts)]
    assert replayed == recorded
    assert len(provider.requests) == 4


def test_one_by_one_calls_replay_in_order_too(provider, tmp_path):
    store = tmp_path / "replay.jsonl"
    _says(provider, "first", "second")
    with llm.recording():
        port = _port(provider, store, "record")
        recorded = [port.complete("x").text, port.complete("x").text]
    with llm.recording():
        port = _port(provider, store, "replay")
        assert [port.complete("x").text, port.complete("x").text] == recorded == ["first", "second"]


def test_more_identical_calls_than_recorded_fails_closed(provider, tmp_path):
    store = tmp_path / "replay.jsonl"
    with llm.recording():
        _port(provider, store, "record").complete("x")
    with llm.recording():
        port = _port(provider, store, "replay")
        port.complete("x")
        with pytest.raises(LLMError, match="2 times; the recording holds 1"):
            port.complete("x")
    assert len(provider.requests) == 1


def test_recording_again_replaces_the_earlier_answers(provider, tmp_path):
    store = tmp_path / "replay.jsonl"
    _says(provider, "old-1", "old-2", "new-1")
    with llm.recording():
        _port(provider, store, "record").complete_many(["x", "x"], max_concurrency=1)
    with llm.recording():
        _port(provider, store, "record").complete("x")
    llm.replay.forget()  # read the file afresh, as another process would
    with llm.recording():
        port = _port(provider, store, "replay")
        assert port.complete("x").text == "new-1"
        with pytest.raises(LLMError, match="holds 1 answer"):
            port.complete("x")


def test_a_file_recorded_before_occurrences_still_replays(provider, tmp_path):
    store = tmp_path / "replay.jsonl"
    with llm.recording():
        _port(provider, store, "record").complete("x")
    # Rewrite the line as 0.7.1 wrote it: no occurrence, no session.
    (line,) = [json.loads(x) for x in store.read_text(encoding="utf-8").splitlines()]
    del line["occurrence"], line["session"]
    store.write_text(json.dumps(line) + "\n", encoding="utf-8")
    llm.replay.forget()
    with llm.recording():
        port = _port(provider, store, "replay")
        assert [port.complete("x").text for _ in range(3)] == ["echo: x"] * 3


def test_calls_record_their_occurrence(provider, tmp_path):
    with llm.recording() as calls:
        _port(provider, tmp_path / "r.jsonl", "record").complete_many(["x", "y", "x"])
    # Calls are logged as they finish, so compare which occurrences each prompt got.
    by_key = {}
    for c in calls:
        by_key.setdefault(c["request_key"], set()).add(c["occurrence"])
    assert sorted(by_key.values(), key=len) == [{0}, {0, 1}]
