"""Unit tests for the ONNX model plugin.

The graph under test is built here rather than committed, so the suite has no
binary fixture to keep in step with the code. It is a two-class linear model:
small enough to reason about, and shaped like the classifiers the plugin will
actually serve.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from ubunye.plugins.ml.base import FeatureSchema
from ubunye.plugins.ml.onnx import MODEL_FILENAME, OnnxModel

onnx = pytest.importorskip("onnx", reason="onnx not installed")
pytest.importorskip("onnxruntime", reason="onnxruntime not installed")


@pytest.fixture
def linear_onnx(tmp_path: Path) -> Path:
    """A 3-feature, 2-class linear graph: logits = X @ W + b."""
    from onnx import TensorProto, helper, numpy_helper

    weight = numpy_helper.from_array(
        np.array([[1.0, -1.0], [2.0, -2.0], [0.5, -0.5]], dtype=np.float32), name="W"
    )
    bias = numpy_helper.from_array(np.array([0.0, 0.0], dtype=np.float32), name="b")

    graph = helper.make_graph(
        [
            helper.make_node("MatMul", ["features", "W"], ["xw"]),
            helper.make_node("Add", ["xw", "b"], ["logits"]),
        ],
        "linear",
        [helper.make_tensor_value_info("features", TensorProto.FLOAT, [None, 3])],
        [helper.make_tensor_value_info("logits", TensorProto.FLOAT, [None, 2])],
        [weight, bias],
    )
    model = helper.make_model(graph, opset_imports=[helper.make_opsetid("", 13)])
    model.ir_version = 9

    path = tmp_path / "linear.onnx"
    path.write_bytes(model.SerializeToString())
    return path


def test_loads_from_path_and_is_ready_to_predict(linear_onnx: Path):
    """A frozen graph needs no fit, so constructing from a path must be enough."""
    model = OnnxModel(path=linear_onnx)
    out = model.predict(np.array([[1.0, 1.0, 1.0]], dtype=np.float32))
    assert out.shape == (1, 2)
    # 1*1 + 1*2 + 1*0.5 = 3.5 against its negation.
    assert out[0, 0] == pytest.approx(3.5)
    assert out[0, 1] == pytest.approx(-3.5)


def test_predict_without_session_is_a_clear_error():
    with pytest.raises(RuntimeError, match="No ONNX session"):
        OnnxModel().predict(np.zeros((1, 3), dtype=np.float32))


def test_fit_refuses_rather_than_pretending(linear_onnx: Path):
    """Silently accepting fit would be the worst outcome: no error, no training."""
    model = OnnxModel(path=linear_onnx)
    with pytest.raises(NotImplementedError, match="frozen artifacts"):
        model.fit(np.zeros((2, 3), dtype=np.float32), np.zeros(2))


def test_proba_softmaxes_multiclass_logits(linear_onnx: Path):
    model = OnnxModel(path=linear_onnx)
    preds, probs = model.predict(np.array([[1.0, 1.0, 1.0]], dtype=np.float32), proba=True)
    assert probs.sum(axis=1) == pytest.approx(1.0)
    assert preds[0] == 0  # the positive logit wins
    assert probs[0, 0] > probs[0, 1]


def test_single_row_gets_a_batch_axis(linear_onnx: Path):
    """A 1D row is a common caller mistake and a cheap one to absorb."""
    model = OnnxModel(path=linear_onnx)
    out = model.predict(np.array([1.0, 1.0, 1.0], dtype=np.float32))
    assert out.shape == (1, 2)


def test_higher_rank_input_is_not_reshaped(tmp_path: Path):
    """Image models pass 4D tensors; guessing at their axes would corrupt them."""
    from onnx import TensorProto, helper

    graph = helper.make_graph(
        [helper.make_node("Identity", ["x"], ["y"])],
        "identity",
        [helper.make_tensor_value_info("x", TensorProto.FLOAT, [None, 2, 4, 4])],
        [helper.make_tensor_value_info("y", TensorProto.FLOAT, [None, 2, 4, 4])],
    )
    model_proto = helper.make_model(graph, opset_imports=[helper.make_opsetid("", 13)])
    model_proto.ir_version = 9
    path = tmp_path / "identity.onnx"
    path.write_bytes(model_proto.SerializeToString())

    tensor = np.random.default_rng(0).standard_normal((3, 2, 4, 4)).astype(np.float32)
    out = OnnxModel(path=path).predict(tensor)
    assert out.shape == (3, 2, 4, 4)
    np.testing.assert_allclose(out, tensor, rtol=1e-6)


def test_schema_selects_and_orders_columns(linear_onnx: Path):
    """A frame with extra columns in the wrong order must still score correctly."""
    pd = pytest.importorskip("pandas")

    model = OnnxModel(path=linear_onnx, schema=FeatureSchema(features=["a", "b", "c"]))
    frame = pd.DataFrame({"c": [1.0], "ignore_me": [99.0], "a": [1.0], "b": [1.0]})
    out = model.predict(frame)
    assert out[0, 0] == pytest.approx(3.5)


def test_save_then_load_round_trips(linear_onnx: Path, tmp_path: Path):
    original = OnnxModel(path=linear_onnx)
    target = tmp_path / "saved"
    original.save(target)

    assert (target / MODEL_FILENAME).exists()
    assert (target / "ubunye_model.json").exists()

    reloaded = OnnxModel().load(target)
    np.testing.assert_allclose(
        reloaded.predict(np.array([[1.0, 2.0, 3.0]], dtype=np.float32)),
        original.predict(np.array([[1.0, 2.0, 3.0]], dtype=np.float32)),
        rtol=1e-6,
    )


def test_from_file_makes_a_registry_ready_directory(linear_onnx: Path, tmp_path: Path):
    """Exports arrive as a loose .onnx; the registry wants a directory."""
    target = tmp_path / "model_dir"
    model = OnnxModel.from_file(linear_onnx, target)

    assert (target / MODEL_FILENAME).exists()
    assert (target / "ubunye_model.json").exists()
    assert model.predict(np.array([[1.0, 1.0, 1.0]], dtype=np.float32)).shape == (1, 2)


def test_params_expose_the_graph_signature(linear_onnx: Path):
    params = OnnxModel(path=linear_onnx).params
    assert params["input_name"] == "features"
    assert params["input_shape"] == [None, 3]
    assert params["output_names"] == ["logits"]
    assert params["providers"] == ["CPUExecutionProvider"]


def test_missing_file_names_the_path(tmp_path: Path):
    with pytest.raises(FileNotFoundError, match="nope.onnx"):
        OnnxModel(path=tmp_path / "nope.onnx")


def test_registered_in_the_ml_entry_point_group():
    """The engine dispatches on entry points, so registration is part of the feature."""
    from importlib.metadata import entry_points

    names = {ep.name for ep in entry_points(group="ubunye.ml")}
    assert "onnx" in names
