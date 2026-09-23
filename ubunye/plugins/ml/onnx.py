"""
ONNX model wrapper implementing BaseModel.

Adds batch deep-learning inference to the ``ubunye.ml`` group, which until now
held ``sklearn`` and ``sparkml`` and therefore covered classical models only.
The gap this closes is real: scoring a trained CNN or transformer over object
storage is ordinary industrial work, and neither existing plugin can do it.

## Inference only, on purpose

An ONNX graph is a frozen artifact. It is the output of training somewhere else,
in PyTorch or TensorFlow or sklearn, and there is no meaningful way to fit one.
``fit`` therefore raises rather than pretending. A wrapper that silently accepted
``fit`` and did nothing would be worse than one that refuses.

This makes the plugin the serving half of a two-repo story: train in the
framework of your choice, export once, and score at scale here.

## Why ONNX rather than a torch plugin

Portability and weight. onnxruntime is roughly 300 MB against PyTorch's 2.5 GB,
which matters when every Spark executor installs it, and an ONNX graph runs the
same on a laptop, an executor and a Databricks node without a CUDA build
matching the driver. That is the same promise the engine makes about pipelines,
applied to models.

## Input shape

Tabular callers pass a 2D array of features, exactly as the sklearn plugin does.
Callers with tensors, such as an image model reading tiles through the ``binary``
reader, pass the full N-dimensional array and it is forwarded unchanged. The
plugin does not reshape: only the caller knows whether a 4096-element row is a
64x64 image or 4096 features.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np

from .base import BaseModel, FeatureSchema

MODEL_FILENAME = "model.onnx"


class OnnxModel(BaseModel):
    """Run a trained ONNX graph under the unified Ubunye model interface.

    Args:
        path: An ``.onnx`` file to load immediately. A model given this way is
            ready to predict without ``load()``, because a frozen graph needs no
            fitting and requiring a ceremonial ``fit`` would be theatre.
        providers: onnxruntime execution providers, in priority order. Defaults
            to CPU, which is the only provider present on a stock executor.
        output_index: Which graph output to return when the model has several.
        schema: Optional feature schema, used to select and order columns from
            tabular input.
    """

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        providers: Optional[list[str]] = None,
        output_index: int = 0,
        schema: Optional[FeatureSchema] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(schema=schema, **kwargs)
        self._providers = providers or ["CPUExecutionProvider"]
        self._output_index = output_index
        self._session: Any = None
        self._input_name: Optional[str] = None
        self._model_bytes: Optional[bytes] = None

        if path is not None:
            self._init_session(Path(path))
            self._is_fitted = True

    # ---- session ----
    def _init_session(self, path: Path) -> None:
        try:
            import onnxruntime as ort
        except ImportError as exc:  # pragma: no cover - exercised by the extra
            raise ImportError(
                "OnnxModel needs onnxruntime. Install it with "
                "'pip install ubunye-engine[onnx]' or 'pip install onnxruntime'."
            ) from exc

        if not path.exists():
            raise FileNotFoundError(f"No ONNX model at {path}")

        self._model_bytes = path.read_bytes()

        # One thread per session. A Spark executor already runs several tasks in
        # parallel, and letting each session spawn its own thread pool oversubscribes
        # the core and makes the stage slower, not faster.
        options = ort.SessionOptions()
        options.intra_op_num_threads = 1
        options.inter_op_num_threads = 1

        self._session = ort.InferenceSession(
            str(path), sess_options=options, providers=self._providers
        )
        self._input_name = self._session.get_inputs()[0].name

    def _assert_session(self) -> None:
        if self._session is None:
            raise RuntimeError(
                "No ONNX session. Construct with OnnxModel(path=...) or call load(dir)."
            )

    def _assert_fitted(self) -> None:
        """Report the actual problem instead of the inherited one.

        ``BaseModel`` answers an unfitted predict with "call fit() before
        predict()". For this plugin that advice is wrong twice over: fitting is
        impossible, and following it raises ``NotImplementedError``. What the
        caller is missing is a loaded graph, so say that.
        """
        self._assert_session()

    # ---- core hooks ----
    def _fit_core(self, X: Any, y: Optional[Any]) -> None:
        raise NotImplementedError(
            "ONNX graphs are frozen artifacts and cannot be fitted. Train in PyTorch, "
            "TensorFlow or sklearn, export to ONNX, then load it here for scoring."
        )

    def _predict_core(self, X: Any, proba: bool = False) -> Any:
        self._assert_session()

        arr = self._to_array(X)
        outputs = self._session.run(None, {self._input_name: arr})
        out = np.asarray(outputs[self._output_index])

        if not proba:
            return out

        # Softmax over the class axis when the graph emits logits. A single
        # output column is already a probability or a regression value, and
        # normalising it would turn every prediction into 1.0.
        if out.ndim >= 2 and out.shape[1] > 1:
            shifted = out - out.max(axis=1, keepdims=True)
            exp = np.exp(shifted)
            probs = exp / exp.sum(axis=1, keepdims=True)
            preds = probs.argmax(axis=1)
            return preds, probs
        return out, out

    def _to_array(self, X: Any) -> np.ndarray:
        """Coerce input to the float32 array onnxruntime expects.

        Tabular frames are reduced to their feature columns when a schema says
        which those are. Arrays of any rank pass through untouched, because the
        caller is the only one who knows what the extra dimensions mean.
        """
        if hasattr(X, "to_numpy"):
            feats = self.schema.features if self.schema else None
            X = X[list(feats)].to_numpy() if feats else X.to_numpy()

        arr = np.ascontiguousarray(np.asarray(X, dtype=np.float32))
        # A single tabular row arrives 1D; the graph expects a batch axis.
        # Higher-rank input is left alone: adding an axis to a 3D image tensor
        # would guess at whether it is (C,H,W) or a batch of 2D frames.
        if arr.ndim == 1:
            arr = arr.reshape(1, -1)
        return arr

    def _save_core(self, path: Path) -> None:
        self._assert_session()
        if self._model_bytes is None:  # pragma: no cover - set by every load path
            raise RuntimeError("No model bytes to save.")
        (path / MODEL_FILENAME).write_bytes(self._model_bytes)

    def _load_core(self, path: Path) -> None:
        self._init_session(Path(path) / MODEL_FILENAME)

    # ---- introspection ----
    @property
    def params(self) -> Dict[str, Any]:
        base = super().params
        base.update({"providers": list(self._providers), "output_index": self._output_index})
        if self._session is not None:
            base["input_name"] = self._input_name
            base["input_shape"] = list(self._session.get_inputs()[0].shape)
            base["output_names"] = [o.name for o in self._session.get_outputs()]
        return base

    @staticmethod
    def from_file(src: str | Path, dest_dir: str | Path) -> "OnnxModel":
        """Copy a loose ``.onnx`` file into an Ubunye model directory and load it.

        The registry expects a directory holding ``ubunye_model.json`` alongside
        the artifact. Exports arrive as a bare file, so this does the one step
        between the two rather than making every caller remember it.
        """
        dest_dir = Path(dest_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(Path(src), dest_dir / MODEL_FILENAME)

        model = OnnxModel()
        model._save_meta(dest_dir)
        model._load_core(dest_dir)
        model._is_fitted = True
        return model
