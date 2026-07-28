# Scoring ONNX models

`format`/`type: onnx` runs a trained ONNX graph inside a pipeline. It fills the
gap the `ubunye.ml` group had until 0.5.1: `sklearn` and `sparkml` cover
classical models, and neither can score a trained neural network over object
storage.

Install the extra:

```bash
pip install ubunye-engine[onnx]
```

It is a separate extra on purpose. `onnxruntime` is about 300 MB and only
pipelines that score a frozen graph need it.

## Inference only

An ONNX graph is a frozen artifact, so `fit()` raises:

```python
model.fit(X, y)
# NotImplementedError: ONNX graphs are frozen artifacts and cannot be fitted.
```

Train wherever you like, export once, score here. That split is the point: the
training framework never has to be installed on an executor.

## Loading

Three ways in, depending on what you have.

```python
from ubunye.plugins.ml.onnx import OnnxModel

# A loose .onnx file. Ready to predict immediately, with no fit ceremony.
model = OnnxModel(path="flood_unet.onnx")

# An Ubunye model directory (model.onnx + ubunye_model.json).
model = OnnxModel().load("s3a://models/flood/v3")

# A loose file you want to put into a model directory for the registry.
model = OnnxModel.from_file("flood_unet.onnx", "models/flood/v3")
```

## Predicting

Tabular input behaves like the sklearn plugin. With a schema, columns are
selected and ordered for you:

```python
from ubunye.plugins.ml.base import FeatureSchema

model = OnnxModel(path="risk.onnx", schema=FeatureSchema(features=["age", "ltv", "term"]))
model.predict(frame)                      # raw graph output
preds, probs = model.predict(frame, proba=True)   # softmax over the class axis
```

`proba=True` softmaxes only when the graph emits more than one column. A
single-column output is already a probability or a regression value, and
normalising it would turn every prediction into `1.0`.

## Tensors, not just tables

Arrays of any rank are forwarded unchanged, which is what makes image models
work:

```python
tiles = ...                     # (N, 2, 256, 256): a batch of SAR tiles
logits = model.predict(tiles)   # (N, 2, 256, 256)
preds, probs = model.predict(tiles, proba=True)
# preds: (N, 256, 256), the segmentation mask
# probs: (N, 2, 256, 256), per-class probability
```

The plugin never reshapes higher-rank input. Only the caller knows whether a
4096-element row is a 64x64 image or 4096 features, and a wrong guess corrupts
the batch silently. A 1D row is the one exception: it gets a batch axis, because
that is unambiguous.

## In a pipeline

Reading rasters through the `binary` reader and scoring them per partition:

```yaml
MODEL: "ml"
VERSION: "1.0.0"

CONFIG:
  inputs:
    tiles:
      format: binary
      path: "s3a://s1-archive/za/2024/"
      path_glob_filter: "*.tif"
  transform:
    type: model
    params:
      action: predict
      model_class: "model.FloodModel"
  outputs:
    water:
      format: delta
      path: "s3a://flood/water-by-tile"
      mode: append
```

Note the output is `delta`, not `binary`. Spark's `binaryFile` source is
read-only and the config schema rejects `binary` in `outputs`, so a pipeline can
read rasters but cannot write them. Emit statistics, not images: water fraction
per tile, area, a bounding box. If you need the mask raster itself, write it
from your `transformations.py` with the library of your choice.

## Threading

Each session is created with `intra_op_num_threads = 1` and
`inter_op_num_threads = 1`. An executor already runs several tasks in parallel,
and letting every session spawn its own thread pool oversubscribes the core and
makes the stage slower rather than faster.

## Providers

Defaults to `CPUExecutionProvider`, the only one present on a stock executor.
Override when the cluster has GPUs and the matching onnxruntime build:

```python
OnnxModel(path="model.onnx", providers=["CUDAExecutionProvider", "CPUExecutionProvider"])
```

Order is priority. Keep CPU last as a fallback, or a node without the GPU build
fails instead of running slowly.

## Inspecting a graph

`params` reports the signature, which is the quickest way to find out what a
model you were handed actually expects:

```python
>>> OnnxModel(path="flood_unet.onnx").params
{'providers': ['CPUExecutionProvider'],
 'output_index': 0,
 'input_name': 'sar',
 'input_shape': ['batch', 2, 256, 256],
 'output_names': ['logits']}
```
