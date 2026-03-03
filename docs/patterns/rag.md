# Pattern: RAG Document Pipeline

A Retrieval-Augmented Generation (RAG) pipeline uses Ubunye to ingest, clean,
chunk, embed, and store documents so an LLM can retrieve relevant context at query time.

---

## Architecture

```
REST API / S3 (raw docs)
    → Ubunye ETL task (clean + chunk)
    → Delta Lake (chunks table)
    → Embedding task (chunk → vector)
    → Vector store (Pinecone, Weaviate, pgvector, …)
```

---

## Step 1 — Ingest raw documents

Fetch documents from a REST API (or S3, JDBC):

```yaml
# pipelines/rag/ingest/documents/config.yaml
MODEL: etl
VERSION: "1.0.0"

CONFIG:
  inputs:
    raw_docs:
      format: rest_api
      url: "https://content-api.example.com/v1/articles"
      auth:
        type: bearer
        token: "{{ env.CONTENT_API_TOKEN }}"
      pagination:
        type: next_link
        link_field: next
      response:
        root_key: articles

  transform:
    type: task

  outputs:
    raw_documents:
      format: delta
      path: s3://datalake/rag/raw_documents/
      mode: append
```

`transformations.py` — clean HTML, extract text:

```python
from ubunye.core.interfaces import Task

class IngestTask(Task):
    def transform(self, sources: dict) -> dict:
        df = sources["raw_docs"]
        # Strip HTML tags, normalise whitespace, filter short docs
        clean = (
            df.filter("LENGTH(body) > 200")
              .withColumn("text", _strip_html_udf("body"))
              .select("id", "title", "text", "published_at", "url")
        )
        return {"raw_documents": clean}
```

---

## Step 2 — Chunk documents

```yaml
# pipelines/rag/chunk/documents/config.yaml
MODEL: etl
VERSION: "1.0.0"

CONFIG:
  inputs:
    raw_documents:
      format: delta
      path: s3://datalake/rag/raw_documents/

  transform:
    type: task

  outputs:
    chunks:
      format: delta
      path: s3://datalake/rag/chunks/
      mode: overwrite
```

`transformations.py` — sliding-window chunker:

```python
from ubunye.core.interfaces import Task

CHUNK_SIZE = 512    # tokens (approximate characters * 0.75)
OVERLAP    = 64

class ChunkTask(Task):
    def transform(self, sources: dict) -> dict:
        df = sources["raw_documents"]
        chunks_rdd = df.rdd.flatMap(_chunk_record)
        schema = "doc_id string, chunk_index int, text string"
        return {"chunks": df.sparkSession.createDataFrame(chunks_rdd, schema)}

def _chunk_record(row):
    words = row["text"].split()
    step  = CHUNK_SIZE - OVERLAP
    for i, start in enumerate(range(0, len(words), step)):
        chunk = " ".join(words[start : start + CHUNK_SIZE])
        yield (row["id"], i, chunk)
```

---

## Step 3 — Embed and index

Embed each chunk and upsert into your vector store.
Use a `UbunyeModel` for library-independent embedding:

```python
# model.py
from ubunye.models.base import UbunyeModel

class EmbeddingModel(UbunyeModel):
    def train(self, df):
        return {}   # embedding models are pre-trained; nothing to train

    def predict(self, df):
        import openai, json
        rows = df.toPandas()
        rows["embedding"] = rows["text"].apply(
            lambda t: openai.embeddings.create(
                model="text-embedding-3-small", input=t
            ).data[0].embedding
        )
        return df.sparkSession.createDataFrame(rows)

    def save(self, path): ...
    @classmethod
    def load(cls, path): return cls()
    def metadata(self): return {"library": "openai", "library_version": "1.x", "features": ["text"], "params": {}}
```

```yaml
# pipelines/rag/embed/chunks/config.yaml
MODEL: ml
VERSION: "1.0.0"

CONFIG:
  inputs:
    chunks:
      format: delta
      path: s3://datalake/rag/chunks/

  transform:
    type: model
    params:
      action: predict
      model_class: "model.EmbeddingModel"
      model_path: ".ubunye/model_store/rag/EmbeddingModel/versions/1.0.0/model"

  outputs:
    embedded_chunks:
      format: delta
      path: s3://datalake/rag/embedded_chunks/
      mode: overwrite
```

---

## Lineage tracking

Enable lineage to trace which documents fed which embeddings:

```bash
ubunye run -d pipelines -u rag -p ingest -t documents --lineage
ubunye run -d pipelines -u rag -p chunk  -t documents --lineage
ubunye run -d pipelines -u rag -p embed  -t chunks    --lineage
```

```bash
ubunye lineage list
ubunye lineage trace --run-id <run_id>
```

---

## Orchestration — Airflow DAG

```yaml
ORCHESTRATION:
  type: airflow
  schedule: "0 3 * * *"    # nightly refresh
  retries: 2
  tags: [rag, nightly]
```

```bash
ubunye export airflow -c pipelines/rag/ingest/documents/config.yaml -o dags/rag_ingest.py
ubunye export airflow -c pipelines/rag/chunk/documents/config.yaml  -o dags/rag_chunk.py
ubunye export airflow -c pipelines/rag/embed/chunks/config.yaml     -o dags/rag_embed.py
```
