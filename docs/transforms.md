---
icon: material/lightning-bolt
---
# In-Broker Stream Transforms & WASM

<div class="doc-badge-row" markdown>
<span class="md-tag md-tag--primary">WASM Engine</span>
<span class="md-tag">5 min read</span>
<span class="md-tag">Stream Processing</span>
</div>

AeroStream features an **In-Broker Stream Processing & Transformation Engine**. Instead of spinning up heavy external processing clusters (such as Apache Flink or Kafka Streams) for straightforward event sanitization, routing, or PII masking, AeroStream executes transforms inline directly on the broker.

![Dual Engine Architecture](images/dual_engine_architecture.png)

---

## Supported Transform Types

AeroStream provides four transform types. Each one reads a source topic, processes every record and produces the result to a target topic:

| `type` | What it does | Config keys |
|---|---|---|
| `FILTER` | Keeps records that match a predicate on JSON fields; the rest are dropped. | `filter_expression`, e.g. `level == "CRITICAL"` |
| `MASK_PII` | Replaces the listed JSON fields (at any depth) with `***`. | `fields_to_mask` (comma-separated), `mask_pattern` |
| `JSON_MAP` | Adds, renames and removes JSON fields. | `add_fields` (`k:v,k2:v2`), `rename_fields` (`old:new`), `remove_fields`, `set_<field>` |
| `WASM` | Runs your own module in a sandbox. | `code` (base64 module), `timeout_ms` |

Every type also accepts two optional settings:

* `dlq_topic`: records whose transform fails are produced there unchanged instead of being dropped.
* `start_offset`: `earliest` (default) processes the existing backlog; `latest` handles only records produced after the transform's first pass.

---

## Automated PII Data Masking

Data privacy regulations (GDPR, PCI-DSS, HIPAA) mandate that raw sensitive credentials must never reach general analytics consumers.

With the `MASK_PII` transform, fields containing sensitive data are masked as records flow from the source to the target topic:

=== "Raw Ingress Message (`orders-raw`)"

    ```json
    {
      "order_id": "ORD-9821",
      "customer_email": "user@example.com",
      "credit_card": "4111-2222-3333-4444",
      "cvv": "782",
      "total_amount": 99.50
    }
    ```

=== "Sanitized Output Message (`orders-sanitized`)"

    ```json
    {
      "order_id": "ORD-9821",
      "customer_email": "user@example.com",
      "credit_card": "***",
      "cvv": "***",
      "total_amount": 99.50
    }
    ```

---

## Delivery Semantics

Transforms run in the control plane's transform runner, which tails each running transform's source topic:

* **At-least-once.** The runner commits a transform's offset only after the output (and any dead-letter records) were produced. If the target topic is missing or the produce fails, the same records are retried on the next pass, so a record can be transformed more than once but is never skipped.
* **Ordering and keys.** Record keys are preserved. Output goes to the target partition `source partition mod target partitions`, so per-partition ordering holds when both topics have the same partition count.
* **Real Kafka records.** Output is produced through the broker's Kafka port as ordinary record batches, so any Kafka consumer reads the target topic.
* **Start position.** By default a new transform starts at the beginning of the source topic and processes the existing backlog; with `"start_offset": "latest"` it skips the backlog. Deleting and re-creating a transform starts over from its start position.
* **Persistence.** Transform definitions and committed offsets are stored under `AEROSTREAM_TRANSFORMS_DIR` (default `<data dir>/controller/transforms` in the container image) and survive a restart without reprocessing.
* **Counters.** `GET /api/transforms` reports `messages_processed`, `messages_filtered`, `messages_failed` and `last_error` for each transform.

The source and target topics must already exist. Compressed record batches (gzip, snappy, lz4, zstd) are decoded; records that are not valid JSON make `FILTER`, `MASK_PII` and `JSON_MAP` fail, which sends them to `dlq_topic` when one is set.

---

## WASM Sandbox Execution

For custom logic, a `WASM` transform runs your module in a [wazero](https://wazero.io) sandbox:

* **Isolation.** Every record runs in a fresh instance: no state carried over, no filesystem, environment or network access. Linear memory is capped at 16 MiB.
* **Time limit.** Each call has a deadline (`timeout_ms`, default 250 ms). A guest that loops forever is killed and the record counts as failed.
* **Validation.** The module is compiled when the transform is registered, so a corrupt upload or a missing export is rejected with `400`.

The module must export `memory`, `alloc(size: i32) -> i32` and `transform(ptr: i32, len: i32) -> i64`. The host calls `alloc`, copies the record into the returned buffer and calls `transform`, which returns `(out_ptr << 32) | out_len`, or `0` to drop the record.

```rust
// Rust guest (target wasm32-unknown-unknown) that upper-cases every record.
// Built with `cargo build --release --target wasm32-unknown-unknown` (crate-type = ["cdylib"]); the compiled module is
// used as a test fixture in go-controller/pkg/transform/testdata.
use std::alloc::{alloc as heap_alloc, Layout};

#[no_mangle]
pub extern "C" fn alloc(size: i32) -> i32 {
    unsafe { heap_alloc(Layout::from_size_align(size as usize, 1).unwrap()) as i32 }
}

#[no_mangle]
pub extern "C" fn transform(ptr: i32, len: i32) -> i64 {
    let input = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    let out = input.to_ascii_uppercase().leak();
    ((out.as_ptr() as i64) << 32) | out.len() as i64
}
```

Register it with the compiled module base64-encoded in `code`:

```bash
curl -X POST http://localhost:9001/api/transforms \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"shout\",\"source_topic\":\"in\",\"target_topic\":\"out\",\"type\":\"WASM\",
       \"code\":\"$(base64 -w0 target/wasm32-unknown-unknown/release/shout.wasm)\"}"
```

---

## Transforms REST API

Transforms are managed at runtime through the Control Plane REST API (`http://localhost:9001`):

### Create a PII Masking Transform

```bash
curl -X POST http://localhost:9001/api/transforms \
  -H "Content-Type: application/json" \
  -d '{
    "name": "sanitize-orders",
    "source_topic": "orders-raw",
    "target_topic": "orders-clean",
    "type": "MASK_PII",
    "config": {
      "fields_to_mask": "credit_card,cvv,password,ssn",
      "dlq_topic": "orders-dlq"
    }
  }'
```

### Try a Transform Without Producing Anything

```bash
curl -X POST http://localhost:9001/api/transforms/test \
  -H "Content-Type: application/json" \
  -d '{"transform_name":"sanitize-orders","payload":{"order_id":"1","cvv":"782"}}'
```

### List, Pause, Resume and Delete

```bash
curl -s http://localhost:9001/api/transforms | jq
curl -X POST   http://localhost:9001/api/transforms/sanitize-orders/pause
curl -X POST   http://localhost:9001/api/transforms/sanitize-orders/resume
curl -X DELETE http://localhost:9001/api/transforms/sanitize-orders
```

---

## Stream Processing: Windowed Aggregations and Joins

Stateful jobs live under `/api/streams`. A job tails its source topic, keeps its state in a local store and can write each result to a `target_topic` as a regular Kafka record. State and consumed offsets are kept under `AEROSTREAM_STREAMS_DIR` (default `<data dir>/controller/streams`), so a restart resumes exactly where the job stopped.

```bash
# Total spend per user in 1-hour tumbling windows
curl -X POST http://localhost:9001/api/streams -H "Content-Type: application/json" -d '{
  "name": "spend", "type": "AGGREGATE", "source_topic": "orders",
  "key_field": "user", "value_field": "amt", "agg": "sum",
  "window": {"type": "tumbling", "size_ms": 3600000},
  "target_topic": "spend-by-user"
}'

curl "http://localhost:9001/api/streams/spend/state?key=ann"   # windows for one key
curl  http://localhost:9001/api/streams/spend/state            # all keys
curl -X POST http://localhost:9001/api/streams/spend/pause     # also: resume
curl -X DELETE http://localhost:9001/api/streams/spend
```

Records read from Kafka topics use the record timestamp as event time unless `timestamp_field` names a JSON field. Records that miss the key or value field are counted in `skipped`, and batches the engine cannot decode (for example an unsupported codec) in `undecodable_batches`; both appear in the job's `metrics`.
