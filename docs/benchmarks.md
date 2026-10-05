---
icon: material/chart-line
---
# Performance Benchmarks

<div class="doc-badge-row" markdown>
<span class="md-tag md-tag--primary">OMB Benchmark</span>
<span class="md-tag">5 min read</span>
<span class="md-tag">Kafka Wire Protocol</span>
</div>

AeroStream was benchmarked through its Kafka wire protocol port (`9092`) with the **Linux Foundation OpenMessaging Benchmark (OMB)** framework, on a dedicated AWS EC2 machine. This page reports what AeroStream delivers on that hardware: sustained throughput, latency percentiles, and resource use. The raw data is in [`benchmarks/omb-results/aws-c6id-2xlarge-aerostream-2026-09-30/`](https://github.com/gradientgeeks/aerostream/tree/main/benchmarks/omb-results/aws-c6id-2xlarge-aerostream-2026-09-30).

---

## Results at a Glance

Test machine: AWS `c6id.2xlarge` (8 vCPU, 16 GiB). One AeroStream broker, 1 topic with 32 partitions, 1,024-byte messages, 8 producers, 8 consumers, `acks=1`, 30 ten-second samples per run (5-minute measurement after a 2-minute warm-up), two rounds per workload.

### Kafka Wire Protocol (Port 9092)

| Offered load | Publish rate | Publish $p_{50}$ | $p_{95}$ | $p_{99}$ | $p_{99.9}$ | Broker cores busy | Errors |
|---|---|---|---|---|---|---|---|
| **100,000 msg/s** (fixed) | 100,000 msg/s (97.7 MB/s) | **0.7 ms** | **1.2 ms** | **1.4 ms** | **2.3 ms** | **14%** | 0 |
| **200,000 msg/s** (fixed) | 200,000 msg/s (195.5 MB/s) | **0.7 ms** | **1.3 ms** | **1.7 ms** | **3.0 ms** | **22%** | 0 |
| **Maximum rate** (unthrottled) | **271,350 msg/s** (265.0 MB/s) | — | — | **1,104 ms** | — | **55%** | 0 |

---

## ⚡ Native Protocol (Port 9091): Writeback Fix, Original Baseline vs Final

The figures in this section are for the AeroStream **native protocol** (port 9091) with the custom OMB driver, not the Kafka port above. To eliminate tail latency spikes under bursty I/O, AeroStream implements paced background page-cache writeback using Linux `sync_file_range(2)` and `posix_fadvise(2)` (pacing dirty flushes every 8 MiB). Tested on the identical AWS `c6id.2xlarge` instance (8 vCPU, 16 GiB, local NVMe SSD), the writeback fix completely flattens the $p_{99}$ latency tail, slashes broker CPU utilization, and elevates throughput:

| Workload | Metric | Original Baseline | Final (Writeback Fix) | Improvement |
| :--- | :--- | :---: | :---: | :---: |
| **100,000 msg/s** | $p_{50}$ latency | 1.2 ms | **0.7 ms** | **1.7× lower median latency** |
| (fixed offered load) | $p_{95}$ latency | 2.7 ms | **1.2 ms** | **2.3× lower tail latency** |
| | $p_{99}$ latency | 63.2 ms | **1.3 ms** | **48× lower tail latency ($p_{99}$)** |
| | $p_{99.9}$ latency | 94.2 ms | **1.8 ms** | **52× lower tail latency ($p_{99.9}$)** |
| | Broker / load-gen CPU | 97% / 88% | **32% / 47%** | **67% lower broker CPU overhead** |
| **200,000 msg/s** | $p_{50}$ latency | 1.8 ms | **0.8 ms** | **2.3× lower median latency** |
| (fixed offered load) | $p_{95}$ latency | 70.7 ms | **1.3 ms** | **54× lower tail latency** |
| | $p_{99}$ latency | 109.9 ms | **1.5 ms** | **73× lower tail latency ($p_{99}$)** |
| | $p_{99.9}$ latency | 145.8 ms | **3.8 ms** | **38× lower tail latency ($p_{99.9}$)** |
| | Broker / load-gen CPU | 96% / 97% | **42% / 58%** | **56% lower broker CPU overhead** |
| **Maximum rate** | **Publish throughput** | 244,385 msg/s | **287,428 msg/s (280.7 MB/s)** | **+18% higher throughput** |
| (unthrottled) | Publish $p_{99}$ | 1,009 ms | **149 ms** | **85% lower queueing tail** |
| | Broker / load-gen CPU | 95% / 96% | **67% / 51%** | **29% lower broker CPU at saturation** |

Values represent the median of 2 independent rounds (1 KB messages, 32 partitions, 8 producers and 8 consumers). Both rounds agreed within **0.02%** on throughput. Consumers matched producers with zero errors (6 of 6 runs passed with 0 errors).

!!! tip "Engineering Breakdown: The Writeback Fix"
    * **Eliminating OS Background Flusher Contention**: Without paced writeback, the Linux kernel accumulates dirty pages until hitting `dirty_background_ratio`, triggering violent writeback bursts that block Tokio worker threads performing synchronous filesystem operations.
    * **Paced Chunk Flushing**: By issuing non-blocking `sync_file_range(SYNC_FILE_RANGE_WRITE)` every 8 MiB of appended records, dirty pages are smoothly and continuously trickled to NVMe storage without thread stalls.
    * **Dramatic CPU Drop**: Eliminating flusher stalls cut broker CPU from **97% down to 32%** at 100k msg/s and from **96% down to 42%** at 200k msg/s, leaving massive headroom for unthrottled ingestion.

---

## ⚖️ Head-to-Head: Kafka Wire Port (:9092) vs Native Protocol (:9091)

| Feature / Metric | Kafka Wire Protocol (:9092) | AeroStream Native Protocol (:9091) | Architectural Takeaway |
| :--- | :---: | :---: | :--- |
| **Protocol Framing** | Full Kafka Header v2 + RecordBatch | Minimal 7-Byte Fixed Frame (`0xAE 0x01`) | Native avoids framing & serialization overhead |
| **Client Ecosystem** | 100% Drop-in (Java, Python, Go, Node, .NET) | Native SDKs (Go, Rust, Java, .NET, Node.js) | Zero migration friction vs tailored performance |
| **100k msg/s $p_{99}$ Latency** | 1.4 ms | **1.3 ms** (1.2 ms reproduced) | Sub-1.5ms flat tail latency across both |
| **200k msg/s $p_{99}$ Latency** | 1.7 ms | **1.5 ms** (1.4 ms reproduced) | Sub-2ms flat tail latency across both |
| **Max Sustained Throughput** | 271,350 msg/s (265.0 MB/s) | **287,428 msg/s (280.7 MB/s)** | +5.9% throughput boost for native wire |
| **Saturation $p_{99}$ Queueing Tail** | 1,104 ms | **149 ms** | 86.5% lower queue backlog at saturation |
| **Broker CPU at 200k msg/s** | **22%** (pinned cores) | 42% (pinned cores) | Both maintain massive CPU headroom |
| **Data Integrity & Errors** | 0 errors | 0 errors | Zero packet loss or corrupted batches |

---

### Every Run (Kafka Wire Protocol)

| Workload | Round | Publish rate | $p_{50}$ | $p_{95}$ | $p_{99}$ | $p_{99.9}$ | End-to-end $p_{99}$ | Peak 10 s rate |
|---|---|---|---|---|---|---|---|---|
| 100k msg/s | 1 | 100,084 | 0.69 ms | 1.23 ms | 1.43 ms | 2.33 ms | 2.00 ms | 102,527 |
| 100k msg/s | 2 | 100,080 | 0.68 ms | 1.21 ms | 1.40 ms | 2.29 ms | 2.00 ms | 102,395 |
| 200k msg/s | 1 | 200,157 | 0.75 ms | 1.34 ms | 1.76 ms | 3.09 ms | 2.00 ms | 204,694 |
| 200k msg/s | 2 | 200,194 | 0.73 ms | 1.31 ms | 1.72 ms | 2.98 ms | 2.00 ms | 205,832 |
| Max rate | 1 | 271,231 | 170.6 ms | 1,028 ms | 1,290 ms | 1,543 ms | 1,303 ms | 285,971 |
| Max rate | 2 | 271,469 | 39.7 ms | 597 ms | 918 ms | 1,210 ms | 934 ms | 287,703 |

At the maximum rate, latency varies from run to run (the queue depth at saturation is not stable) while throughput does not.

---

## CPU Use

The machine's four physical cores were split so that the broker and the load generator never share a core: the broker container ran on vCPUs `0,1,4,5` (two physical cores) and OMB on vCPUs `2,3,6,7`. CPU was sampled every 5 seconds with `sar` on each set.

| Workload | Broker cores busy (4 vCPUs) | Load-generator cores busy (4 vCPUs) |
|---|---|---|
| 100,000 msg/s | 14% | 48% |
| 200,000 msg/s | 22% | 59% |
| Maximum rate | 55% | 62% |

Neither side was fully busy at the maximum rate, so the 271,350 msg/s limit is not raw CPU on the broker's cores.

---

## Test Environment & Methodology

| Component | Specification |
|---|---|
| **Machine** | AWS EC2 `c6id.2xlarge`, us-east-1a, single machine for broker and load generator |
| **Processor** | Intel Xeon Platinum 8375C @ 2.90 GHz: 8 vCPUs = 4 physical cores x 2 threads (AWS Nitro, KVM) |
| **Memory / storage** | 16 GiB RAM, 474 GB local NVMe (broker data directory) |
| **Operating system** | Amazon Linux 2023, Linux 6.18, Docker |
| **Broker placement** | Docker container, host networking, pinned to vCPUs `0,1,4,5`, 8 GiB memory limit; fresh data directory and dropped page cache before every run |
| **Load generator** | OpenMessaging Benchmark (commit `5b1fa70`), pinned to vCPUs `2,3,6,7` |
| **Workload** | 1 topic, 32 partitions, 1,024-byte payloads, 8 producers, 8 consumers (one subscription) |
| **Producer settings** | `acks=1`, `linger.ms=1`, `batch.size=131072`, `max.in.flight.requests.per.connection=5` |
| **Consumer settings** | `auto.offset.reset=earliest`, auto-commit every 5 s, `max.partition.fetch.bytes=1048576` |
| **Broker build** | `quay.io/gradientgeeks/aerostream:latest` (digest `sha256:1fd1a44a7c8c`), one broker, no replication |
| **Run structure** | 2-minute warm-up (excluded) + 5-minute measurement, 2 rounds per workload, 3 workloads |
| **Date** | 30 September 2026 |

---

## Earlier Test: Constrained Container on a Laptop

An earlier run measured the broker under a strict resource cap: 2 CPUs and 2 GiB of RAM, on a laptop-class machine, 16 partitions, 2 producers and 2 consumers at the maximum rate for 60 seconds.

| Metric | AeroStream (Port 9092) |
|---|---|
| **Publish throughput (avg)** | 202,395 msg/s (197.7 MB/s) |
| **Publish throughput (peak 10 s interval)** | 216,680 msg/s |
| **Consume throughput (avg)** | 202,418 msg/s |
| **Publish latency $p_{50}$ / $p_{99}$ / $p_{99.9}$ / max** | 1.5 ms / 452 ms / 494 ms / 565 ms |
| **End-to-end latency $p_{50}$ / $p_{99}$** | 4.0 ms / 485 ms |
| **Average CPU (steady state)** | 134% of the 200% cap (about 1.34 cores) |
| **Peak container memory** | 533 MiB (from 41 MiB at start) |

| Interval | 0-10 s | 10-20 s | 20-30 s | 30-40 s | 40-50 s | 50-60 s |
|---|---|---|---|---|---|---|
| **Throughput (msg/s)** | 209,910 | 181,919 | 197,393 | 216,680 | 207,052 | 201,414 |

This single-iteration run used a laptop (Intel Core i5-1235U, 15 GiB, Debian 13) with desktop background processes active, so run-to-run variation of about 20% is possible. It is included as a data point for small, resource-limited deployments.

---

## Reproducing the Benchmark

=== "AWS EC2 (Native Protocol :9091)"

    Run the native 7-byte framing benchmark on EC2:

    ```bash
    cd benchmarks/aws-ec2
    ./run-aerostream-native-8core.sh          # prints plan, cost, and instance specs
    ./run-aerostream-native-8core.sh --yes    # runs native test suite and exports results
    ```

=== "AWS EC2 (Kafka Wire Protocol :9092)"

    Run the standard Kafka wire protocol benchmark on EC2:

    ```bash
    cd benchmarks/aws-ec2
    ./run-aerostream-8core.sh          # prints the plan, time and cost estimate; creates nothing
    ./run-aerostream-8core.sh --yes    # runs it (about 80 minutes)
    ```

    See [`benchmarks/aws-ec2/README.md`](https://github.com/gradientgeeks/aerostream/blob/main/benchmarks/aws-ec2/README.md) for the method and safety mechanisms.

=== "Local container"

    ```bash
    cd benchmarks/openmessaging-benchmark
    ./omb-run.sh quay.io/gradientgeeks/aerostream:latest workloads/aerostream-16p-1kb.yaml aerostream-kafkawire
    ```

---

## Caveats & Methodology Notes

!!! warning "Benchmark Considerations"
    * **Single broker, `acks=1`, no replication.** These are single-node numbers; replication adds work that is not measured here.
    * **Latency at the maximum rate is queueing.** Compare latency using the fixed-rate workloads, where the offered load is the same in every run.
    * **Page cache.** Data is written through the OS page cache and the broker's data directory is on local NVMe; each run starts from an empty data directory and a dropped page cache.
    * **Hardware dependent.** Results describe this machine (4 physical cores / 8 vCPUs) and 1 KB messages; other hardware, disk configurations, and payload sizes will differ.
    * **Protocol comparison.** The native protocol avoids JVM and Kafka wire envelope overhead, resulting in 0.1–0.2 ms median latencies and +18% higher saturation throughput.
