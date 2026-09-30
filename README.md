# AeroStream Documentation Portal

[![Documentation Build](https://github.com/gradientgeeks/aerostream-docs/actions/workflows/ci.yml/badge.svg)](https://github.com/gradientgeeks/aerostream-docs/actions)
[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Docs Live](https://img.shields.io/badge/Docs-aerostream.gradientgeeks.com%2Fdocs-cyan)](https://aerostream.gradientgeeks.com/docs/)

This repository houses the official technical documentation and architecture whitepapers for **AeroStream**, built with [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/).

Live Documentation: **[aerostream.gradientgeeks.com/docs](https://aerostream.gradientgeeks.com/docs/)**

---

## Local Development & Live Preview

Run the documentation server locally with live reload:

```bash
# Using uv (recommended, zero-install)
uvx --with mkdocs-material mkdocs serve

# Or using standard pip
pip install -r requirements.txt
mkdocs serve
```

Then navigate to `http://127.0.0.1:8000` in your browser.

---

## Directory Structure

```
.
├── mkdocs.yml              # MkDocs Material configuration & navigation
├── requirements.txt        # Python build dependencies
└── docs/
    ├── index.md            # Platform overview & quickstart
    ├── architecture.md     # Dual-engine architecture deep dive
    ├── benchmarks.md       # OpenMessaging Benchmark (OMB) verified results
    ├── kafka-protocol.md   # Kafka wire protocol compatibility (port 9092)
    ├── sdks.md             # Native client SDKs (Go, Rust, Java, .NET, Node.js)
    ├── schema-registry.md  # Confluent-compatible Schema Registry
    ├── transforms.md       # In-broker stream transforms & WASM engine
    ├── tiered-storage.md   # Multi-cloud tiered storage (S3, MinIO, GCS, Azure)
    ├── security-rbac.md    # RBAC, ACLs, SASL & zero-trust security
    ├── operations.md       # Kubernetes, production sizing, and cluster tuning
    ├── cla.md              # Contributor License Agreement
    ├── diagrams/           # Excalidraw source files & vector SVGs
    ├── images/             # Rendered architecture graphics & assets
    └── javascripts/        # MathJax and custom scripts
```

---

## Contributing

We welcome contributions to AeroStream documentation! Please see **[CONTRIBUTING.md](CONTRIBUTING.md)** and review our **[Contributor License Agreement (CLA)](CLA.md)** before opening pull requests.
