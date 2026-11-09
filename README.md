[![Helios Integration Engine](https://github.com/HeliosSoftware/hie/blob/main/github-banner.png)](https://heliossoftware.com)

# Helios Integration Engine

The Helios Integration Engine (HIE) moves healthcare data into [HL7® FHIR®](https://hl7.org/fhir): lab CSVs, HL7 v2 messages, SQL databases, S3 drops and other FHIR servers in, FHIR resources delivered to the [Helios FHIR Server](https://github.com/HeliosSoftware/hfs) out, with every row accounted for. It is written in Rust, and transforms are written in Python.

## Why HIE?

- **🧾 Every row accounted for**: each record is stored before its file is acknowledged, and keeps its file, line, outcome and reason; nothing is lost when a server is down, and a bad row is rejected on its own with the line and the reason
- **🔁 Idempotent delivery**: resources go out by id or by business identifier, so a re-sent file updates instead of duplicating; large loads go through `$bulk-submit` and are settled by receipts
- **🐍 Transforms in Python**: a function that takes one record and returns resources, with terminology lookups against a FHIR terminology server; run in a local Python worker
- **📡 Sources that fit what customers have**: folders, S3 buckets (public, read-only, or announced by SQS), SQL databases, HL7 v2 over MLLP, other FHIR servers; zips unpacked as drops
- **🔔 Operable**: alert rules for silence, missing deliveries, rejected shares and unreachable servers; Prometheus metrics; an API with scopes behind any OIDC provider; several instances on one store

## What People Build with HIE

- **Lab and device feeds into a FHIR server**: CSV or HL7 v2 results becoming `Observation`s with LOINC codes
- **Migrations**: another FHIR server or a bulk export loaded into HFS, by the million
- **Registries and warehouses**: SQL tables pulled by a cursor and delivered as FHIR
- **Customer drops**: files landing in a bucket, taken, transformed, delivered and archived without anyone watching

# Quick Start

A public sandbox runs the latest release at `https://hie.heliossoftware.com` (`/health`, `/api`).

With Docker, against a FHIR server on your machine:

```bash
docker run --rm -p 8090:8090 \
  -v $PWD/hie.yaml:/hie/hie.yaml:ro \
  -v $PWD/sources:/hie/sources -v $PWD/transforms:/hie/transforms \
  ghcr.io/heliossoftware/hie:latest
```

From source, with Rust 1.90+ and Python 3.9+:

```bash
cargo run -- check      # validate hie.yaml and sources/
cargo run               # API on http://127.0.0.1:8090
```

Then drop `examples/data/labs.csv` into `inbox/lab-results/` and watch `GET /api/sources/lab-results`. The configuration, the environment variables and the Docker setup are in [Running HIE](docs/running.md).

# Documentation

- [Running HIE](docs/running.md) — binary, Docker, environment, the store and several instances
- [Sources](docs/sources.md) — folder, S3, SQS, FHIR server, database, HL7 v2 over MLLP; formats; zips; delivery methods; alert rules
- [Transforms](docs/transforms.md) — the Python contract, terminology, where transforms run
- [API and authentication](docs/api.md) — the HTTP API, scopes, the engine's identity towards HFS
- [Development, tests, release](docs/development.md) — layout, test suites, releasing to the sandbox
- [What the engine does, shown](docs/prototype-results.md) — scenarios against a real HFS
- [Decisions](docs/decisions.md) — the reasons behind each design choice
- [Discussions](https://github.com/HeliosSoftware/hie/discussions) — the architecture (#1), the stack (#2), the shared Python runtime (#3)
- [Roadmap](ROADMAP.md)

# License

HIE is released under the [MIT License](LICENSE.md).
