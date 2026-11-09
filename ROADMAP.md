# Helios Integration Engine — Roadmap

> This document outlines the development direction for the Helios Integration Engine (HIE). It is organized into three horizons — **Now**, **Next**, and **Later** — to set expectations without overpromising timelines. Items move between horizons as priorities evolve with customer needs, community feedback, and contributor availability.
>
> Want to influence the roadmap? Open or comment on a [GitHub Discussion](https://github.com/HeliosSoftware/hie/discussions).

---

## ✅ Shipped

Available today in the current release. The reasons behind each choice are in [docs/decisions.md](docs/decisions.md).

**Sources**

- Folder, S3 bucket (including public and read-only buckets, and buckets that announce objects through an SQS queue), another FHIR server, SQL databases (PostgreSQL, MySQL, SQLite, SQL Server), HL7 v2 over MLLP — see [docs/sources.md](docs/sources.md)
- CSV, NDJSON, FHIR Bundle, HL7 v2 and zipped drops of any of them

**Transforms**

- Python transforms, one record in, FHIR resources out, with terminology lookups and translations against a FHIR terminology server — see [docs/transforms.md](docs/transforms.md)
- Transforms run in a local Python worker, one per source

**Delivery**

- Batch Bundles (REST), idempotent by id or identifier, with retries and backoff
- `$bulk-submit` to the Helios FHIR Server: manifests over source objects or over the engine's transformed NDJSON, settled by receipts

**Operations**

- Every row stored before its file is acknowledged, with file, line, outcome and reason
- Alert rules (silence, missing by, rejected share, unreachable) with webhooks; Prometheus metrics
- SQLite or PostgreSQL store; several instances sharing one store with leases
- API with SMART-style scopes (`hie/read`, `hie/manage`, `hie/transforms`) behind any OIDC provider; client credentials towards HFS

---

## 🔨 Now — Actively In Progress

| Area | Item | Status |
|------|------|--------|
| **Runtime** | The local worker runs each transform under operating-system limits (memory, files written, open files) and a per-request deadline | 🟡 In progress |
| **Customers** | First real loads: lab CSV drops in S3, zipped FHIR exports from a public bucket | 🟡 In progress |
| **Documentation** | Restructure the documentation; publish a book like HFS's | 🟡 In progress |

---

## 🧭 Next

- Loads view: failures grouped by reason with a sample row, and "retry these N rows"
- User interface for sources, loads, transforms and alerts, following the HFS administrative UI
- Patient matching: source identifiers to `Patient` references
- Outbound: FHIR Subscriptions and HL7 v2 messages out of HFS
- HL7 v2 batches (`BHS`/`BTS`), TLS on the MLLP listener
- CDA / C-CDA documents as a source format
- Private-key JWT (SMART backend services) towards HFS

---

## 🔭 Later

- Connectors: MongoDB, Oracle, SFTP, Kafka
- FHIR Mapping Language and StructureMap as an alternative to Python
- Record-level parallelism within one source

---

## License

HIE is released under the [MIT License](LICENSE.md).
