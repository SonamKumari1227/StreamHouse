# ingestion/

Everything that lands raw data in Bronze. Append-only, no business logic, no reshaping beyond what
the contract requires.

| Path | Purpose | Status |
| --- | --- | --- |
| `bronze_cdc_stream.py` | Spark Structured Streaming: Kafka → Bronze Delta. Checkpointed and idempotent, so exactly-once survives a mid-batch kill. Contract violations route to `dlq.*` rather than being dropped. | Phase 2 |
| `bronze_gps_stream.py` | The same for the GPS ping topic. | Phase 2 |
| `api_extractors/` | Open-Meteo and Nager.Date pulls — incremental, watermarked, idempotent on re-run, with retry and backoff. Both are free public HTTPS APIs with no key. | Phase 4 |

Bronze is immutable and partitioned by `ingest_date`. It keeps the full payload plus Kafka offset
metadata, so anything downstream can be rebuilt from it.
