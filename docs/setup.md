# Setup

## Tools

- Python 3.11+ (this run used 3.12.3)
- JDK 17 or 21 (this run used OpenJDK 21)
- No Azure CLI is required for the local pipeline

## Install and run

```bash
make setup
make pipeline
make test
make dashboard
```

`make setup` creates `.venv` and installs `requirements-dev.txt` (PySpark 3.5.5, delta-spark 3.2.1, pytest, ruff).

`make pipeline` reads the committed sample in `data/sample` (small scale, seed 42) and writes Delta tables under `data/lake`. It does not call Azure.

`make test` runs unit tests and a full tiny medallion run, including a watermark skip, a `--force` replay, and a `--fail-after silver` recovery.

`make dashboard` serves Streamlit at http://localhost:8501. The page states that Power BI is the production target.

## Regenerate data

```bash
make generate          # small, overwrites data/sample, seed 42
make generate-tiny     # data/landing/tiny
make generate-medium   # data/landing/medium, not committed
```

Point a run at another drop with `NL_LANDING` and `NL_LOCAL_ROOT`:

```bash
NL_LANDING=data/landing/medium NL_LOCAL_ROOT=/tmp/nl-medium \
  .venv/bin/python -m northline.cli run --env dev
```

## Configuration

`conf/dev.yml` is local files. `conf/prod.yml` defaults to `abfss://` and Unity Catalog names. Override with the variables in `.env.example`. Do not commit `.env`.

`NL_STORAGE_MODE=adls` is for a Databricks cluster that can see the lake. A laptop run should stay on `local`.
