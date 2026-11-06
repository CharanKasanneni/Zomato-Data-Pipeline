# Zomato Free Local Data Pipeline

This project is a local data pipeline for food delivery data. It uses only free tools. It needs no cloud account. It needs no API key.

## Quick start

Run these commands in the project folder. The first command makes the virtual environment.

```bash
python -m venv .venv
```

Activate it. On Windows, run `.venv\Scripts\Activate.ps1`. On Linux and macOS, run `source .venv/bin/activate`. Then:

```bash
pip install -r requirements.txt
python -m datagen.generate
python run_pipeline.py
```

All five steps must show `ok`. The run takes about 2 minutes. The step `publish_postgres` skips itself when the variable `ZOMATO_PG_URL` is not set.

## Architecture

The diagram shows the parts of the project and the direction of the data. DuckDB builds all data. Postgres serves the finished tables. Airflow starts the pipeline steps. Airflow is optional.

```mermaid
flowchart LR
    subgraph SRC[Sources]
        GEN[datagen.generate]
        SRCF[datagen.from_source]
    end

    subgraph LAKE[Lake]
        CSV[CSV files in data/lake/raw]
    end

    subgraph WH[DuckDB warehouse]
        RAW[(raw)]
        STG[(staging views)]
        MRT[(marts)]
        SNP[(snapshots)]
        AIS[(ai)]
    end

    subgraph AIL[AI layer]
        ENR[enrich_reviews]
        LLM[Ollama, OpenAI, Claude or rules]
        APP[Review chat and text-to-SQL]
    end

    subgraph SRV[Serving]
        PG[(Postgres marts)]
        TOOLS[Other tools]
    end

    subgraph ORC[Orchestration]
        RUN[run_pipeline.py]
        AF[Airflow DAG zomato_batch]
    end

    GEN --> CSV
    SRCF --> CSV
    CSV -->|reload_raw| RAW
    RAW -->|dbt| STG
    STG -->|dbt| MRT
    STG -->|dbt| SNP
    STG -->|stg_reviews| ENR
    ENR <--> LLM
    ENR --> AIS
    AIS -->|dbt| MRT
    MRT -->|publish_postgres| PG
    PG --> TOOLS
    MRT --> APP
    AIS --> APP
    RUN -.->|runs the steps| WH
    AF -.->|runs the same steps| WH
```

| Part | Tool | Role |
|---|---|---|
| Lake | Local CSV files | Keeps the raw data as it arrives |
| Warehouse | DuckDB | Stores and transforms all data |
| Transformation | dbt-core and dbt-duckdb | Builds and tests the models |
| AI layer | Ollama, OpenAI, Claude or rules | Adds sentiment and topic to each review |
| Serving | Postgres in Docker | Gives the finished tables to other tools |
| Orchestration | `run_pipeline.py` and Airflow | Runs the five steps in order |

## Free tools in this project

| Cloud tool | Free tool in this project |
|---|---|
| Amazon S3 | Local folder `data/lake/raw/<table>` |
| Snowflake | DuckDB file `data/warehouse/zomato.duckdb` |
| Snowflake COPY INTO | DuckDB `read_csv` |
| dbt with Snowflake | dbt-core with dbt-duckdb |
| Airflow cloud service | Airflow DAG file and the runner `run_pipeline.py` |
| Cloud database for serving | Postgres in Docker (optional) |
| OpenAI | Ollama (default). OpenAI and Claude are optional. |

## Install

The project needs Python 3.12 on Windows, Linux or macOS.

1. Create a virtual environment and activate it.
2. Install the packages:

```bash
pip install -r requirements.txt
```

On Windows, activate the environment with `Scripts\Activate.ps1` in PowerShell.

The tested versions are duckdb 2.0.0, dbt-core 1.12.6 and dbt-duckdb 1.11.1.

## Data

The project has two data sources. Use one of them.

### Source 1: generated data

Run this command to write generated data into `data/lake/raw/`:

```bash
python -m datagen.generate
```

The default size is 100,000 orders. The run takes about 5 seconds.

Use these options to add more data:

- `--batch2 50 --status-changes 10` adds a second batch of orders.
- `--mutate-restaurants 3` changes the rating and cost of 3 restaurants.

### Source 2: prepared source files

The folder `data/source/` holds four files: `users.csv`, `food.csv`, `menu.csv` and `order_items.csv`. The files hold about 10 million orders and 23 million order items.

The command `python -m datagen.prepare_source` writes these four files. It changes the person names and the dish names. It maps each restaurant id to a new id. It sets a new price for each dish. It sets each line amount to the new price times the quantity.

The source files have no orders table, no restaurants table and no reviews table. The loader `datagen/from_source.py` builds these tables from the order items:

- The loader sets the amount of each order to the sum of its order items.
- The loader creates one restaurant for each restaurant id in the source.
- The loader creates a review for 3 percent of the delivered orders.
- The loader writes an empty password column. The source passwords never enter the warehouse.

Run the loader with a separate lake folder:

```bash
python -m datagen.from_source --out data/lake_source/raw --max-orders 1000000
```

Remove `--max-orders` to load all 10 million orders. The full load needs much more time and disk space.

## Run the pipeline

The runner runs five steps in this order:

1. `reload_raw` loads the lake files into the DuckDB raw tables.
2. `dbt_build_core` builds the staging models, the dimensions, the facts and the marts.
3. `enrich_reviews` adds sentiment and topic to each review.
4. `dbt_build_ai` builds the review insights mart.
5. `publish_postgres` copies the mart tables to Postgres. The step does nothing when the variable `ZOMATO_PG_URL` is not set.

Run all steps:

```bash
python run_pipeline.py
```

Run the pipeline on the prepared source data. These commands are for Bash:

```bash
export ZOMATO_LAKE_DIR="$PWD/data/lake_source/raw"
export ZOMATO_DUCKDB_PATH="$PWD/data/warehouse/zomato_source.duckdb"
python run_pipeline.py
```

The run of 1 million orders takes about 70 seconds.

Use `--steps` to run some steps only. Example: `python run_pipeline.py --steps enrich_reviews,dbt_build_ai`.

The runner returns exit code 0 when all steps succeed. It returns 1 when a step fails. It returns 2 for an unknown step name.

### Replace the raw data

The facts `fct_orders` and `fact_order_items` are incremental. A normal run adds new rows. It does not remove rows that left the raw data. After you replace the raw data with a different set, build all models again from the start:

```bash
python -m pipeline.dbt build --full-refresh --exclude tag:ai
python run_pipeline.py
```

### Check the result

All steps must show `ok`. Then run this command:

```bash
python -c "import duckdb; c=duckdb.connect('data/warehouse/zomato.duckdb', read_only=True); print(c.sql('select count(*) from marts.fct_orders').fetchall())"
```

The count must match the number of orders in the lake. The default is 100,000. Close other programs that open the DuckDB file before you run the pipeline. An open connection locks the file.

## Warehouse layout

| Schema | Content |
|---|---|
| `raw` | Tables loaded from the lake files |
| `staging` | Views that clean the raw tables |
| `marts` | Dimensions, facts and marts |
| `snapshots` | History of restaurant changes (SCD2) |
| `ai` | Review sentiment and topic |

The facts `fct_orders` and `fact_order_items` are incremental models. They use the merge strategy.

## AI layer

### Choose a provider

Set these environment variables:

| Variable | Meaning |
|---|---|
| `LLM_PROVIDER` | `ollama` (default), `openai`, `anthropic` or `none` |
| `LLM_MODEL` | Model name. Each provider has a default. |
| `EMBED_MODEL` | Embedding model. Search uses TF-IDF when this variable is not set. |
| `OPENAI_API_KEY` | Key for OpenAI |
| `ANTHROPIC_API_KEY` | Key for Claude |

The project reads the keys from the environment only. It never writes a key to a file.

To use OpenAI or Claude, install one package:

```bash
pip install openai
```

```bash
pip install anthropic
```

Claude has no embedding API. With the provider `anthropic`, the search uses Ollama embeddings when Ollama runs. Otherwise the search uses TF-IDF.

### Review enrichment

The step `enrich_reviews` sends each new review to the model. The model returns sentiment and topic. If the model fails, the step uses a rule-based method (VADER sentiment and keyword topics). The step stops calling the model after 5 bad answers in a row.

A local model is slow. A run of 2,638 reviews with llama3.2:3b takes about 3 hours. Set `LLM_PROVIDER=none` to use the rule-based method only.

### Applications

Start the review chat:

```bash
streamlit run ai/rag_chat.py
```

Start the text-to-SQL application:

```bash
streamlit run ai/text_to_sql.py
```

The text-to-SQL application accepts one SELECT statement only. It rejects every other statement. It opens the warehouse as read-only with external access off.

## Airflow

The DAG file is `airflow/dags/zomato_batch.py`. Airflow does not run on native Windows. Use Docker or WSL2.

Steps for WSL2 with Ubuntu:

1. Install Python 3.12.
2. Create a virtual environment and install Airflow with the constraint file of Airflow 3.0.6.
3. Install the project packages in the same environment.
4. Set the DAG folder and run Airflow:

```bash
export AIRFLOW__CORE__DAGS_FOLDER=<path to this project>/airflow/dags
airflow db migrate
airflow standalone
```

### Airflow in Docker

The file `docker-compose.yml` starts two services:

- `postgres` stores the Airflow metadata in the database `airflow`. It also holds the database `zomato`, which receives the mart tables.
- `airflow` runs Airflow 3.0.6. The image has the project packages. The container mounts the project folder.

```bash
docker compose up --build
```

Open http://localhost:8080. The login name is `admin`. Get the password with this command:

```bash
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Unpause the DAG `zomato_batch` and start it with **Trigger**.

The container reads the lake files from `data/lake/raw`. Run `python -m datagen.generate` first.

Do not run the pipeline on your computer and in the container at the same time. Both use the same DuckDB file.

Stop the services with `docker compose down`. This keeps your data. The option `-v` deletes the Postgres data and the Airflow password.

The user name and the password of Postgres are both `zomato`. These values are for local use only.

## Postgres

DuckDB builds all data. Postgres has two roles only:

1. Postgres stores the Airflow metadata.
2. Postgres serves the finished mart tables to other tools.

The step `publish_postgres` copies each table of the DuckDB schema `marts` to the Postgres schema `marts`. The copy runs in one transaction. If the copy fails, Postgres keeps the old tables.

Set the connection text, then run the step:

```bash
export ZOMATO_PG_URL="postgresql://zomato:zomato@localhost:5433/zomato"
python run_pipeline.py --steps publish_postgres
```

The port 5433 is the host port of the compose service `postgres`. Start the service first: `docker compose up -d postgres`.

## See the pipeline

### Workflow of the five steps

Each step starts only after the step before it succeeds. If a step fails, the run stops and the next steps do not run.

```mermaid
flowchart LR
    S1[1. reload_raw<br/>lake files to raw tables] --> S2[2. dbt_build_core<br/>staging, dimensions, facts, marts]
    S2 --> S3[3. enrich_reviews<br/>sentiment and topic]
    S3 --> S4[4. dbt_build_ai<br/>review insights mart]
    S4 --> S5[5. publish_postgres<br/>copy marts to Postgres]
    S5 --> OK([All steps ok])
    S1 -.->|failure| STOP([Run stops, exit code 1])
    S2 -.->|failure| STOP
    S3 -.->|failure| STOP
    S4 -.->|failure| STOP
    S5 -.->|failure| STOP
```

### Models of dbt

```mermaid
flowchart LR
    subgraph Raw
        r[raw tables]
    end
    subgraph Staging
        so[stg_orders]
        soi[stg_order_items]
        sr[stg_restaurants]
        su[stg_users]
        sf[stg_food]
        sm[stg_menu]
        srv[stg_reviews]
    end
    subgraph Marts
        fo[fct_orders]
        foi[fact_order_items]
        dd[dim_date]
        dc[dim_customer]
        dr[dim_restaurants]
        df[dim_food]
        m1[mart_daily_city_revenue]
        m2[mart_delivery_sla]
        m3[mart_restaurant_performance]
        mri[mart_review_insights]
    end
    ai[(ai.review_enriched)]
    r --> so & soi & sr & su & sf & sm & srv
    so --> fo & dd & foi
    soi --> foi
    su --> dc
    sr --> dr
    sf --> df
    fo --> m1 & m2 & m3
    dr --> m1 & m2 & m3 & mri
    srv --> m3 & mri
    srv -->|enrich_reviews| ai
    ai --> mri
```

The model `stg_menu` has no model that uses it. The dbt docs site (next section) shows every link.

### Lineage of the dbt models

dbt makes a website that shows each model, each column and the links between them. Run these commands:

```bash
python -m pipeline.dbt docs generate
python -m pipeline.dbt docs serve --port 8081
```

Open http://localhost:8081. Click the green button at the bottom right to see the lineage graph. Click a model to see its SQL, its columns and its tests.

### Runs in Airflow

Open http://localhost:8080. Click the DAG `zomato_batch`, then click **Graph**. The graph shows the five tasks in order. A green task succeeded. A red task failed. Click a task, then click **Logs**, to read its output.

The runner `run_pipeline.py` does the same work. The project does not need Airflow.

## Tests

Run all tests:

```bash
python -m pytest -q
```

The suite has 288 tests. Some tests are skipped: one needs Airflow, and three need the variable `ZOMATO_TEST_PG_URL` (a Postgres test database). The tests never call Ollama or a paid API.

## Limits

- A normal run does not remove rows that left the raw data. Use `--full-refresh` as shown in "Replace the raw data".
- A late order item for an unchanged order does not reach the incremental models.
- The Airflow image shows a version warning between dbt and Airflow (protobuf). The DAG runs without error.
- DuckDB allows one writer. Do not run the pipeline on your computer and in Docker at the same time.
- The tests and the generated data contain no real personal data. The names, emails and passwords in the generated data are random values.

## Folder map

| Folder or file | Content |
|---|---|
| `datagen/` | Generator and source loader |
| `warehouse/` | SQL files, the loader for the raw tables and the copy to Postgres |
| `zomato_dbt/` | dbt project |
| `pipeline/` | Shared settings and pipeline steps |
| `ai/` | Model clients, enrichment, search and applications |
| `airflow/dags/` | Airflow DAG |
| `run_pipeline.py` | Runner |
| `tests/` | Tests |
| `data/` | Lake, source files and warehouse (not in version control) |
