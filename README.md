# ADS-B Alert System

This is a dynamically generated Apache Airflow project designed to track aircraft entering and leaving specific geographic zones using open data from `adsb.fi`. The system maintains state tracking for aircraft and triggers SMTP-based email alerts based on entry, re-entry, and exit conditions.

## Architecture

The system utilizes the following stack:
- **Apache Airflow:** Orchestrates the fetching, processing, and alerting steps.
- **PostgreSQL:** Persists aircraft state data locally so historical presence is maintained across multiple DAG runs.
- **Docker & Docker Compose:** Containerizes Airflow components and the Database for reproducible deployments.

## Data Source (Why adsb.fi?)

The pipeline pulls from the `adsb.fi` API version 3. 
We chose `adsb.fi` because it is a **free, open-source, and highly reliable** community-driven ADS-B network. Unlike many commercial flight tracking APIs that charge high fees or severely rate-limit data, adsb.fi provides unfiltered open data to the community.

`https://opendata.adsb.fi/api/v3/lat/{lat}/lon/{lon}/dist/{dist}`

This endpoint returns aircraft telemetry within `{dist}` nautical miles of the `{lat}, {lon}` center point, up to a maximum distance of **250 NM**.

### API Limits and Usage Guidelines
By using this system, you are accessing the `adsb.fi` public API endpoints. Please be aware of their terms:
- **Rate Limit:** The public endpoints are rate limited to **1 request per second**. Ensure your DAG schedules do not violate this (e.g., avoid scheduling multiple trackers concurrently at the exact same second).
- **Penalties:** Making excessive invalid HTTP requests (status codes 400, 401, 403, 404, or 429) will result in a temporary IP address restriction. 
- **Airflow Handling:** The Airflow `fetch_data` task explicitly checks for these error codes to fail the task properly rather than pushing empty/invalid data downstream.

- **For more information about the API:** [ADSB.fi OpenData Repository](https://github.com/adsbfi/opendata/blob/main/README.md)
- **To watch real-time global visuals:** [ADSB.fi Global Map](https://globe.adsb.fi/)

## Object-Oriented Database Connection

To ensure scalability and ease of use in future development, database interactions are abstracted using an Object-Oriented approach. 

The `DBConnector` class (located in [`dags/tasks/db_connector.py`](dags/tasks/db_connector.py)) handles all connectivity requirements using credentials from the `.env` file. Any new task module that requires database access can easily instantiate this class without rewriting connection logic:

```python
from tasks.db_connector import DBConnector

db = DBConnector()
conn = db.get_connection()
cursor = conn.cursor()
```

### Zero-Touch Airflow Configuration
Thanks to the environment variable injection configured in our [`docker/docker-compose.yml`](docker/docker-compose.yml), **you do not need to manually create any Connections or Variables in the Airflow UI**. The PostgreSQL connection, Airflow webserver configurations, and SMTP credentials are all injected dynamically at boot time from your `.env` file.

## Dynamic DAG Generation

Instead of writing a separate Airflow DAG Python file for every zone you want to monitor, this system uses [`dags/dynamic_dag_generator.py`](dags/dynamic_dag_generator.py) to parse the [`config/trackers.yaml`](config/trackers.yaml) file. Airflow will automatically create a unique DAG for every block defined in the YAML.

This allows you to scale the system configurationally without modifying Python code.

## Configuration Usage

All zones are defined in [`config/trackers.yaml`](config/trackers.yaml).
Each item must contain:
- `dag_id`: The unique identifier for Airflow.
- `schedule`: Standard CRON expression.
- `location`: Contains `lat`, `lon`, and `radius_nm`.
- `target_filters`: Contains `category_codes` (ADS-B aircraft categories) and `description_keywords` to match specific flight types.
- `alert_config`: SMTP recipient definitions (`email_to`, `email_cc`).

## Deployment Steps

1. Clone or navigate to the project directory.
2. Copy the environment template to a real `.env` file:
   ```bash
   cp .env.template .env
   ```
3. **Configure your `.env` file.**
   Open the `.env` file and fill it out. Here is an explanation of the variables:
   - `AIRFLOW_ADMIN_USER` & `AIRFLOW_ADMIN_PASSWORD`: Used to log into the Airflow UI (e.g., `admin` / `admin123`).
   - `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`: Credentials for the local tracking database (e.g., `airflow` / `supersecret` / `airflow`).
   - **SMTP Settings (Gmail Example):** If you are using Gmail to send alerts, you **cannot** use your normal password. You must generate an App Password:
     1. Go to your Google Account Manage page.
     2. Enable **2-Step Verification** (2FA) if it isn't already.
     3. Search for **"App passwords"**.
     4. Generate a new password (e.g., name it "Airflow Alerts").
     5. Copy the 16-character code and paste it into `SMTP_PASSWORD` without spaces.
     - `SMTP_HOST`: `smtp.gmail.com`
     - `SMTP_PORT`: `587`
     - `SMTP_USER`: `your.email@gmail.com`
     - `SMTP_PASSWORD`: `abcd efgh ijkl mnop` (the generated App Password)

4. Update `config/trackers.yaml` with the exact zones and aircraft you wish to monitor.
5. Navigate to the `docker/` directory.
6. Run the deployment command:
   ```bash
   docker compose --env-file ../.env up -d
   ```
7. Access the Airflow Web UI at `http://localhost:8080` (using the credentials specified in your `.env` file).
8. Enable your generated DAGs in the UI to begin monitoring.
