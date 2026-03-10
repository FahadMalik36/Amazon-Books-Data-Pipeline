# Books ETL Pipeline with Apache Airflow & PostgreSQL

An end-to-end data engineering pipeline that automatically scrapes book data, transforms it, and loads it into a PostgreSQL database — orchestrated with Apache Airflow and containerised with Docker.

---

## Pipeline Overview

The DAG runs on a daily schedule and executes 3 tasks in sequence:

```
fetch_amazon_books → deduplicate_books → load_to_postgres
```

| Task | Description |
|---|---|
| `fetch_amazon_books` | Scrapes book title, price, and rating from the web |
| `deduplicate_books` | Removes duplicate entries using pandas |
| `load_to_postgres` | Creates the table if needed and inserts clean data |

Data is passed between tasks using Airflow **XCom**.

---

## Tech Stack

- **Apache Airflow 3** — workflow orchestration
- **Python** — scraping (`requests`, `BeautifulSoup`) and transformation (`pandas`)
- **PostgreSQL 16** — data storage
- **Docker & Docker Compose** — containerised infrastructure
- **pgAdmin** — database inspection

---

## Project Structure

```
├── dags/
│   └── app.py          # DAG definition + ETL logic
├── docker-compose.yaml # Airflow + Postgres + pgAdmin services
├── .env                # Environment variables (UID, pip requirements)
└── README.md
```

---

## Screenshots

### Airflow DAG — All tasks succeeded
![Airflow DAG Graph](screenshots/airflow_dag.png)

### pgAdmin — Data loaded into PostgreSQL
![pgAdmin Table](screenshots/pgadmin_table.png)

---

## Getting Started

### Prerequisites
- Docker Desktop

### Run the pipeline

1. Clone the repo:
   ```bash
   git clone https://github.com/your-username/amazon-books.git
   cd amazon-books
   ```

2. Start all services:
   ```bash
   docker compose up -d
   ```

3. Open Airflow at `http://localhost:8080` (user: `airflow`, password: `airflow`)

4. Add the Postgres connection:
   - Go to **Admin → Connections → +**
   - Set Conn ID: `postgres_default`, Type: `Postgres`, Host: `postgres`, Database/Login/Password: `airflow`, Port: `5432`

5. Enable and trigger the `fetch_and_store_amazon_books` DAG

6. View the data in pgAdmin at `http://localhost:5050` (email: `admin@admin.com`, password: `admin`)

---

## Database Schema

```sql
CREATE TABLE amazon_books (
    id         SERIAL PRIMARY KEY,
    title      VARCHAR(500),
    author     VARCHAR(255),
    price      NUMERIC(10, 2),
    rating     NUMERIC(3, 1),
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```
