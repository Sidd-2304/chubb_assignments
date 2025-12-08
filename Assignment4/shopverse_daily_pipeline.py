"""
ShopVerse Daily Pipeline - FINAL CLEAN VERSION
Compatible with: Airflow 2.10.x + Python 3.11
"""

from __future__ import annotations
import os
import csv
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# --------------------------------------------------------------------
# AIRFLOW CONFIG
# --------------------------------------------------------------------
POSTGRES_CONN_ID = "postgres_dwh"
BASE_PATH = Variable.get("shopverse_data_base_path", "/opt/airflow/data")
MIN_ORDER_THRESHOLD = int(Variable.get("shopverse_min_order_threshold", 10))
ALERT_EMAIL = Variable.get("shopverse_alert_email", "ops@example.com")

default_args = {
    "owner": "shopverse",
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------
@dag(
    dag_id="shopverse_daily_pipeline",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=True,
    max_active_runs=1,
)
def shopverse_daily_pipeline():

    # ----------------------------------------------------------------
    # File paths for backfills
    # ----------------------------------------------------------------
    customers_file = f"{BASE_PATH}/landing/customers/customers_{{{{ ds_nodash }}}}.csv"
    products_file = f"{BASE_PATH}/landing/products/products_{{{{ ds_nodash }}}}.csv"
    orders_file   = f"{BASE_PATH}/landing/orders/orders_{{{{ ds_nodash }}}}.json"

    # ----------------------------------------------------------------
    # Utility tasks
    # ----------------------------------------------------------------
    @task
    def create_folders():
        for folder in [
            f"{BASE_PATH}/landing/customers",
            f"{BASE_PATH}/landing/products",
            f"{BASE_PATH}/landing/orders",
            f"{BASE_PATH}/anomalies",
        ]:
            os.makedirs(folder, exist_ok=True)
        return "ok"

    @task
    def read_csv(path: str):
        rows = []
        with open(path) as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append(r)
        return rows

    @task
    def read_json(path: str):
        with open(path) as f:
            return json.load(f)

    # ----------------------------------------------------------------
    # FILE SENSORS
    # ----------------------------------------------------------------
    wait_customers = FileSensor(
        task_id="wait_customers",
        filepath=customers_file,
        poke_interval=30,
        timeout=3600,
        fs_conn_id="fs_default",
    )

    wait_products = FileSensor(
        task_id="wait_products",
        filepath=products_file,
        poke_interval=30,
        timeout=3600,
        fs_conn_id="fs_default",
    )

    wait_orders = FileSensor(
        task_id="wait_orders",
        filepath=orders_file,
        poke_interval=30,
        timeout=3600,
        fs_conn_id="fs_default",
    )

    # ----------------------------------------------------------------
    # STAGING TASK GROUP 
    # ----------------------------------------------------------------
    with TaskGroup("staging") as staging:

        @task
        def truncate_table(table_name: str):
            pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            pg.run(f"TRUNCATE TABLE {table_name};")
            return "done"

        @task
        def load_customers_task(path: str):
            rows = read_csv(path)
            pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            for r in rows:
                pg.run("""
                    INSERT INTO stg_customers
                    (customer_id, first_name, last_name, email, signup_date, country)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (customer_id) DO UPDATE SET
                        first_name=EXCLUDED.first_name,
                        last_name=EXCLUDED.last_name,
                        email=EXCLUDED.email,
                        signup_date=EXCLUDED.signup_date,
                        country=EXCLUDED.country;
                    """,
                    parameters=(
                        r["customer_id"], r["first_name"], r["last_name"],
                        r["email"], r["signup_date"], r["country"]
                    )
                )
            return len(rows)

        @task
        def load_products_task(path: str):
            rows = read_csv(path)
            pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            for r in rows:
                pg.run("""
                    INSERT INTO stg_products
                    (product_id, product_name, category, unit_price)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (product_id) DO UPDATE SET
                        product_name=EXCLUDED.product_name,
                        category=EXCLUDED.category,
                        unit_price=EXCLUDED.unit_price;
                    """,
                    parameters=(
                        r["product_id"], r["product_name"], r["category"],
                        r["unit_price"]
                    )
                )
            return len(rows)

        @task
        def load_orders_task(path: str):
            rows = read_json(path)
            pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            count = 0
            for o in rows:
                qty = int(o.get("quantity", 0))
                if qty <= 0:
                    continue
                pg.run("""
                    INSERT INTO stg_orders
                    (order_id, order_timestamp, customer_id, product_id, quantity,
                     total_amount, currency, status, raw_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (order_id) DO UPDATE SET
                        order_timestamp=EXCLUDED.order_timestamp,
                        customer_id=EXCLUDED.customer_id,
                        product_id=EXCLUDED.product_id,
                        quantity=EXCLUDED.quantity,
                        total_amount=EXCLUDED.total_amount,
                        currency=EXCLUDED.currency,
                        status=EXCLUDED.status,
                        raw_json=EXCLUDED.raw_json;
                    """,
                    parameters=(
                        o["order_id"], o["order_timestamp"], o["customer_id"],
                        o["product_id"], qty, o["total_amount"], o["currency"],
                        o["status"], json.dumps(o)
                    )
                )
                count += 1
            return count

        # Execution order
        truncate_table("stg_customers") >> load_customers_task(customers_file)
        truncate_table("stg_products") >> load_products_task(products_file)
        truncate_table("stg_orders")   >> load_orders_task(orders_file)

    # ----------------------------------------------------------------
    # TRANSFORM & LOAD
    # ----------------------------------------------------------------
    @task
    def transform(execution_date):
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Clean staging
        pg.run("DELETE FROM stg_orders WHERE quantity <= 0 OR customer_id IS NULL OR product_id IS NULL;")

        # Dim Customers
        pg.run("""
            INSERT INTO dim_customers
            SELECT customer_id, first_name, last_name, email, signup_date, country, NOW()
            FROM stg_customers
            ON CONFLICT (customer_id) DO UPDATE SET
                first_name=EXCLUDED.first_name,
                last_name=EXCLUDED.last_name,
                email=EXCLUDED.email,
                signup_date=EXCLUDED.signup_date,
                country=EXCLUDED.country,
                last_updated=NOW();
        """)

        # Dim Products
        pg.run("""
            INSERT INTO dim_products
            SELECT product_id, product_name, category, unit_price, NOW()
            FROM stg_products
            ON CONFLICT (product_id) DO UPDATE SET
                product_name=EXCLUDED.product_name,
                category=EXCLUDED.category,
                unit_price=EXCLUDED.unit_price,
                last_updated=NOW();
        """)

        # Fact Orders
        pg.run("""
            INSERT INTO fact_orders
            SELECT DISTINCT ON (order_id)
                order_id,
                order_timestamp,
                customer_id,
                product_id,
                quantity,
                total_amount,
                currency,
                CASE WHEN currency <> 'USD' THEN TRUE ELSE FALSE END,
                status,
                %s
            FROM stg_orders
            ORDER BY order_id, order_timestamp DESC
            ON CONFLICT (order_id) DO UPDATE SET
                order_timestamp=EXCLUDED.order_timestamp,
                customer_id=EXCLUDED.customer_id,
                product_id=EXCLUDED.product_id,
                quantity=EXCLUDED.quantity,
                total_amount=EXCLUDED.total_amount,
                currency=EXCLUDED.currency,
                currency_mismatch_flag=EXCLUDED.currency_mismatch_flag,
                status=EXCLUDED.status,
                load_date=EXCLUDED.load_date;
        """, parameters=(execution_date,))
        return "transformed"

    # ----------------------------------------------------------------
    # DATA QUALITY
    # ----------------------------------------------------------------
    @task
    def dq_customers():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        count = pg.get_first("SELECT COUNT(*) FROM dim_customers;")[0]
        return count > 0

    @task
    def dq_no_nulls():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        count = pg.get_first("""
            SELECT COUNT(*) FROM fact_orders
            WHERE customer_id IS NULL OR product_id IS NULL;
        """)[0]
        return count == 0

    @task
    def dq_fact_matches_stg(execution_date):
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        stg = pg.get_first("SELECT COUNT(*) FROM stg_orders;")[0]
        fact = pg.get_first(
            "SELECT COUNT(*) FROM fact_orders WHERE load_date = %s;",
            parameters=(execution_date,)
        )[0]
        return stg == fact

    @task
    def dq_validate(*checks):
        if not all(checks):
            raise ValueError("DATA QUALITY FAILED")
        return "dq_ok"

    # ----------------------------------------------------------------
    # BRANCHING
    # ----------------------------------------------------------------
    @task
    def count_fact_orders(execution_date):
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        return pg.get_first(
            "SELECT COUNT(*) FROM fact_orders WHERE load_date = %s;",
            parameters=(execution_date,)
        )[0]

    def branch_logic(ti):
        count = int(ti.xcom_pull(task_ids="count_fact_orders"))
        return "warn_low_volume" if count < MIN_ORDER_THRESHOLD else "normal_end"

    branch = BranchPythonOperator(
        task_id="branch_on_volume",
        python_callable=branch_logic,
    )

    @task
    def warn_low_volume(count):
        path = f"{BASE_PATH}/anomalies/low_volume_{{{{ ds_nodash }}}}.txt"
        with open(path, "w") as f:
            f.write(f"LOW ORDER COUNT DETECTED: {count}")
        return path

    end_normal = EmptyOperator(task_id="normal_end")

    notify_failure = EmailOperator(
        task_id="notify_failure",
        to=ALERT_EMAIL,
        subject="ShopVerse DAG FAILURE",
        html_content="Data quality failure detected.",
        trigger_rule="one_failed",
    )

    # ----------------------------------------------------------------
    # ORCHESTRATION — FINAL DAG FLOW
    # ----------------------------------------------------------------
    start = create_folders()

    start >> [wait_customers, wait_products, wait_orders] >> staging

    transform_task = transform("{{ ds }}")

    dq1 = dq_customers()
    dq2 = dq_no_nulls()
    dq3 = dq_fact_matches_stg("{{ ds }}")

    dq_pass = dq_validate(dq1, dq2, dq3)

    # Branching
    count_task = count_fact_orders("{{ ds }}")
    transform_task >> count_task >> branch

    warn = warn_low_volume(count_task)

    branch >> warn
    branch >> end_normal

    dq_pass >> end_normal

    # Failure notifications
    [dq1, dq2, dq3] >> notify_failure


dag = shopverse_daily_pipeline()
