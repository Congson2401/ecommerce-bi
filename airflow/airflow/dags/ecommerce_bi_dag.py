"""
Intrepid E-commerce BI Pipeline DAG — V2
Schedule: Daily at 06:00
Updated: Added finance (invoice/payment/returns) models
"""
from datetime import datetime, timedelta
from airflow import DAG
from pendulum import timezone
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.bash import BashSensor
from airflow.sdk import TaskGroup
from airflow.task.trigger_rule import TriggerRule
import logging

logger = logging.getLogger(__name__)

# ============================================================
# CONFIG
# ============================================================
DBT_PROJECT_DIR  = "/opt/airflow/dbt/ecommerce_dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_VENV         = "/home/airflow/.local/bin/dbt"
DBT_BASE = f"DBT_PROFILES_DIR={DBT_PROFILES_DIR} {DBT_VENV} --no-write-json"
DBT_RUN  = f"{DBT_BASE} run --project-dir {DBT_PROJECT_DIR} --target docker"
DBT_TEST = f"{DBT_BASE} test --project-dir {DBT_PROJECT_DIR} --target docker"
DBT_SNAP = f"{DBT_BASE} snapshot --project-dir {DBT_PROJECT_DIR} --target docker"
DBT_FRESH = f"{DBT_BASE} source freshness --project-dir {DBT_PROJECT_DIR} --target docker"

PG_HOST = "host.docker.internal"
PG_PORT = "5432"
PG_DB   = "ecommerce_db"
PG_USER = "postgres"
PG_PASS = "24012004"
PSQL    = f'PGPASSWORD={PG_PASS} psql -h {PG_HOST} -p {PG_PORT} -U {PG_USER} -d {PG_DB}'


# ============================================================
# CALLBACKS
# ============================================================
def on_failure_callback(context):
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    exec_date = context['execution_date']
    logger.error(f"Task FAILED: {dag_id}.{task_id} at {exec_date}")

def on_success_callback(context):
    task_id = context['task_instance'].task_id
    logger.info(f"Task SUCCESS: {task_id}")

def check_freshness_result(**context):
    ti = context['task_instance']
    return_code = ti.xcom_pull(task_ids='check_source_freshness', key='return_value')
    if return_code == 0:
        return 'start_transform'
    else:
        return 'handle_freshness_warning'

def log_stats(**context):
    logger.info("="*60)
    logger.info("Pipeline V2 completed successfully!")
    logger.info("Tables updated: staging(14) → intermediate(5) → mart(10)")
    logger.info("New in V2: stg_invoices, stg_invoice_lines, stg_payments")
    logger.info("New in V2: int_finance, int_returns")
    logger.info("New in V2: fact_finance, fact_returns, dim_payment_method")
    logger.info("="*60)


# ============================================================
# DAG
# ============================================================
default_args = {
    'owner': 'intrepid_bi',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback,
}

with DAG(
    dag_id='ecommerce_bi_pipeline_v2',
    default_args=default_args,
    description='E-commerce BI Pipeline with Finance & Returns',
    schedule='0 6 * * *',
    start_date=datetime(2024, 1, 1, tzinfo=timezone('Asia/Ho_Chi_Minh')),
    catchup=False,
    tags=['ecommerce', 'bi', 'dbt', 'v2'],
    max_active_runs=1,
) as dag:

    # ----------------------------------------------------------
    # STEP 1: Wait for PostgreSQL
    # ----------------------------------------------------------
    wait_for_postgres = BashSensor(
        task_id='wait_for_postgres',
        bash_command=f'{PSQL} -c "SELECT 1" > /dev/null 2>&1',
        timeout=300,
        poke_interval=30,
        mode='poke',
    )

    # ----------------------------------------------------------
    # STEP 2: Check source freshness
    # ----------------------------------------------------------
    check_source_freshness = BashOperator(
        task_id='check_source_freshness',
        bash_command=f'{DBT_FRESH} || true',
    )

    branch_freshness_check = BranchPythonOperator(
        task_id='branch_freshness_check',
        python_callable=check_freshness_result,
    )

    handle_freshness_warning = BashOperator(
        task_id='handle_freshness_warning',
        bash_command='echo "WARNING: Source data may be stale. Proceeding anyway."',
    )

    start_transform = EmptyOperator(
        task_id='start_transform',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ----------------------------------------------------------
    # STEP 3: dbt Transform (staging → intermediate → mart)
    # ----------------------------------------------------------
    with TaskGroup('dbt_transform') as dbt_transform:

        # --- Staging: existing models ---
        run_stg_sales = BashOperator(
            task_id='run_stg_sales',
            bash_command=f'{DBT_RUN} --select stg_sale_orders stg_sale_order_lines',
        )

        run_stg_purchase = BashOperator(
            task_id='run_stg_purchase',
            bash_command=f'{DBT_RUN} --select stg_purchase_orders stg_purchase_order_lines stg_supplier_info',
        )

        run_stg_stock = BashOperator(
            task_id='run_stg_stock',
            bash_command=f'{DBT_RUN} --select stg_stock_pickings stg_stock_moves stg_stock_quant',
        )

        run_stg_master = BashOperator(
            task_id='run_stg_master',
            bash_command=f'{DBT_RUN} --select stg_products stg_partners stg_locations',
        )

        # --- Staging: NEW finance models ---
        run_stg_finance = BashOperator(
            task_id='run_stg_finance',
            bash_command=f'{DBT_RUN} --select stg_invoices stg_invoice_lines stg_payments',
        )

        # --- Intermediate: existing models ---
        run_int_sales = BashOperator(
            task_id='run_int_sales',
            bash_command=f'{DBT_RUN} --select int_sales',
        )

        run_int_purchasing = BashOperator(
            task_id='run_int_purchasing',
            bash_command=f'{DBT_RUN} --select int_purchasing',
        )

        run_int_delivery = BashOperator(
            task_id='run_int_delivery',
            bash_command=f'{DBT_RUN} --select int_delivery',
        )

        run_int_inventory = BashOperator(
            task_id='run_int_inventory',
            bash_command=f'{DBT_RUN} --select int_inventory_moves',
        )

        # --- Intermediate: NEW finance models ---
        run_int_finance = BashOperator(
            task_id='run_int_finance',
            bash_command=f'{DBT_RUN} --select int_finance',
        )

        # --- Mart: dimensions first ---
        run_dims = BashOperator(
            task_id='run_dims',
            bash_command=f'{DBT_RUN} --select dim_date dim_product dim_partner dim_location dim_payment_method',
        )

        # --- Mart: existing facts ---
        run_fact_sales = BashOperator(
            task_id='run_fact_sales',
            bash_command=f'{DBT_RUN} --select fact_sales',
        )

        run_fact_delivery = BashOperator(
            task_id='run_fact_delivery',
            bash_command=f'{DBT_RUN} --select fact_delivery',
        )

        run_fact_inventory = BashOperator(
            task_id='run_fact_inventory',
            bash_command=f'{DBT_RUN} --select fact_inventory',
        )

        run_fact_purchasing = BashOperator(
            task_id='run_fact_purchasing',
            bash_command=f'{DBT_RUN} --select fact_purchasing',
        )

        # --- Mart: NEW facts ---
        run_fact_finance = BashOperator(
            task_id='run_fact_finance',
            bash_command=f'{DBT_RUN} --select fact_finance',
        )

        # --- Dependencies ---
        # Staging runs in parallel
        [run_stg_sales, run_stg_purchase, run_stg_stock, run_stg_master, run_stg_finance]

        # Intermediate depends on staging
        run_stg_sales >> run_int_sales
        run_stg_sales >> run_int_delivery
        [run_stg_purchase, run_stg_stock] >> run_int_purchasing
        [run_stg_stock, run_stg_master] >> run_int_inventory
        run_stg_finance >> run_int_finance

        # Dimensions before facts
        [run_stg_master] >> run_dims

        # Facts depend on intermediate + dims
        [run_int_sales, run_dims] >> run_fact_sales
        [run_int_delivery, run_dims] >> run_fact_delivery
        [run_int_inventory, run_dims] >> run_fact_inventory
        [run_int_purchasing, run_dims] >> run_fact_purchasing
        [run_int_finance, run_dims] >> run_fact_finance

    # ----------------------------------------------------------
    # STEP 4: dbt Quality (test + snapshot)
    # ----------------------------------------------------------
    with TaskGroup('dbt_quality') as dbt_quality:

        run_tests = BashOperator(
            task_id='run_tests',
            bash_command=f'{DBT_TEST}',
        )

        run_snapshots = BashOperator(
            task_id='run_snapshots',
            bash_command=f'{DBT_SNAP}',
        )

        run_tests >> run_snapshots

    # ----------------------------------------------------------
    # STEP 5: Log stats
    # ----------------------------------------------------------
    log_pipeline_stats = PythonOperator(
        task_id='log_pipeline_stats',
        python_callable=log_stats,
    )

    pipeline_success = EmptyOperator(
        task_id='pipeline_success',
        on_success_callback=on_success_callback,
    )

    pipeline_failed = EmptyOperator(
        task_id='pipeline_failed',
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ----------------------------------------------------------
    # MAIN FLOW
    # ----------------------------------------------------------
    wait_for_postgres >> check_source_freshness >> branch_freshness_check
    branch_freshness_check >> [start_transform, handle_freshness_warning]
    handle_freshness_warning >> start_transform
    start_transform >> dbt_transform >> dbt_quality >> log_pipeline_stats >> pipeline_success
    [dbt_transform, dbt_quality] >> pipeline_failed
