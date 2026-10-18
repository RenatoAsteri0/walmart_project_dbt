from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import time
import pendulum
from dotenv import load_dotenv
from os import getenv
load_dotenv()

@dag(
    dag_id = 'orchestrate',
    schedule = '0 11 * * *',
    catchup = False,
    start_date = pendulum.datetime(2026, 10, 12, tz='America/Sao_Paulo')
)
def orchestrate():
    
    @task
    def ingest_cdc():
        w = WorkspaceClient(
        host=getenv('HOST_DATABRICKS_JOB'),
        token=getenv('TOKEN_DATABRICKS_JOB'))

        job_trigger = w.jobs.run_now(job_id=996324209516271)

        while True:
            job_run = w.jobs.get_run(job_trigger.run_id)
            print(f'Job run status: {job_run.state.life_cycle_state}, result: {job_run.state.result_state}')
            if job_run.state.life_cycle_state in [
                RunLifeCycleState.TERMINATED,
                RunLifeCycleState.INTERNAL_ERROR,
                RunLifeCycleState.SKIPPED,
            ]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully!")
                    break
                else:
                    raise Exception (f"Job failed with state: {job_run.state.result_state}")
            time.sleep(10)

    @task.bash
    def clean_unuse_files():
        return "rm -rf /opt/airflow/walmart_project/target && rm -rf /opt/airflow/walmart_project/logs"
        
    @task.bash
    def source_freshness():
        return "cd /opt/airflow/walmart_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id = "silver_technical",
        cwd = '/opt/airflow/walmart_project',
        bash_command = "dbt run --select silver_t",
        trigger_rule='all_done'
    )

    silver_technical_tests = BashOperator(
        task_id = "silver_technical_tests",
        cwd = '/opt/airflow/walmart_project',
        bash_command = "dbt test --select silver_t"
    )

    silver_business = BashOperator(
        task_id = "silver_business",
        cwd = '/opt/airflow/walmart_project',
        bash_command = "dbt run --select silver_b"
    )
    silver_business_tests = BashOperator(
        task_id = "silver_business_tests",
        cwd = '/opt/airflow/walmart_project',
        bash_command = "dbt test --select silver_b"
    )

    gold_dimensions = BashOperator(
        task_id = 'gold_dimensions',
        cwd = '/opt/airflow/walmart_project',
        bash_command = "dbt snapshot"
    )

    gold_facts = BashOperator(
        task_id = 'gold_facts',
        cwd = '/opt/airflow/walmart_project',
        bash_command = "dbt run --select path:models/gold/fact"
    )

    ingest_cdc() >> clean_unuse_files() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_dimensions >> gold_facts

orchestrate_dag = orchestrate()
