from airflow import DAG
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['t-virus12@yandex.com'],
    'email_on_failure': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'DAG_DMDS_DATA_CLIENT_HIST__DMOUTDS_DATA_CLIENT_HIST__ADV',
    default_args=default_args,
    description='Загрузка сущности dmoutds_data.client_hist__adv',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1, 0, 15),
    tags=['DMDS'],
)

# Выполняем предварительную очистку промежуточной сущности
sq__task__dmoutds_data_client_hist__adv__truncate = """
TRUNCATE TABLE dmoutds_data.client_hist__adv
"""

# Заполняем промежуточную сущность
sq__task__dmoutds_data_client_hist__adv__insert = """
INSERT INTO dmoutds_data.client_hist__adv (
  src_client_id,
  client_type,
  client_name,
  valid_from,
  valid_to,
  as_of_day,
  operation_day
)
SELECT a.src_client_id,
       a.client_type,
       ltrim(a.second_name || ' ' || a.first_name || ' ' || a.middle_name) as "CLIENT_NAME",
       a.valid_from,
       a.valid_to,
       trunc(SYSDATE) AS "AS_OF_DAY",
       trunc(SYSDATE) - 1 AS "OPERATION_DAY"
FROM dmds_data.client_hist a
WHERE a.valid_from >= trunc(SYSDATE) - 14
"""

# Ждём выполнения пререквизитного дага DAG_DMINDS_DATA_CLIENT_HIST__DMDS_DATA_CLIENT_HIST
wait_for_other_dag = ExternalTaskSensor(
    task_id='wait_for_DAG_DMINDS_DATA_CLIENT_HIST__DMDS_DATA_CLIENT_HIST',
    external_dag_id='DAG_DMINDS_DATA_CLIENT_HIST__DMDS_DATA_CLIENT_HIST',
    external_task_id=None,  # Указывать конкретную задачу или None, чтобы дожидаться всего DAG
    check_existence=True,
    timeout=7200,  # Количество секунд ожидания перед тайм-аутом
    poke_interval=60,  # Как часто проверять статус задачи
    dag=dag
)

# Выполняем очистку таблицы в области выгрузки для последующей полной перегрузки
task__dmoutds_data_client_hist__adv__truncate = OracleOperator(
    task_id='task__dmoutds_data_client_hist__adv__truncate',
    oracle_conn_id='test_xe',
    sql=sq__task__dmoutds_data_client_hist__adv__truncate,
    dag=dag,
)

# Заполнение таблицы
task__dmoutds_data_client_hist__adv__insert = OracleOperator(
    task_id='task__dmoutds_data_client_hist__adv__insert',
    oracle_conn_id='test_xe',
    sql=sq__task__dmoutds_data_client_hist__adv__insert,
    dag=dag,
)

wait_for_other_dag >> task__dmoutds_data_client_hist__adv__truncate >> task__dmoutds_data_client_hist__adv__insert
