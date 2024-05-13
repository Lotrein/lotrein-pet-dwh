from airflow import DAG
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['t-virus12@yandex.ru'],
    'email_on_failure': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'DAG_DMINDS_DATA_CLIENT_HIST__DMDS_DATA_CLIENT_HIST',
    default_args=default_args,
    description='Загрузка сущности dmds_data.client_hist',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['DMDS'],
)

# Выполняем предварительную очистку промежуточной сущности
sq__task__dmds_stg_client_dmdelta__truncate = """
TRUNCATE TABLE dmds_stg.client_dmdelta
"""

# Заполняем промежуточную сущность
sq__task__dmds_stg_client_dmdelta__insert = """
INSERT INTO dmds_stg.client_dmdelta (
  src_client_id,
  dwh_client_id,
  client_type,
  first_name,
  second_name,
  middle_name,
  valid_from,
  valid_to,
  job_update,
  job_insert,
  as_of_day,
  operation_day
)
SELECT a.src_client_id,
       a.dwh_client_id,
       b.client_type_name,
       a.first_name,
       a.second_name,
       a.middle_name,
       a.valid_from,
       a.valid_to,
       a.job_update,
       a.job_insert,
       trunc(SYSDATE) AS "AS_OF_DAY",
       trunc(SYSDATE) - 1 AS "OPERATION_DAY"
FROM dminds_data.client_hist a
JOIN dminds_data.client_type_dict b ON a.client_type_id = b.client_type_id
WHERE a.valid_from >= trunc(SYSDATE) - 14
"""

# Выделяем из дельты новые данные и загружаем в конечную таблицу
sq__task__dmds_data_client_hist__merge = """
BEGIN
  superuser.p_upload_merge(p_target_owner_name => 'dmds_data',
                           p_target_table_name => 'client_hist',
                           p_source_owner_name => 'dmds_stg',
                           p_source_table_name => 'client_dmdelta');
END;
"""

# Очистка промежуточной таблицы
task__dmds_stg_client_dmdelta__truncate = OracleOperator(
    task_id='task__dmds_stg_client_dmdelta__truncate',
    oracle_conn_id='test_xe',
    sql=sq__task__dmds_stg_client_dmdelta__truncate,
    dag=dag,
)

# Заполнение промежуточной таблицы
task__dmds_stg_client_dmdelta__insert = OracleOperator(
    task_id='task__dmds_stg_client_dmdelta__insert',
    oracle_conn_id='test_xe',
    sql=sq__task__dmds_stg_client_dmdelta__insert,
    dag=dag,
)

# Инкрементальное заполнение целевой таблицы
task__dmds_data_client_hist__merge = OracleOperator(
    task_id='task__dmds_data_client_hist__merge',
    oracle_conn_id='test_xe',
    sql=sq__task__dmds_data_client_hist__merge,
    dag=dag,
)

task__dmds_stg_client_dmdelta__truncate >> task__dmds_stg_client_dmdelta__insert >> task__dmds_data_client_hist__merge
