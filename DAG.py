import logging
import pendulum
import json
from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from . import config
from .stg_repository.stg_repository import StgRepository_currencies
from .stg_repository.stg_repository import StgRepository_transactions
from .cdm_repository.cdm_repository import CdmRepository_global_metrics

log = logging.getLogger('STG_CDM_repository_log')
logging.basicConfig(level=logging.INFO)


@dag( 
    schedule_interval='0 04 * * *',  # Задаем расписание выполнения дага - каждый  день в 4 урта.
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=True,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - True (нужно).
    tags=['Load_from_PG_to_Vertica'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def Load_from_PG_to_Vertica():

    # Получаем аргументы для подключения к ДБ
    pg_conn_arg = config.pg_arg
    vertica_conn_arg = config.vertica_arg
    
    #ВОКЕРЫ
    def Load_from_PG_to_Vertica_STG_currencies(upload_date):
        q = StgRepository_currencies(pg_conn_arg, vertica_conn_arg, log).Load_from_PG_to_Vertica_stg_currencies(upload_date)

    def Load_from_PG_to_Vertica_STG_transactions(upload_date):
        q = StgRepository_transactions(pg_conn_arg, vertica_conn_arg, log).Load_from_PG_to_Vertica_stg_transactions(upload_date)

    def Load_Vertica_CDM_global_metrics(upload_date):
        q = CdmRepository_global_metrics(vertica_conn_arg, log).Load_from_STG_to_CDM_Vertica(upload_date)
 
    @task_group(group_id='Load_from_PG_to_Vertica_STG')
    def task_group_1():

        task_Load_from_PG_to_Vertica_STG_currencies = PythonOperator(
        task_id='Load_from_PG_to_Vertica_STG_currencies',
        python_callable = Load_from_PG_to_Vertica_STG_currencies,
        op_kwargs={'upload_date': '{{macros.ds_add(ds, -1)}}'}
        )   
    
        task_Load_from_PG_to_Vertica_STG_transactions = PythonOperator(
        task_id='Load_from_PG_to_Vertica_STG_transactions',
        python_callable = Load_from_PG_to_Vertica_STG_transactions,
        op_kwargs={'upload_date': '{{macros.ds_add(ds, -1)}}'}
        )

    @task_group(group_id='Load_Vertica_CDM_global_metrics')
    def task_group_2():

        task_Load_Vertica_CDM_global_metrics = PythonOperator(
        task_id='Load_Vertica_CDM_global_metrics',
        python_callable = Load_Vertica_CDM_global_metrics,
        op_kwargs={'upload_date': '{{macros.ds_add(ds, -1)}}'}
        )   

    task_group_1() >> task_group_2()

Start_DAG = Load_from_PG_to_Vertica()