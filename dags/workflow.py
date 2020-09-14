
from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from random import randrange
from airflow import AirflowException
import os
import requests
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'saga_workflow',
    default_args=default_args,
    description='A simple tutorial DAG',
    #schedule_interval=timedelta(days=1),
)

# t1, t2 and t3 are examples of tasks created by instantiating operators

def init_transaction(**kwargs):
    return str(randrange(10000,99999))


init = PythonOperator(
    task_id='init_transaction',
    provide_context=True,
    python_callable=init_transaction,
    dag=dag,
)

init_log = SimpleHttpOperator(
    task_id='init_log',
    method='GET',
    http_conn_id = 'INTERFACE_SERVICE',
    endpoint='/log',
    data={"text": "[TX:{{ti.xcom_pull(task_ids='init_transaction')}}][orchestrator] Request received. Transaction will be processed now. "},
    headers={},
    dag=dag,
)

def book_hotel_do(**kwargs):
    # get hotel connection string
    conn = os.environ["AIRFLOW_CONN_HOTEL_SERVICE"]
    # send request to service
    r = requests.get(conn + "?tid=" + kwargs['ti'].xcom_pull(task_ids='init_transaction'))
    if r.status_code == 200:
        return 'Book_Flight'
    else:
        return 'hotel_booking_failed'

book_hotel = BranchPythonOperator(
        task_id='Book_Hotel',
        retries=0,
        python_callable=book_hotel_do,
        provide_context=True,
        dag=dag
)

compensate_book_hotel = SimpleHttpOperator(
    task_id='compensate_book_hotel',
    method='GET',
    http_conn_id = 'HOTEL_SERVICE',
    endpoint='/compensate',
    trigger_rule='none_skipped',
    data={"tid": "{{ti.xcom_pull(task_ids='init_transaction')}}"},
    headers={},
    dag=dag,
)

compensate_book_flight_and_hotel1 = SimpleHttpOperator(
    task_id='compensate_book_flight_and_hotel1',
    method='GET',
    http_conn_id = 'FLIGHT_SERVICE',
    depends_on_past=False,
    endpoint='/compensate',
    trigger_rule='none_skipped',
    data={"tid": "{{ti.xcom_pull(task_ids='init_transaction')}}"},
    headers={},
    dag=dag,
)
compensate_book_flight_and_hotel2 = SimpleHttpOperator(
    task_id='compensate_book_flight_and_hotel2',
    method='GET',
    http_conn_id = 'HOTEL_SERVICE',
    depends_on_past=False,
    endpoint='/compensate',
    trigger_rule='none_skipped',
    data={"tid": "{{ti.xcom_pull(task_ids='init_transaction')}}"},
    headers={},
    dag=dag,
)


def book_flight_do(**kwargs):
    # get flight connection string
    conn = os.environ["AIRFLOW_CONN_FLIGHT_SERVICE"]
    # send request to service
    r = requests.get(conn + "?tid=" + kwargs['ti'].xcom_pull(task_ids='init_transaction'))
    if r.status_code == 200:
        return 'Book_Car_Rental'
    else:
        return 'flight_booking_failed'

book_flight = BranchPythonOperator(
        task_id='Book_Flight',
        retries=0,
        python_callable=book_flight_do,
        provide_context=True,
        dag=dag
)

def book_car_rental_do(**kwargs):
    # get flight connection string
    conn = os.environ["AIRFLOW_CONN_CAR_RENTAL_SERVICE"]
    # send request to service
    r = requests.get(conn + "?tid=" + kwargs['ti'].xcom_pull(task_ids='init_transaction'))
    if r.status_code == 200:
        return 'finish'
    else:
        return 'car_rental_booking_failed'

book_car_rental = BranchPythonOperator(
        task_id='Book_Car_Rental',
        retries=0,
        python_callable=book_car_rental_do,
        provide_context=True,
        dag=dag
)



finish = SimpleHttpOperator(
    task_id='finish',
    method='GET',
    http_conn_id = 'INTERFACE_SERVICE',
    endpoint='/log',
    data={"text": "[TX:{{ti.xcom_pull(task_ids='init_transaction')}}][orchestrator] Transaction completed succesfully."},
    headers={},
    trigger_rule='all_success',
    dag=dag,
)

hotel_booking_failed = SimpleHttpOperator(
    task_id='hotel_booking_failed',
    method='GET',
    http_conn_id = 'INTERFACE_SERVICE',
    #trigger_rule='one_failed',
    endpoint='/log',
    data={"text": "[TX:{{ti.xcom_pull(task_ids='init_transaction')}}][orchestrator] Hotel booking failed. No compensating local transaction required."},
    headers={},
    dag=dag,
)

flight_booking_failed = SimpleHttpOperator(
    task_id='flight_booking_failed',
    method='GET',
    http_conn_id = 'INTERFACE_SERVICE',
    #trigger_rule='one_failed',
    endpoint='/log',
    data={"text": "[TX:{{ti.xcom_pull(task_ids='init_transaction')}}][orchestrator] Flight booking failed. Triggering compensation local transaction for Hotel."},
    headers={},
    dag=dag,
)
car_rental_booking_failed = SimpleHttpOperator(
    task_id='car_rental_booking_failed',
    method='GET',
    http_conn_id = 'INTERFACE_SERVICE',
    #trigger_rule='one_failed',
    endpoint='/log',
    data={"text": "[TX:{{ti.xcom_pull(task_ids='init_transaction')}}][orchestrator] Car Rental booking failed. Triggering compensation local transactions for Hotel and Flight."},
    headers={},
    dag=dag,
)

finish_incomplete1 = SimpleHttpOperator(
    task_id='finish_incomplete',
    method='GET',
    http_conn_id = 'INTERFACE_SERVICE',
    #trigger_rule='one_failed',
    endpoint='/log',
    data={"text": "[TX:{{ti.xcom_pull(task_ids='init_transaction')}}][orchestrator] Transaction was not successfully completed."},
    headers={},
    dag=dag,
)
finish_incomplete2 = SimpleHttpOperator(
    task_id='finish_incomplete2',
    method='GET',
    http_conn_id = 'INTERFACE_SERVICE',
    #trigger_rule='one_failed',
    endpoint='/log',
    data={"text": "[TX:{{ti.xcom_pull(task_ids='init_transaction')}}][orchestrator] Transaction was not successfully completed."},
    headers={},
    dag=dag,
)
finish_incomplete3 = SimpleHttpOperator(
    task_id='finish_incomplete3',
    method='GET',
    http_conn_id = 'INTERFACE_SERVICE',
    #trigger_rule='one_failed',
    endpoint='/log',
    data={"text": "[TX:{{ti.xcom_pull(task_ids='init_transaction')}}][orchestrator] Transaction was not successfully completed."},
    headers={},
    dag=dag,
)


init >> init_log >> book_hotel >> [book_flight,hotel_booking_failed]
#book_hotel >> finish_incomplete1
book_flight >> [book_car_rental,flight_booking_failed]
book_flight >> flight_booking_failed
flight_booking_failed >> compensate_book_hotel
compensate_book_hotel >> finish_incomplete1
hotel_booking_failed >> finish_incomplete2
book_car_rental >> [finish,car_rental_booking_failed]
car_rental_booking_failed >> compensate_book_flight_and_hotel1 >> compensate_book_flight_and_hotel2 >> finish_incomplete3