from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator


def generate_hello():
    d = "hello World"
    return d


default_args = {
    "owner": "pauline",
    "start_date": datetime(2018, 8, 11),
}

dag = DAG(
    dag_id="test_hello_world",
    description="Simple tutorial DAG with email operator and params",
    schedule_interval="00 09 * * *",
    default_args=default_args,
    catchup=False,
)


hello_op = PythonOperator(
    task_id="hello_task", python_callable=generate_hello, xcom_push=True, dag=dag
)

send_email_op = EmailOperator(
    task_id="send_email",
    to="your_email@gmail.com",
    subject="Airflow Alert",
    params={"content1": hello_operator},
    html_content="Templated Content: {{ task_instance.xcom_pull(task_ids='hello_task') }} !",
    provide_context=True,
    dag=dag,
)

hello_op >> send_email_op
