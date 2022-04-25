from datetime import datetime, timedelta
from textwrap import dedent

from newsmodel.preprocess import NewspieacePreprocess

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
# branch를 사용하기 위해서 BranchPythonOperator을 불러와야 한다.
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': "admin",
    'depend_on_past': False,
    'email': "13a71032776@gmail.com",
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

dag_args = dict(
    dag_id="test_data_pipeline",
    default_args=default_args,
    description="tutorial DAG python",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 23),
    tags=['Datapipeline']
)

dod_command = "python3 /home/joh87411/Pipeline/crawler/DoD_scraper/scraping_latest_news.py"
whitehouse_command = "python3 /home/joh87411/Pipeline/crawler/whitehouse_scraper/scraping_latest_news.py"
dos_command = "python3 /home/joh87411/Pipeline/crawler/dos_scraper/scraping_latest_news.py"
Preprocesor = NewspieacePreprocess(body_column_name="content")

with DAG(**dag_args) as dag:
    start = BashOperator(
        task_id='start',
        bash_command='echo "start crawling"',
    )

    dod = BashOperator(
        task_id='dod_crawler',
        depends_on_past=False,
        bash_command=dod_command
    )

    whitehouse = BashOperator(
        task_id='whitehouse_crawler',
        depends_on_past=False,
        bash_command=whitehouse_command
    )

    dos = BashOperator(
        task_id='dos_crawler',
        depends_on_past=False,
        bash_command=dos_command
    )

    to_csv = BashOperator(
        task_id='to_csv',
        depends_on_past=False,
        bash_command=get_jsondata(
            '/home/joh87411/output', '/home/joh87411/output')
    )

    preprocessing = BashOperator(
        task_id='preprocessing',
        depends_on_past=False,
        bash_command=Preprocesor.run_preprocessing(
            '/home/joh87411/output/daily_csv')
    )

    complete = BashOperator(
        task_id='complete_bash',
        depends_on_past=False,
        bash_command='echo "complete crawling"',
        trigger_rule=TriggerRule.NONE_FAILED
    )

    start >> [dod, whitehouse, dos] >> to_csv >> preprocessing >> complete
