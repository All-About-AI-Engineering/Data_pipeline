import json
import os
from datetime import datetime, timedelta
from textwrap import dedent

import pandas as pd
from newsmodel.preprocess import NewspieacePreprocess

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
# branch를 사용하기 위해서 BranchPythonOperator을 불러와야 한다.
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
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
    dag_id="tutorial-python-op",
    default_args=default_args,
    description="tutorial DAG python",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 23),
    tags=['Datapipeline']
)


def get_jsondata(json_path, csv_path):
    """
    # Description: 주어진 path에 위치한 json data를 반환합니다.
    차후에 패키지 부분으로 넘길 예정

    -------------
    # argument
    - path: json data가 위치한 경로
    -------------
    # Return
    - json_path:  지정한 path에 위치한 json data
    - csv_path : 저장할 csv 파일
    """

    json_files = [
        pos_json
        for pos_json in os.listdir(json_path)
        if pos_json.endswith(".json")
    ]

    json_data = pd.DataFrame(
        columns=["date", "title", "content", "source", "url", "category"]
    )

    for index, js in enumerate(json_files):
        with open(os.path.join(json_path, js), encoding="UTF8") as json_file:
            json_text = json.load(json_file)

            # here you need to know the layout of your json and each json has to have
            # the same structure (obviously not the structure I have here)
            date = json_text["date"]
            title = json_text["title"]
            content = json_text["content"]
            source = json_text["source"]
            url = json_text["url"]
            category = json_text["category"].lower()

            # here I push a list of data into a pandas DataFrame at row given by 'index'
            json_data.loc[index] = [
                date,
                title,
                content,
                source,
                url,
                category,
            ]
    # 필요 없는 카테고리에 속하는 기사들을 제외한다. 이 카테고리에 대한 부분은 지속적으로 보완해야 한다.
    # json_data = json_data[
    #    json_data["category"].str.contains(use_category)
    # ].reset_index(drop=True)
    # json_data = json_data[
    #    ~json_data["category"].str.contains(notuse_category)
    # ].reset_index(drop=True)

    json_data["title"] = json_data["title"].str.replace("\n", " ")
    json_data["content"] = json_data["content"].str.replace("\n", " ")
    json_data.to_csv('{}/daily_csv.csv'.format(csv_path))


dod_command = "python3 /home/joh87411/Pipeline/crawler/DoD_scraper/scraping_latest_news.py"
whitehouse_command = "python3 /home/joh87411/Pipeline/crawler/whitehouse_scraper/scraping_latest_news.py"
dos_command = "python3 /home/joh87411/Pipeline/crawler/dos_scraper/scraping_latest_news.py"
Preprocesor = NewspieacePreprocess("content")

with DAG(**dag_args) as dag:
    start = BashOperator(
        task_id='start',
        bash_command='echo "start crawling"',
    )
    with TaskGroup(group_id="crawler") as crawler:
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

    complete_crawling = DummyOperator(
        task_id='complete_crawling'
    )

    to_csv = PythonOperator(
        task_id='to_csv',
        depends_on_past=False,
        python_callable=get_jsondata,
        op_args=['/home/joh87411/output/', '/home/joh87411/output']
    )

    complete = BashOperator(
        task_id='complete_bash',
        depends_on_past=False,
        bash_command='echo "complete crawling"',
        trigger_rule=TriggerRule.NONE_FAILED
    )
    # 수정 필요- pacakge 자체의 수정이 필요하다.
    preprocessing = BashOperator(
        task_id='preprocessing',
        depends_on_past=False,
        bash_command=Preprocesor.run_preprocessing(
            '/home/joh87411/output/daily_csv')
    )

    start >> crawler >> complete_crawling >> to_csv >> complete
