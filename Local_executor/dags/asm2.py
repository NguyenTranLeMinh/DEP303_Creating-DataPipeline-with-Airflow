from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from myFunctions import _branching, _dowload_question_file_task, _dowload_answer_file_task
from datetime import datetime


default_args = {
    'start_date': datetime(2022,2,6)
}

with DAG('asm2',
        schedule_interval='@daily',
        default_args=default_args,
        catchup=False) as dag:
    start = DummyOperator(
        task_id='start'
    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=_branching,
        do_xcom_push=False
    )

    end = DummyOperator(
        task_id='end'
    )

    with TaskGroup(group_id='processing') as processing:
        clear_file = BashOperator(
            task_id='clear_file',
            bash_command='''rm ~/airflow/downloads/Questions.csv
                            rm ~/airflow/downloads/Answers.csv
                            echo Deleted!'''
        )

        with TaskGroup(group_id='questions_processing') as questions_processing:
            dowload_question_file_task = PythonOperator(
                task_id='dowload_question_file_task',
                python_callable=_dowload_question_file_task
            )

            import_questions_mongo = BashOperator(
                task_id='import_questions_mongo',
                bash_command='''mongoimport --type csv -d asm2 -c questions --headerline --drop /home/airflow/airflow/downloads/Questions.csv'''
            )

            dowload_question_file_task >> import_questions_mongo
        
        with TaskGroup(group_id='answers_processing') as answers_processing:
            dowload_answer_file_task = PythonOperator(
                task_id='dowload_answer_file_task',
                python_callable=_dowload_answer_file_task
            )

            import_answers_mongo = BashOperator(
                task_id='import_answers_mongo',
                bash_command='''mongoimport --type csv -d asm2 -c answers --headerline --drop /home/airflow/airflow/downloads/Answers.csv'''
            )

            dowload_answer_file_task >> import_answers_mongo

        spark_process = SparkSubmitOperator(
            task_id='spark_process',
            application='/home/airflow/airflow/spark_files/basicsparksubmit.py',
            conn_id='spark_local'
        )

        import_output_mongo = BashOperator(
            task_id='import_output_mongo',
            bash_command='''
                for filename in /home/airflow/airflow/output_file/*.csv
                do
                    mongoimport --type csv -d asm2 -c output --headerline ${filename}
                done'''
        )

        clear_file >> [questions_processing, answers_processing] >> spark_process >> import_output_mongo >> end

    
    start >> branching >> [processing, end] 
