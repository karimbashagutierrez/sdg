from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# ETL DAG
dag = DAG(
    'ETL_SDG',
    description='ETL to validate metadata dynamically ',
    schedule_interval=None,
    start_date=datetime(2024, 6, 15),
    catchup=False
)

# Start cluster spark hadoop
start_cluster_task = BashOperator(
    task_id='start_cluster_task',
    bash_command='docker start hadoop-master hadoop-slave1 hadoop-slave2',
    dag=dag
)

# Start cluster kafka
start_kafka_task = BashOperator(
    task_id='start_kafka_task',
    bash_command='docker start zookeeper broker',
    dag=dag
)

# Push data into hdfs
push_files_task = BashOperator(
    task_id='push_files_task',
    bash_command='hdfs dfs -put /data/metadata.json /data/metadata.json && hdfs dfs -put /data/input/events/person/person_inputs.json /data/input/events/person/person_inputs.json',
    dag=dag
)

# Submit spark application
spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    application ='sdg_2.12-0.1.0-SNAPSHOT.jar',
    conn_id= 'spark_local',
    dag=dag)

# Pull data from hdfs
get_files_task = BashOperator(
    task_id='get_files_task',
    bash_command='hdfs dfs -get /data/output/discards/person/raw_ko /data/output/discards/person/raw_ko',
    dag=dag
)

# Stop cluster spark hadoop
stop_cluster_task = BashOperator(
    task_id='stop_cluster_task',
    bash_command='docker stop hadoop-master hadoop-slave1 hadoop-slave2',
    dag=dag
)

# Stop cluster kafka
stop_kafka_task = BashOperator(
    task_id='stop_kafka_task',
    bash_command='docker stop zookeeper broker',
    dag=dag
)


# Define the task dependencies
start_cluster_task >> start_kafka_task >> push_files_task >> spark_submit_task >> get_files_task >> stop_cluster_task >> stop_kafka_task