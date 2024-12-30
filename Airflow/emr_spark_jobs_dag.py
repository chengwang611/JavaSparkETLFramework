from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrJobFlowSensor, EmrStepSensor
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# AWS EMR Cluster Configuration
JOB_FLOW_OVERRIDES = {
    "Name": "Airflow-EMR-Spark-Jobs",
    "ReleaseLabel": "emr-6.5.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {"Name": "Master nodes", "Market": "ON_DEMAND", "InstanceRole": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1},
            {"Name": "Core nodes", "Market": "ON_DEMAND", "InstanceRole": "CORE", "InstanceType": "m5.xlarge", "InstanceCount": 2},
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

# Spark Step Configuration
SPARK_STEPS = [
    {
        "Name": "Spark Job 1",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--class", "com.example.spark.Job1",
                "s3://<your-bucket-name>/path/to/job1.jar",
                "--input", "s3://<your-bucket-name>/path/to/input1",
                "--output", "s3://<your-bucket-name>/path/to/output1",
            ],
        },
    },
    {
        "Name": "Spark Job 2",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--class", "com.example.spark.Job2",
                "s3://<your-bucket-name>/path/to/job2.jar",
                "--input", "s3://<your-bucket-name>/path/to/input2",
                "--output", "s3://<your-bucket-name>/path/to/output2",
            ],
        },
    },
]

# Default arguments for the DAG
DEFAULT_ARGS = {
    "Owner": "airflow",
    "DependsOnPast": False,
    "Retries": 1,
}

# Define the DAG
with DAG(
        dag_id="emr_spark_jobs_dag",
        default_args=DEFAULT_ARGS,
        description="Trigger multiple Spark batch jobs on AWS EMR",
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False,
) as dag:

    start_task = DummyOperator(task_id="start")

    # Create EMR Cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )

    # Wait for Cluster to be Ready
    wait_for_cluster = EmrJobFlowSensor(
        task_id="wait_for_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    )

    # Add Spark Steps
    add_spark_steps = EmrAddStepsOperator(
        task_id="add_spark_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=SPARK_STEPS,
    )

    # Wait for Spark Steps to Complete
    last_step = len(SPARK_STEPS) - 1
    wait_for_steps = EmrStepSensor(
        task_id="wait_for_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[%d] }}" % last_step,
    )

    # End Task
    end_task = DummyOperator(task_id="end")

    # DAG Task Dependencies
    start_task >> create_emr_cluster >> wait_for_cluster >> add_spark_steps >> wait_for_steps >> end_task
