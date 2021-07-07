import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor


default_args = {
    "owner": "bazaar-admin",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(1),
}

dag = DAG("bazaar", default_args=default_args, schedule_interval=timedelta(1))


aws_hook = AwsHook(aws_conn_id=AWS_CONNECTION_ID)
emr_client = aws_hook.get_client_type('emr')

response = emr_client.list_clusters(ClusterStates=['RUNNING', 'WAITING'])

matching_clusters = list(
    filter(lambda cluster: cluster['Name'] == EMR_CLUSTER_NAME, response['Clusters'])
)

if len(matching_clusters) == 1:
    cluster_id = matching_clusters[0]['Id']
else:
# handle the error
    def run_spark_job():
        # here is the code that retrieves the cluster id

        step_args = f'spark-submit --master yarn --deploy-mode client ' \
                    f'--class name.mikulskibartosz.SparkJob ' \
                    f's3://bucket_name/spark_job.jar ' \
                    f'--job_parameter something'


        step_configuration = {
            'Name': 'step_name',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': step_args
            }
        }
        step_ids = emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step_configuration])
        return cluster_id, step_ids['StepIds'][0]


    wait_for_it = EmrStepSensor(
        task_id='wait_for_it',
        job_flow_id="{{ task_instance.xcom_pull('run_emr_step_task', key='return_value')[0] }}",
        step_id="{{ task_instance.xcom_pull('run_emr_step_task', key='return_value')[1] }}",
        dag=dag)





