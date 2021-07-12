import airflow 
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
import pendulum


local_tz = pendulum.timezone("America/Los_Angeles")

SLACK_CONN_ID = 'slack_webhook'

def task_fail_slack_alert(context):
    slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
        :red_circle: Task Failed.
        *Task*: {task}
        *Dag*: {dag}
        *Execution Time*: {exec_date}
        *Log Url*: {log_url}
        """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id, 
        ti=context.get('execution_date'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url
    )
    failed_alert = SlackWebhookOperator(
        task_id='slack',
        http_conn_id=SLACK_CONN_ID  ,
        webhook_token=slack_token, 
        message=slack_msg
    )
    return failed_alert.execute(context=context)

default_args = {
	'owner' : 'airflow',
	#'run_as_user' : 'some_user',								#in case we want to execute some non-root/user specific tasks
	'depends_on_past' : False,
	'retries' : 1, 												#get this value
	'retry_delay' : timedelta(minutes=1), 						#get this value
	'on_failure_callback' : task_fail_slack_alert
}

with DAG(
	'whitelist',													#change the dag_id
	default_args=default_args,
	#max_active_runs=1,
	catchup=False,
	schedule_interval="0 2 * * *",
	#start_date=datetime(2021, 05, 18, 17, 0, tzinfo=local_tz),	#change start_date such that execution date = start_date + schdule_interval
	start_date=days_ago(1),
        tags = ['blooms', 'whitelist']								#change whitelist to the filter name
) as dag:
	command_to_run="php /SCRIPTS/BLOOM_FILTER_GENERATION/scripts/bloom_filter_generator_7.php -p 1440 WHITELIST_BLOOM_FILTER "
	bash_task = BashOperator(
		task_id="whitelist-filter",									#change the task_id
		bash_command=command_to_run,
		dag=dag,
		execution_timeout=timedelta(minutes=10)					#get this value
	)
