from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

SLACK_CONN_ID = 'slack_connection'

def task_fail(context):
        slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
        slack_msg = """
            :red_circle: Task Failed.
            *Task*: {task}
            *Host*: {host}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
            task=context.get('task_instance').task_id,
            host=context.get('task_instance').hostname,
            dag=context.get('task_instance').dag_id,
            ti=context.get('execution_date'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
        failed_alert = SlackWebhookOperator(
            task_id="slack",
            http_conn_id=SLACK_CONN_ID,
            webhook_token=slack_token,
            message=slack_msg,
            # icon_emoji="https://icons.iconarchive.com/icons/google/noto-emoji-smileys/48/10045-downcast-face-with-sweat-icon.png",
            # username="airflow",
        )
        return failed_alert.execute(context=context)


default_args = {
	'owner': 'airflow',
        #'run_as_user':'test',
	'depends_on_past': False,
	'retries':0,
	'retry_delay':timedelta(minutes=1),
        'on_failure_callback':task_fail
}

with DAG(
	'test-job-w',
	default_args=default_args,
	description='A simple DAG',
	max_active_runs=1,
	schedule_interval='*/5 * * * *',
	start_date=days_ago(1),
	catchup = False
	
) as dag:
	
	t1 = BashOperator(
		task_id="test-job",
                bash_command="sudo php /SCRIPTS/BLOOM_FILTER_GENERATION/scripts/bloom_filter_generator_7.php -p 1440 WHITELIST_BLOOM_FILTR ",
		dag=dag,
                execution_timeout=timedelta(minutes=5)
	)

	# slack = SlackAPIPostOperator(
	# 	task_id="post_hello",
	# 	dag=dag,
	# 	token="",
	# 	text="hello there",
	# 	channel="#bloom-demo"
	# )
