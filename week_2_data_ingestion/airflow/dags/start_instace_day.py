import os
from datetime import datetime

from airflow import models
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineSetMachineTypeOperator,
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator


# [START howto_operator_gce_args_common]
GCP_PROJECT_ID = 'data-engineer-339410' #os.environ.get('GCP_PROJECT_ID')
GCE_ZONE = 'us-central1-c'
GCE_INSTANCE = 'ml-pipeline-demand-predict'
# [END howto_operator_gce_args_common]
GCE_SHORT_MACHINE_TYPE_NAME = os.environ.get('e2-standard-4')

with models.DAG(
    'example_gcp_compute',
    schedule_interval='@once',  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # [START howto_operator_gce_start]
    gce_instance_start = ComputeEngineStartInstanceOperator(
        project_id=GCP_PROJECT_ID, 
        zone=GCE_ZONE, 
        resource_id=GCE_INSTANCE,
        #gcp_conn_id='my_gcp_connection',
        task_id='gcp_compute_start_task'
    )

    os_login_without_iap_tunnel = SSHOperator(
        task_id="os_login_without_iap_tunnel",
        ssh_hook=ComputeEngineSSHHook(
            instance_name=GCE_INSTANCE,
            zone=GCE_ZONE,
            project_id=GCP_PROJECT_ID,
            #gcp_conn_id='my_gcp_connection',
            use_oslogin=False,
            use_iap_tunnel=False,
        ),
        command="echo os_login_without_iap_tunnel",
    )

    gce_instance_stop = ComputeEngineStopInstanceOperator(
        project_id=GCP_PROJECT_ID, 
        zone=GCE_ZONE, 
        resource_id=GCE_INSTANCE,
        #gcp_conn_id='my_gcp_connection',
        task_id='gcp_compute_stop_task'
    )
    gce_instance_start >> os_login_without_iap_tunnel >> gce_instance_stop