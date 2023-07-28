import os
import boto3
from botocore.waiter import WaiterModel
from botocore.waiter import create_waiter_with_client
from botocore.exceptions import WaiterError


def get_waiter_config(waiter_name):
    delay = int(os.environ.get('REDSHIFT_QUERY_DELAY'))
    max_attempts = int(os.environ.get('REDSHIFT_QUERY_MAX_ATTEMPTS'))

    #Configure the waiter settings
    waiter_config = {
      'version': 2,
      'waiters': {
        'DataAPIExecution': {
          'operation': 'DescribeStatement',
          'delay': delay,
          'maxAttempts': max_attempts,
          'acceptors': [
            {
              "matcher": "path",
              "expected": "FINISHED",
              "argument": "Status",
              "state": "success"
            },
            {
              "matcher": "pathAny",
              "expected": ["PICKED","STARTED","SUBMITTED"],
              "argument": "Status",
              "state": "retry"
            },
            {
              "matcher": "pathAny",
              "expected": ["FAILED","ABORTED"],
              "argument": "Status",
              "state": "failure"
            }
          ],
        },
      },
    }
    return waiter_config


def get_redshift_waiter_client(rsd_client):
    waiter_name = 'DataAPIExecution'

    waiter_config = get_waiter_config(waiter_name)
    waiter_model = WaiterModel(waiter_config)
    return create_waiter_with_client(waiter_name, waiter_model, rsd_client)


def copy_s3_to_rstable(bucket_name, table_name):
    rsd_client = boto3.client('redshift-data')
    rs_copy_command = f'''
        COPY {table_name} FROM 's3://{bucket_name}/transformed-data' 
        IAM_ROLE 'arn:aws:iam::169592149406:role/service-role/S3ToRedshift-MarioGalaxy2-role-9qkhs916'
        CSV 
        IGNOREHEADER 1
    '''
    rs_copy_command_id = rsd_client.execute_statement(
        WorkgroupName='galaxy2-dw',
        Database='dev',
        #SecretArn=secret_arn,
        Sql=rs_copy_command
    )['Id']
    custom_waiter = get_redshift_waiter_client(rsd_client)
    try:
        custom_waiter.wait(Id=rs_copy_command_id)    
    except WaiterError as e:
        print (e)
    return rsd_client.describe_statement(Id=rs_copy_command_id)['Status']

def lambda_handler(event, context):
    # TODO implement
    try:
        bucket_name = event['Bucket']
        table_name = event['TableName']
        copy_res = copy_s3_to_rstable(bucket_name, table_name)
    except:
        raise
    return {
        'statusCode': 200,
        'statementStatus': copy_res
    }