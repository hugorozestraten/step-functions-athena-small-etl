import boto3
import time

params = {
    'region': 'us-east-1',
    'database': 'mydatabase',
    'bucket': 'mybucket',
    'path': 'athena_query_exec/regularoutput',
    'deletebucket': 'tablebucket',
    'deletelocation': 'tablelocation/',
    'query': 'SELECT * FROM databasename.tablename LIMIT 100',
    'drop':'DROP TABLE databasename2.tablename2'
}

session = boto3.Session()
sessiondrop = boto3.Session()
s3 = session.resource("s3")


def athena_query(client, params):

    response = client.start_query_execution(
        QueryString=params["query"],
        QueryExecutionContext={
            'Database': params['database']
        },
	ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )
    return response


def lambda_handler(event, context):
    if 'query' in event:
        #print(event)
        params['database']=event['database']
        params['bucket']=event['bucket']
        params['path']=event['path']
        params['query']=event['query']
        if 'deletebucket' in event:
            params['deletebucket']=event['deletebucket']
            params['deletelocation']=event['deletelocation']
        if 'drop' in event:
            params['drop']=event['drop']
        params['region']=event['region']
    if 'drop' in params:
        try:
            clientdrop = session.client('athena', region_name=params["region"])
            response = clientdrop.start_query_execution(
                QueryString=params["drop"],
                QueryExecutionContext={
                    'Database': params['database']
                },
        	ResultConfiguration={
                    'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
                }
            )
            time.sleep(2)
        except:
            print('error on drop tables.')
    if 'deletebucket' in params:
        try:
            deletebucket=params['deletebucket']
            deletelocation=params['deletelocation']
            bucket = s3.Bucket(deletebucket)
            bucket.objects.filter(Prefix=deletelocation).delete()
        except:
            print('there was a problem trying to delete your path')
    try:
        client = session.client('athena', region_name=params["region"])
        response=athena_query(client, params)
        resp=response
        #print(resp)
        resposta=response['QueryExecutionId']
        print('finished your query: {} against: {} database in region {} the output is in {}/{}'.format(params['query'],params['database'],params['region'],params['bucket'],params['path']))
        #print(resposta)
        return(resposta)
    except:
        print('there was a problem executing your  query: {} against: {} database in region {} with output to {}/{}'.format(params['query'],params['database'],params['region'],params['bucket'],params['path']))
