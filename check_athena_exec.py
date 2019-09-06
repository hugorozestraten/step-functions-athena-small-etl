import boto3

client = boto3.client('athena')


def lambda_handler(event, context):
    evento=str(event)
    print('log {} '.format(evento))
    try:
            queryid=evento
            print(queryid)
            response = client.get_query_execution(
                QueryExecutionId=queryid
            )
            resposta=response['QueryExecution']['Status']['State']
            return(resposta)
    except Exception as e:
            print(e)
            print('error on get response for query_id {}'.format(event['payload']))
