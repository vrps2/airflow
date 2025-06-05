import json


def triggerLambda(name,job_id,payload):

    payload_json=json.dumps(payload)
    retry_count=1

    logging.info("Inside trigger Lambda function")
    client=boto3.client('lambda',region_name='us-east-1')
    session=boto3.Session()


    while retry_count <4:
        try:
            response=client.invoke(FunctionName=name,
                                   InvocationType='RequestResponse',
                                   Payload=payload_json
                                   )
            print("response:",response)

            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                logging.info(f"Lambda invocation failed. Retrying ({retry_count}/3) in 60 secs")
            else:
                logging.info("Lambda invocation successful")

                application_response= response['Payload'].read()   #Response of applicaton/logic in lambda
                application_response_to_string=application_response.decode('utf-8')  #Convert lambda response from bytes to string
                application_response_to_json= json.loads(application_response_to_string) #convert lambda response from string to json
                response.pop('Payload')
                final_response={'Payload':application_response_to_json}
                final_response.update(response)

                print('Lambda final response:',final_response)

                try:{
                    if final_response['Payload']['statusCodeValue']==200:




                }
