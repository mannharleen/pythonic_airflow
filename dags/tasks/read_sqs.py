import boto3

session = boto3.session.Session(profile_name='oesa')
client = session.client('sqs')


def send_sqs_msg_batch():
    queue_url = client.get_queue_url(QueueName='sqs1')['QueueUrl']
    response = client.send_message_batch(
        QueueUrl = queue_url,
        Entries=[
            {'Id': '1', 'MessageBody': 'msg1'},
            {'Id': '2', 'MessageBody': 'msg2'},
            {'Id': '3', 'MessageBody': 'msg3'},
            {'Id': '4', 'MessageBody': 'msg4'},
            {'Id': '5', 'MessageBody': 'msg5'},
        ]
    )

    if response.get('Failed') == None:
        print('All messages send successfully')
    else:
        id_error = list(map(lambda x: x['Id'], response['Failed']))
        print('Following messages were not sent successfully' + str(id_error))

def read_sqs_msg():
    queue_url = client.get_queue_url(QueueName='sqs1')['QueueUrl']
    response = client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)

    if response.get('Messages') != None:
        l_messages_with_extras = response['Messages']
        l_messages = list(map(lambda x: x['Body'], l_messages_with_extras))
        print(l_messages)
        l_receipt_handle = list(map(lambda x: x['ReceiptHandle'], l_messages_with_extras))
        for rh in l_receipt_handle:
            print ('deleting message in sqs {}') #.format(rh))
            client.delete_message(QueueUrl=queue_url, ReceiptHandle=rh)
    else:
        print ('no messages received')

if __name__ == '__main__':
    send_sqs_msg_batch()
    read_sqs_msg()

## sqs
# Order is not maintained
# Duplication? can happen if delete msg doesnt reach the sqs
