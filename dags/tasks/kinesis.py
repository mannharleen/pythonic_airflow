import boto3, base64

session = boto3.session.Session(profile_name='oesa')
client = session.client('kinesis')


def write_stream(msg):
    response = client.put_record(
    StreamName='ks1',
    Data= msg,
    #Data=b'msg3',
    PartitionKey='4'
    )
    print("Message written to the KS: ") # + str(response))


def read_stream():
    shard_iterator = client.get_shard_iterator(
        StreamName='ks1',
        ShardId='shardId-000000000000',
        ShardIteratorType='TRIM_HORIZON'
        #ShardIteratorType='AT_SEQUENCE_NUMBER',
        #StartingSequenceNumber='49586666151439815139779153182322027439584478037943517186'
    )['ShardIterator']
    response = client.get_records(
        ShardIterator=shard_iterator,
        Limit=999
    )

    data_read = list(map(lambda x: x['Data'], response['Records']))
    print(data_read)


if __name__ == '__main__':
    #write_stream(msg='msg4')
    read_stream()