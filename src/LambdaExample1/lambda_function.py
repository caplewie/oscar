import json
import hashlib
import boto3
from hasher import Oscar
from hasher import LambdaTimeoutApproaching
import ssl
import random
import time
maxTTL=4
##Add 2 queues to write to. Using one queue will be blocked by AWS
recallQueuesMaster=[]

def lambda_handler(event, context):
    recallQueues=recallQueuesMaster.copy()
    if len(event['Records']) > 1:
      return {'statusCode':404, 'body':json.dumps('too many events')}
    if event['Records'][0]["eventSource"] == "aws:s3":
      s3Data=event['Records'][0]['s3']
      bucket=s3Data['bucket']['name']
      key=s3Data['object']['key']
      ttl=maxTTL
      t = Oscar(bucket, key)
    elif event['Records'][0]["eventSource"] == "aws:sqs":
      inQueue=event['Records'][0]['eventSourceARN']
      restoreData=json.loads(event['Records'][0]['body'])
      bucket=restoreData['bucket']
#      recallQueues.remove(inQueue.split(':')[-1])
      recallQueues.remove(inQueue.split(':')[-1])
      ttl=int(restoreData['TTL'])
      key=restoreData['key']
      restoreState=restoreData['state']
      t = Oscar(bucket, key)
      t.resume(restoreState)
    try:
      t.start()
      print(t.getDigests())
      t.verify_etag()
    except LambdaTimeoutApproaching:
      print(t.export)
      if ttl > 0:
        toExport=json.loads(t.export)
        toExport['TTL'] = ttl-1

        sendSQS(json.dumps(toExport), random.choice(recallQueues))
        return {
          'statusCode': 200,
          'body': json.dumps(t.export)
        }
      else:
        return {
          'statusCode': 200,
          'body': json.dumps("TTL EXCEEDED")
        }
    writeHashFile(key, t.getDigests())
    return {
        'statusCode': 200,
        'body': json.dumps(t.getDigests())
    }

def sendSQS(export, q):
  sqs = boto3.resource('sqs')
  queue = sqs.get_queue_by_name(QueueName=q)
#  print(queue)
  response = queue.send_message(MessageBody=export)
  
def writeHashFile(filename, hashes):
  bucket="laneslambdatestbucket"
  s3 = boto3.resource('s3')
  object = s3.Object(bucket, 'hashes/'+filename+"-"+str(int(time.time()))+'.hash')
#  object.put(Body=json.dumps(hashes).encode('utf-8'), ObjectLockMode='GOVERNANCE', ObjectLockRetainUntilDate=datetime(2023, 12, 9))
  object.put(Body=json.dumps(hashes).encode('utf-8'))
#  object.put(Body='aaaaa')
