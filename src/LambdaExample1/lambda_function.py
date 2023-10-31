import json
import hashlib
import boto3
from oscar import Oscar
from oscar import LambdaTimeoutApproaching
import ssl
import random
recallQueuesMaster=[]
maxTTL=4

def lambda_handler(event, context):
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
        sendSQS(json.dumps(toExport), inQueue)
        return {
          'statusCode': 200,
          'body': json.dumps(t.export)
        }
      else:
        return {
          'statusCode': 200,
          'body': json.dumps("TTL EXCEEDED")
        }
    
    return {
        'statusCode': 200,
        'body': json.dumps(t.getDigests())
    }

def sendSQS(export, qIn):
  recallQueues=recallQueuesMaster.copy().remove(qIn.split(':')[-1])
  sqs = boto3.resource('sqs')
  queue = sqs.get_queue_by_name(random.choice(recallQueues))
  print(queue)
  response = queue.send_message(MessageBody=export)
  
