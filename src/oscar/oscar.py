import boto3
from botocore.exceptions import ClientError
import hashlib
import rehash
import pickle
import base64
import time
import json

class LambdaTimeoutApproaching(Exception):
    "Raised when execution time exceed threshold indicating Lambda function is about to time out."
    pass

class Hasher:
  def __init__(self, bucket, key):
    self.bucket=bucket
    self.key=key
    self.size=-1
    self.parts=-1
    self.cursor=0
    self.useParts=False
    self.object=None
    self.BLOCKSIZE = 65536
    self.BLOCKSIZE = 65000000
    self.startTime = time.time()
    self.maxRunTime=60
    self.export=None
    self.startPart=1
    self.test=False
    self.ETag=None
    self.ETagRunningHash=None
    self.ETagPartHash=rehash.md5()

    self.hashes=[rehash.sha512(), rehash.sha256(), rehash.sha1(), rehash.md5()]
    self.hashes=[rehash.md5()]
    self.ETagRunningHash=rehash.md5()
    self.partsCutoff=0
    self.setup()


  def getElapsedTime(self):
    return int(time.time()-self.startTime)

  def setup(self):
       ###handle failed auth
    self.s3=boto3.client('s3')
    self.getObjectAtts()
    self.object=self.s3.get_object( Bucket=self.bucket, Key=self.key)


  def start(self):
    if (self.parts > 0):
      self.getObjectParts()
    else:
      self.getObject()


  def startParts(self):
    self.getObjectParts()

  def resume(self, dump):
    resumeData=json.loads(base64.b64decode(dump))
    self.bucket=resumeData['bucket']
    self.key=resumeData['key']
    self.startPart=resumeData['part']
    self.ETagRunningHash=pickle.loads(base64.b64decode(resumeData['ETagRH']))
    for hash in resumeData['hashes']:
      self.hashes.append(pickle.loads(base64.b64decode(hash)))
    pass

  def dump(self, part):
    tmp={'bucket':self.bucket, 'key': self.key, 'part':part, 'hashes':[]}
    for hash in self.hashes:
      tmp['hashes'].append(base64.b64encode(pickle.dumps(hash)).decode("utf-8"))
    tmp['ETagRH']=base64.b64encode(pickle.dumps(self.ETagRunningHash)).decode("utf-8")   
    self.export=json.dumps({'bucket':self.bucket, 'key':self.key,  'state':base64.b64encode(bytes(json.dumps(tmp),'utf-8')).decode("utf-8")})

  def checkBucketandKey(self):
    pass


  def getObject(self):
    self.ETagPartHash=rehash.md5()
    for block in self.readBlocks():
      self.cursor=self.cursor+len(block)
      for m in self.hashes:
        m.update(block)
      self.ETagPartHash.update(block)

  def readBlocks(self):
    stream=self.object['Body']._raw_stream
    while rBytes := stream.read(self.BLOCKSIZE):
      yield rBytes


  def getObjectParts(self):
    for part in range(self.startPart, self.parts+1):
#      print("Part: "+str(part))
#      print( self.getElapsedTime())
###TODO: Check the part size vs estimated speed and time left
      if (self.getElapsedTime() > self.maxRunTime):
        pass
        self.dump(part)
        raise LambdaTimeoutApproaching
#        print("NOPE-IN OUT!!")
        break
      else:
        self.object=self.s3.get_object( Bucket=self.bucket, Key=self.key, PartNumber=(part))
        self.getObject()
        self.ETagRunningHash.update(self.ETagPartHash.digest())
#        print('-------')
#        print(self.ETagRunningHash.hexdigest())
#        print(self.ETagPartHash.hexdigest())

  def getDigests(self):
    toRet={}
    for m in self.hashes:
      toRet[m.name] = m.hexdigest()
    return toRet


  def verify_bucket(self):
    pass

  def verify_object_key(self):
    response = self.s3.list_objects_v2(Bucket=bucket)
    for i in response:
      print(i)

  def getObjectAtts(self):
    atts = self.s3.get_object_attributes( Bucket=self.bucket, Key=self.key, ObjectAttributes=['ETag','Checksum','ObjectParts','StorageClass','ObjectSize'])
    if 'ObjectParts' in atts:
      self.parts=int(atts['ObjectParts']['TotalPartsCount'])
    else:
      self.parts=0
    self.size=int(atts['ObjectSize'])
    self.ETag=atts['ETag']

  def verify_etag(self):
    if self.parts > 0:
      calculated=self.ETagRunningHash.hexdigest()+"-"+str(self.parts)
    else:
      pass
      calculated=self.ETagPartHash.hexdigest()
    if calculated == self.ETag:
      return True
    else:
      return False
