import os
import gc
import random
import botocore
import boto3
import requests
import multiprocessing
import multiprocessing.dummy

requests.exceptions.ReadTimeout
requests.exceptions.ConnectionError
botocore.exceptions.ClientError

MAX_RETRIES = 10

#s3 = boto.connect_s3()
s3C = boto3.client('s3', endpoint_url='http://radosgw.helium:7480/')

class DirectoryUploader:
    def __init__(self, path, bucketname):
        self.path = path
        self.bucketname = bucketname

    def on_item(self, args):
        root, dirs, files = args
        print(root)
        for file in files:
            for x in range(0, MAX_RETRIES):
                try:
                    self.on_file(root, dirs, file)
                except (requests.exceptions.ReadTimeout, botocore.exceptions.ClientError, requests.exceptions.ConnectionError):
                    print('retrying')
                    continue
                except TypeError:
                    print('TypeError, retrying')
                    continue
                except FileNotFoundError:
                    break
                else:
                    break
            else:
                print('Failed to handle {}'.format(file))
        return root

    def on_file(self, root, dirs, file):
        key = os.path.join(root,file).lstrip('./')
        print('\t' + key)
        try:
            resp = s3C.head_object(Bucket=self.bucketname, Key=key)
        except botocore.exceptions.ClientError:
            resp = None
        #print(repr(resp))
        """
        try:
            if resp is None or resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                s3C.upload_file(key, Bucket=self.bucketname, Key=key)
        finally:
            s3C.put_object_acl(ACL='public-read', Bucket=self.bucketname, Key=key)
        """
        if resp is None or resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            s3C.upload_file(key, Bucket=self.bucketname, Key=key,
                    ExtraArgs={'ACL': 'public-read'})

    def do(self):
        with multiprocessing.Pool(100) as p:
            for dir_ in p.imap(self.on_item, os.walk(self.path)):
                if random.randint(0, 10000):
                    gc.collect()
                print(dir_)
        #for dir_ in map(self.on_item, os.walk(self.path)):
        #     print(dir_)

DirectoryUploader('.', 'octodonfr').do()
