from ..store import Store

import boto3
from boto3.s3.transfer import S3Transfer


# TODO(wickman) error handling
class S3Store(Store):
  @classmethod
  def from_netloc(cls, netloc, path):
    if path not in ('', '/'):
      raise ValueError('S3 store does not take path.')
    return cls(netloc)

  def __init__(self, bucket):
    self.bucket = bucket

  @property
  def connection(self):
    return boto3.client('s3')

  def upload(self, sha, filename):
    transfer = S3Transfer(self.connection)
    transfer.upload_file(filename, self.bucket, sha)

  def download(self, sha, filename):
    transfer = S3Transfer(self.connection)
    transfer.download_file(self.bucket, sha, filename)

  def delete(self, sha):
    self.connection.delete(Bucket=self.bucket, Key=sha)
