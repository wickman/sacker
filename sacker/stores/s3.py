from ..store import Store

from boto3.s3.transfer import S3Transfer


# TODO(wickman) error handling
class S3Store(Store):
  def __init__(self, bucket, connection):
    self.bucket = bucket
    self.connection = self.connection

  def upload(self, sha, filename):
    transfer = S3Transfer(self.connection)
    transfer.upload_file(filename, self.bucket, sha)

  def download(self, sha, filename):
    transfer = S3Transfer(self.connection)
    transfer.download_file(self.bucket, sha, filename)

  def delete(self, sha):
    self.connection.delete(Bucket=self.bucket, Key=sha)
