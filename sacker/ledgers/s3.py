import json
import os

from sacker.ledger import Ledger
from sacker.package import Package

import boto3


# TODO(wickman):
#   - Document the case where latest version is removed and re-added.
#   - Document race conditions.
#   - If this concerns people, provide Zookeeper ledger with stronger consistency.

class S3Ledger(Ledger):
  PAGE_SIZE = 100
  TAG_SEPARATOR = 'tags'
  GENERATION_SEPARATOR = 'generations'

  @classmethod
  def get_name(cls, key):
    skey = key.split('/')
    assert len(skey) > 2
    return '/'.join(skey[:-2])

  @classmethod
  def from_netloc(cls, netloc, path):
    if path not in ('', '/'):
      raise ValueError('S3 ledger does not take a path.')
    return cls(netloc)

  def __init__(self, bucket_name):
    self.bucket_name = bucket_name

  def list_packages(self):
    bucket = boto3.resource('s3').Bucket(self.bucket_name)
    encountered_packages = set()
    for obj in bucket.objects.page_size(self.PAGE_SIZE):
      name = self.get_name(obj.key)
      if name not in encountered_packages:
        yield name
        encountered_packages.add(name)

  # TODO(wickman) More input validation
  def list_package_versions(self, package_name):
    bucket = boto3.resource('s3').bucket(self.bucket_name)
    object_iterator = bucket.objects.filter(
        Prefix='%s/generations/' % package_name).page_size(self.PAGE_SIZE)
    for obj in object_iterator:
      yield int(obj.key.split('/')[-1])

  def add(self, package_name, filename, sha, mode, metadata=None):
    try:
      version = max(self.list_package_versions())
    except ValueError:
      version = 0
    json_blob = {
        'sha': sha,
        'basename': os.path.basename(filename),
        'mode': os.stat(filename).st_mode,
    }
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=self.bucket_name,
        Key='%s/generations/%s' % (package_name, version + 1),
        Metadata=metadata or {},
        Body=json.dumps(json_blob)
    )

  def remove(self, package_name, generation):
    pass

  def info(self, package_name, generation):
    package_info = (boto3.resource('s3')
        .Bucket(self.bucket_name)
        .Object('%s/generations/%s' % (self.package_name, generation))).get()
    package_content = json.loads(package_info['Body'].read())
    return Package(
        package_name,
        generation,
        package_content['sha'],
        package_content['basename'],
        package_content['mode'],
        package_info['Metadata'],
    )

  def tag(self, package_name, generation, tag_name):
    pass

  def untag(self, package_name, tag_name):
    pass

  def tags(self, package_name):
    pass
