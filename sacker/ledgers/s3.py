import json
import os
import time

from sacker.ledger import Ledger
from sacker.package import Package

import boto3
from botocore.exceptions import ClientError


class S3Ledger(Ledger):
  """Ledger based on S3, which is compatible with write-only clients e.g. CI"""

  PAGE_SIZE = 100
  TAG_SEPARATOR = 'tags'
  GENERATION_SEPARATOR = 'generations'

  @classmethod
  def from_uri(cls, uri):
    uri = urlparse(uri)
    if uri.scheme != 's3':
      raise ValueError('S3Ledger does not work with %r URIs!' % uri.scheme)
    return cls.from_netloc(uri.netloc, uri.path)

  @classmethod
  def from_netloc(cls, netloc, path):
    if path not in ('', '/'):
      raise ValueError('S3 ledger does not take a path.')
    return cls(netloc)

  def __init__(self, bucket_name):
    self.bucket_name = bucket_name

  def init(self):
    boto3.client('s3').create_bucket(Bucket=self.bucket_name)

  def list_packages(self):
    bucket = boto3.resource('s3').Bucket(self.bucket_name)
    # Restrict to packages with linked 'latest' tags.
    latest_suffix = '/%s/latest' % self.TAG_SEPARATOR
    for obj in bucket.objects.page_size(self.PAGE_SIZE):
      if obj.key.endswith(latest_suffix):
        yield obj.key[:-len(latest_suffix)]

  # TODO(wickman) More input validation
  def list_package_versions(self, package_name):
    bucket = boto3.resource('s3').Bucket(self.bucket_name)
    object_iterator = bucket.objects.filter(
        Prefix='%s/%s/' % (package_name, self.GENERATION_SEPARATOR)
    ).page_size(self.PAGE_SIZE)
    for obj in object_iterator:
      yield int(obj.key.split('/')[-1])

  def _make_timestamp(self):
    # micro-ts
    return int(time.time() * 1000)

  def add(self, package_name, filename, sha, mode, metadata=None):
    json_blob = {
        'sha': sha,
        'basename': os.path.basename(filename),
        'mode': mode,
    }
    s3 = boto3.client('s3')
    timestamp = self._make_timestamp()
    s3.put_object(
        Bucket=self.bucket_name,
        Key='%s/%s/%s' % (package_name, self.GENERATION_SEPARATOR, timestamp),
        Metadata=metadata or {},
        Body=json.dumps(json_blob)
    )
    self.tag(package_name, timestamp, 'latest')
    return timestamp

  def remove(self, package_name, generation):
    pass

  def _resolve_tag(self, package_name, tag_name):
    try:
      tag_info = (boto3.resource('s3')
          .Bucket(self.bucket_name)
          .Object('%s/%s/%s' % (package_name, self.TAG_SEPARATOR, tag_name))).get()
    except ClientError:
      raise self.DoesNotExist('Package %s has no tag %r' % (package_name, tag_name))
    tag_info = json.loads(tag_info['Body'].read())
    return tag_info['version']

  def _get_version(self, package_name, spec):
    try:
      return int(spec)
    except ValueError:
      return self._resolve_tag(package_name, spec)

  def latest(self, package_name):
    try:
      return self._resolve_tag(package_name, 'latest')
    except ValueError:
      return None

  def info(self, package_name, spec):
    generation = self._get_version(package_name, spec)
    try:
      package_info = (boto3.resource('s3')
          .Bucket(self.bucket_name)
          .Object('%s/%s/%s' % (package_name, self.GENERATION_SEPARATOR, generation))
      ).get()
    except ClientError:
      raise self.DoesNotExist('Package %s has no version %d' % (package_name, generation))
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
    if '/' in tag_name:
      raise self.Error('S3 ledger does not support "/" in tag names.')
    json_blob = {'version': generation}
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=self.bucket_name,
        Key='%s/%s/%s' % (package_name, self.TAG_SEPARATOR, tag_name),
        Body=json.dumps(json_blob)
    )

  def untag(self, package_name, tag_name):
    if '/' in tag_name:
      raise self.Error('S3 ledger does not support "/" in tag names.')
    s3 = boto3.client('s3')
    s3.delete_object(
        Bucket=self.bucket_name,
        Key='%s/%s/%s' % (package_name, self.TAG_SEPARATOR, tag_name),
    )

  def tags(self, package_name):
    bucket = boto3.resource('s3').Bucket(self.bucket_name)
    object_iterator = bucket.objects.filter(
        Prefix='%s/%s/' % (package_name, self.TAG_SEPARATOR)
    ).page_size(self.PAGE_SIZE)
    for obj in object_iterator:
      yield obj.key.split('/')[-1]
