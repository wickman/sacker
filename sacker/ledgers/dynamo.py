import json

from sacker.ledger import Ledger
from sacker.package import Package

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

# hash_key=<jobkey>/release S; range_key=release# N
# hash_key=<jobkey>/config S; range_key=config# N
# get highest hash/range for particular hash key, conditional set new

class DynamoLedger(Ledger):
  @classmethod
  def from_uri(cls, uri):
    uri = urlparse(uri)
    if uri.scheme != 'dynamo':
      raise ValueError('DynamoLedger does not work with %r URIs!' % uri.scheme)
    return cls.from_netloc(uri.netloc, uri.path)

  @classmethod
  def from_netloc(cls, netloc, path):
    if path.startswith('/'):
      path = path[1:]
    return cls(netloc, path)

  def __init__(self, region, table):
    self.region = region
    self.table = table
    self._conn = None

  @property
  def connection(self):
    if self._conn is None:
      self._conn = boto3.resource('dynamodb')
    return self._conn

  @property
  def tags_table(self):
    return self.table + '-tags'

  def init(self):
    # create main table
    attribute_definitions = [
        { 'AttributeName': 'package_name', 'AttributeType': 'S' },
        { 'AttributeName': 'version', 'AttributeType': 'N' },
    ]
    key_schema = [
        { 'AttributeName': 'package_name', 'KeyType': 'HASH' },
        { 'AttributeName': 'version', 'KeyType': 'RANGE' },
    ]
    provisioned_throughput = {
        'ReadCapacityUnits': 1,
        'WriteCapacityUnits': 1
    }
    boto3.session.Session(region_name=self.region).client('dynamodb').create_table(
        TableName=self.table,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        ProvisionedThroughput=provisioned_throughput,
    )

    # create tags table
    attribute_definitions = [
        { 'AttributeName': 'package_name', 'AttributeType': 'S' },
        { 'AttributeName': 'tag', 'AttributeType': 'S' },
    ]
    key_schema = [
        { 'AttributeName': 'package_name', 'KeyType': 'HASH' },
        { 'AttributeName': 'tag', 'KeyType': 'RANGE' },
    ]
    boto3.session.Session(region_name=self.region).client('dynamodb').create_table(
        TableName=self.tags_table,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        ProvisionedThroughput=provisioned_throughput,
    )

  def list_packages(self):
    def iter_packages():
      kw = {}
      while True:
        response = self.connection.Table(self.table).scan(**kw)
        for item in response['Items']:
          yield item['package_name']
        if 'LastEvaluatedKey' in response:
          kw['ExclusiveStartKey'] = response['LastEvaluatedKey']
        else:
          break
    return list(set(iter_packages()))

  def list_package_versions(self, package_name):
    response = self.connection.Table(self.table).query(
        KeyConditionExpression=Key('package_name').eq(package_name))
    for item in response['Items']:
      yield int(item['version'])

  def add(self, package_name, basename, sha, mode, metadata=None):
    latest = self.latest(package_name)
    new_latest = latest + 1 if latest is not None else 1

    try:
      resp = self.connection.Table(self.table).put_item(
          Item={
              'package_name': package_name,
              'version': new_latest,
              'basename': basename,
              'sha': sha,
              'mode': mode,
              'metadata': json.dumps(metadata),
          },
          ConditionExpression=Attr('package_name').ne(package_name) & Attr('version').ne(new_latest)
      )
    except ClientError as e:
      raise self.Error('Failed to add version, possible dynamo race condition: %s' % e)

    return new_latest

  def remove(self, package_name, generation):
    raise NotImplementedError

  def latest(self, package_name):
    versions = sorted(self.list_package_versions(package_name))
    return max(versions) if versions else None

  def _get_version(self, package_name, spec):
    if spec == 'latest':
      return self.latest(package_name)
    try:
      return int(spec)
    except ValueError:
      return self._get_tag(package_name, spec)

  def info(self, package_name, spec):
    generation = self._get_version(package_name, spec)
    if generation is None:
      raise self.DoesNotExist('Package %s has no version %s' % (package_name, spec))
    resp = self.connection.Table(self.table).get_item(
        Key={'package_name': package_name, 'version': generation})
    if 'Item' not in resp:
      raise self.DoesNotExist('Package %s has no version %s' % (package_name, spec))
    item = resp['Item']
    return Package(
        package_name,
        generation,
        item['sha'],
        item['basename'],
        item['mode'],
        json.loads(item['metadata']),
    )

  def _get_tag(self, package_name, tag_name):
    resp = self.connection.Table(self.tags_table).get_item(
        Key={'package_name': package_name, 'tag': tag_name}
    )
    if 'Item' in resp:
      return int(resp['Item']['version'])
    else:
      raise self.Error('Tag %s does not exist for %s' % (tag_name, package_name))

  def tag(self, package_name, generation, tag_name):
    if tag_name == 'latest':
      raise self.Error('Cannot alter dynamic tag "latest" for Dynamo ledger.')
    self.connection.Table(self.tags_table).put_item(
        Item={
            'package_name': package_name,
            'tag': tag_name,
            'version': int(generation),
        },
    )

  def untag(self, package_name, tag_name):
    if tag_name == 'latest':
      raise self.Error('Cannot alter dynamic tag "latest" for Dynamo ledger.')
    self.connection.Table(self.tags_table).delete_item(
        Key={
            'package_name': package_name,
            'tag': tag_name,
        },
    )

  def tags(self, package_name):
    resp = self.connection.Table(self.tags_table).query(
        KeyConditionExpression=Key('package_name').eq(package_name))
    latest = self.latest(package_name)
    if latest is not None:
      yield 'latest'
    for item in resp['Items']:
      yield item['tag']
