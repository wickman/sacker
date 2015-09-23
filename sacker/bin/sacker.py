import base64
import hashlib

from boto3.session import Session
from boto3.s3.transfer import S3Transfer


class SackerDataStore(object):
  class Error(Exception): pass
  class ObjectExists(Error): pass
  class ObjectDoesNotExist(Error): pass

  def upload(self, sha, filename):
    """returns sha, raises ObjectExists"""
    raise NotImplemented
  
  def delete(self, sha):
    """returns nothing, raises ObjectDoesNotExist"""
    raise NotImplemented


class ChainedStore(SackerDataStore):
  def __init__(self, stores):
    self.stores = stores
  
  # TODO(wickman) figure out recovery semantics
  def upload(self, sha, filename):
    for store in self.stores:
      store.upload(sha, filename)
  
  def delete(self, sha):
    for store in self.stores:
      store.delete(sha)


class SackerS3Store(SackerDataStore):
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


def compute_hash(filelike, chunksize=65536, hasher=hashlib.sha256):
  hash = hasher()

  while True:
    data = filelike.read(chunksize)
    if data:
      hash.update(data)
    else:
      break
  
  return hash.hexdigest()


class Package(object):
  def __init__(self, name, generation, sha, basename, mode, metadata=None):
    self.name, self.generation, self.sha, self.basename, self.mode, self.metadata = (
        name, generation, sha, basename, mode, metadata or {})


# TODO(wickman):
#   - Document the case where latest version is removed and re-added.
#   - Document race conditions.
#   - If this concerns people, provide Zookeeper ledger with stronger consistency.

class Ledger(object):
  def list_packages(self):
    pass
  
  def list_package_versions(self, package_name):
    pass
  
  def add(self, package_name, basename, sha, metadata=None):
    pass
  
  def remove(self, package_name, generation):
    pass
  
  def info(self, package_name, generation):
    pass
  
  def tag(self, package_name, generation, tag_name):
    pass
  
  def untag(self, package_name, tag_name):
    pass
  
  def tags(self, package_name):
    pass


class S3Ledger(Ledger):
  PAGE_SIZE = 100
  TAG_SEPARATOR = 'tags'
  GENERATION_SEPARATOR = 'generations'

  @classmethod
  def get_name(cls, key):
    skey = key.split('/')
    assert len(skey) > 2
    return '/'.join(skey[:-2])

  def __init__(self, bucket_name):
    self.bucket_name = bucket_name
  
  def list_packages(self):
    bucket = boto3.resource('s3').bucket(self.bucket_name)
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
  

class Sacker(object):
  def __init__(self, metadata_store, data_store, hasher=hashlib.sha256):
    self.metadata_store = metadata_store
    self.data_store = data_store
    self.hasher = hasher
  
  def gc(self, delete=False):
    raise NotImplemented
  
  def list(self):
    return self.metadata_store.list_packages()
  
  def versions(self, package):
    return self.metadata_store.list_package_versions(package)
    
  def add(self, package, filename, metadata=None):
    with open(filename, 'rb') as fp:
      sha = compute_hash(fp, self.hasher)
    self.data_store.upload(sha, filename)
    return self.metadata_store.add(
        package, os.path.basename(filename), sha, metadata=metadata)

  def remove(self, package, generation):
    return self.metadata_store.remove(package, generation)
  
  def info(self, package, spec):
    return self.metadata_store.info(package, spec)

  def download(self, package, spec, filename=None):
    info = self.info(package, spec)
    self.data_store.download(info.sha, filename or info.basename)
  
  def tag(self, package_name, generation, tag_name):
    self.metadata_store.tag(package, generation, tag_name)
  
  def untag(self, package_name, tag_name):
    self.metadata_store.untag(package_name, tag_name)

  def tags(self, package_name):
    return self.metadata_store.tags(package_name)


# ledger e.g.
#    zk://ensemble/path
#    git://repo
#    dynamo://table
#
# store e.g.
#    hdfs://
#    s3://<bucket>
#    /dir


def setup_argparser():
  parser = argparse.ArgumentParser()
  subcommand_parser = parser.add_subparsers(help='subcommand help')
  
  