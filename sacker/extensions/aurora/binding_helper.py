from apache.aurora.client.binding_helper import BindingHelper
from apache.aurora.client.cli import ConfigurationPlugin
from apache.aurora.config.loader import AuroraConfigLoader
from apache.aurora.common.clusters import CLUSTERS
from sacker import ledger as sacker_ledger, store as sacker_store
from sacker.stores.s3 import S3Store
from sacker.ledgers.dynamo import DynamoLedger
from sacker.ledgers.s3 import S3Ledger
from pystachio.matcher import Any, Matcher
from pystachio import Ref

from . import schema as sacker_schema


def get_sacker(cluster):
  ledger = sacker_ledger.parse_ledger(cluster.sacker_ledger_uri)
  store = sacker_store.parse_store(cluster.sacker_store_uri)

  if not isinstance(store, S3Store):
    raise RuntimeError('Sacker binding helper only supports S3 store.')

  return ledger, store


def get_sacker_binding(cluster, name, version="latest"):
  cluster = cluster.with_trait(sacker_schema.SackerClientTrait)
  ledger, store = get_sacker(cluster)
  package = ledger.info(name, version)

  s3_object = sacker_schema.SackerObject(
      sha=package.sha,
      filename=package.basename,
      mode='%o' % (package.mode & 0777),  # limit to lowest bits
      bucket=store.bucket,
      version=package.version,
      metadata=package.metadata if package.metadata is not None else {},
  )
  if cluster.sacker_uri_override:
    s3_object = s3_object(uri=cluster.sacker_uri_override)
  if cluster.sacker_download_command:
    s3_object = s3_object(copy_command=cluster.sacker_download_command)
  return s3_object


class SackerBindingHelper(BindingHelper):
  @property
  def name(self):
    return 'sacker'

  @property
  def matcher(self):
    return Matcher('sacker')[Any][Any]

  def bind(self, config, match, env, binding_dict):
    cluster = CLUSTERS[config.cluster()]
    name, version = match[1:3]
    ref_str = 'sacker[%s][%s]' % (name, version)
    ref = Ref.from_address(ref_str)
    if ref_str in binding_dict:
      s3_struct = binding_dict[ref_str]
    else:
      s3_struct = get_sacker_binding(cluster, name, version)
    binding_dict[ref_str] = s3_struct
    config.bind({ref: s3_struct})
    config.add_metadata(
        key='sacker',
        value='%s/%s sha:%s' % (name, s3_struct.version(), s3_struct.sha())
    )


class SackerBindingHelperPlugin(ConfigurationPlugin):
  def before_execution(self, context):
    # register usable backends
    sacker_ledger.register_ledger('s3', S3Ledger)
    sacker_ledger.register_ledger('dynamo', DynamoLedger)
    sacker_store.register_store('s3', S3Store)

    # register schema
    AuroraConfigLoader.register_schema(sacker_schema)

    # register binding helper
    BindingHelper.register(SackerBindingHelper())

  def after_execution(self, context, result_code):
    pass

  def before_dispatch(self, raw_args):
    pass

  def get_options(self):
    return []
