from __future__ import absolute_import, print_function

import argparse
import json
import os
import sys

from sacker.ledger import parse_ledger
from sacker.store import parse_store
from sacker.util import compute_hash, die


def gc_command(ledger, store, delete=False):
  raise NotImplementedError


def init_command(ledger, store, _):
  print('Initializing ledger...')
  ledger.init()
  print('Initializing store...')
  store.init()
  print('done.')


def list_command(ledger, store, _):
  for package_name in ledger.list_packages():
    print(package_name)


def versions_command(ledger, store, args):
  for version in ledger.list_package_versions(args.package):
    print(version)


def info_command(ledger, store, args):
  try:
    print(ledger.info(args.package, args.spec))
  except ledger.DoesNotExist as e:
    die(e)


def add_command(ledger, store, args):
  with open(args.filename, 'rb') as fp:
    sha = compute_hash(fp)
  store.upload(sha, args.filename)
  # todo(wickman) add metadata kwarg
  ledger.add(
      args.package,
      os.path.basename(args.filename),
      sha,
      os.stat(args.filename).st_mode)


def download_command(ledger, store, args):
  info = ledger.info(args.package, args.spec)
  store.download(info.sha, args.output_filename or info.basename)


def remove_command(ledger, store, args):
  ledger.remove(args.package, args.generation)


def tag_command(ledger, store, args):
  ledger.tag(args.package, args.generation, args.tag_name)


def untag_command(ledger, store, args):
  ledger.untag(args.package, args.tag_name)


def tags_command(ledger, store, args):
  ledger.tags(args.package)


# ledger e.g.
#    zk://ensemble/path
#    git://repo
#    dynamo://table
#
# store e.g.
#    hdfs://
#    s3://<bucket>
#    /dir


class LedgerAction(argparse.Action):
  def __call__(self, parser, namespace, values, option_string=None):
    setattr(namespace, self.dest, parse_ledger(values[0]))


class StoreAction(argparse.Action):
  def __call__(self, parser, namespace, values, option_string=None):
    setattr(namespace, self.dest, parse_store(values[0]))


def setup_argparser():
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--ledger',
      help='Override the ledger backend.',
      action=LedgerAction,
      nargs=1,
      default=None)
  parser.add_argument(
      '--store',
      help='Override the storage backend.',
      action=StoreAction,
      nargs=1,
      default=None)

  subcommand_parser = parser.add_subparsers(help='subcommand help')

  gc_parser = subcommand_parser.add_parser('gc', help='GC the store')
  gc_parser.set_defaults(func=gc_command)

  init_parser = subcommand_parser.add_parser('init', help='Initialize the store')
  init_parser.set_defaults(func=init_command)

  list_parser = subcommand_parser.add_parser('list', help='List packages')
  # todo add package prefix?
  list_parser.set_defaults(func=list_command)

  versions_parser = subcommand_parser.add_parser('versions', help='List package versions')
  versions_parser.set_defaults(func=versions_command)
  versions_parser.add_argument('package', help='Package name')

  info_parser = subcommand_parser.add_parser('info',
      help='Get information about a specific package version.')
  info_parser.set_defaults(func=info_command)
  info_parser.add_argument('package', help='Package name')
  info_parser.add_argument('spec', help='Package version or tag')

  add_parser = subcommand_parser.add_parser('add', help='Add a new package version.')
  add_parser.set_defaults(func=add_command)
  add_parser.add_argument('package', help='Package name')
  add_parser.add_argument('filename', help='Package filename')

  download_parser = subcommand_parser.add_parser('download', help='Download a package.')
  download_parser.set_defaults(func=download_command)
  download_parser.add_argument('package', help='Package name')
  download_parser.add_argument('spec', help='Package version or tag')
  download_parser.add_argument(
      '-o', dest='output_filename', default=None, help='Optional destination for file.')

  remove_parser = subcommand_parser.add_parser(
      'remove', help='Remove a package version from available packages.')
  remove_parser.set_defaults(func=remove_command)
  remove_parser.add_argument('package', help='Package name')
  remove_parser.add_argument('version', help='Package version')

  tag_parser = subcommand_parser.add_parser('tag', help='Tag a package with a label.')
  tag_parser.set_defaults(func=tag_command)
  tag_parser.add_argument('package', help='Package name')
  tag_parser.add_argument('version', help='Package version')
  tag_parser.add_argument('label', help='Package label')

  untag_parser = subcommand_parser.add_parser('untag', help='Untag label from package.')
  untag_parser.set_defaults(func=untag_command)
  untag_parser.add_argument('package', help='Package name')
  untag_parser.add_argument('label', help='Package label')

  tags_parser = subcommand_parser.add_parser('tags', help='List all tagged labels of a package.')
  tags_parser.set_defaults(func=tags_command)
  tags_parser.add_argument('package', help='Package name')
  tags_parser.add_argument('label', help='Package label')

  return parser


def setup_defaults(args):
  for path in os.environ.get('SACKER_CONFIG'), os.path.expanduser('~/.sacker.json'):
    if not path:
      continue

    try:
      with open(path, 'rb') as fp:
        config = json.load(fp)
    except IOError:
      continue

    if 'ledger' in config and not args.ledger:
      args.ledger = parse_ledger(config['ledger'])

    if 'store' in config and not args.store:
      args.store = parse_store(config['store'])

  if not args.store:
    die('Must specify a store.')

  if not args.ledger:
    die('Must specify a ledger.')


# TODO(wickman) Build a proper plugin mechanism
def register_all():
  from sacker.ledger import register_ledger
  from sacker.ledgers.dynamo import DynamoLedger
  from sacker.ledgers.s3 import S3Ledger
  from sacker.store import register_store
  from sacker.stores.s3 import S3Store
  register_store('s3', S3Store)
  register_ledger('dynamo', DynamoLedger)
  register_ledger('s3', S3Ledger)


def main():
  register_all()
  parser = setup_argparser()
  args = parser.parse_args()
  setup_defaults(args)
  sys.exit(args.func(args.ledger, args.store, args))
