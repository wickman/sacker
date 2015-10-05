from urlparse import urlparse

from .util import die


class Ledger(object):
  class Error(Exception): pass
  class Exists(Error): pass
  class DoesNotExist(Error): pass

  @classmethod
  def from_netloc(cls, netloc, path):
    raise NotImplementedError

  def init(self):
    pass

  def list_packages(self):
    raise NotImplementedError

  def list_package_versions(self, package_name):
    raise NotImplementedError

  def add(self, package_name, basename, sha, metadata=None):
    raise NotImplementedError

  def remove(self, package_name, version):
    raise NotImplementedError

  def latest(self, package_name):
    raise NotImplementedError

  def info(self, package_name, version):
    raise NotImplementedError

  def tag(self, package_name, version, tag_name):
    raise NotImplementedError

  def untag(self, package_name, tag_name):
    raise NotImplementedError

  def tags(self, package_name):
    raise NotImplementedError


LEDGERS = {}


def register_ledger(name, impl):
  LEDGERS[name] = impl


def unregister_all():
  LEDGERS.clear()


def parse_ledger(uri):
  uri = urlparse(uri)

  if uri.scheme not in LEDGERS:
    die('Unknown ledger scheme %r' % uri.scheme)

  return LEDGERS[uri.scheme].from_netloc(uri.netloc, uri.path)
