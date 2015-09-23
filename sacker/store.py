from urlparse import urlparse

from .util import die


class Store(object):
  class Error(Exception): pass
  class Exists(Error): pass
  class DoesNotExist(Error): pass

  @classmethod
  def from_netloc(cls, netloc, path):
    raise NotImplementedError

  def init(self):
    pass

  def upload(self, sha, filename):
    """returns sha, raises ObjectExists"""
    raise NotImplementedError

  def download(self, sha, filename):
    """saves sha to filename"""
    raise NotImplementedError

  def delete(self, sha):
    """returns nothing, raises ObjectDoesNotExist"""
    raise NotImplementedError


class ChainedStore(Store):
  def __init__(self, stores):
    self.stores = stores

  def init(self):
    for store in self.stores:
      store.init()

  # TODO(wickman) figure out recovery semantics
  def upload(self, sha, filename):
    for store in self.stores:
      store.upload(sha, filename)

  def download(self, sha, filename):
    for store in self.stores:
      try:
        store.download(sha, filename)
        break
      except store.NotExists:
        continue
    else:
      raise self.NotExists('Could not find %s' % sha)

  def delete(self, sha):
    for store in self.stores:
      store.delete(sha)


STORES = {}


def register_store(name, impl):
  STORES[name] = impl


def unregister_all():
  STORES.clear()


def parse_store(uri):
  uri = urlparse(uri)

  if uri.scheme not in STORES:
    die('Unknown store scheme %r' % uri.scheme)

  return STORES[uri.scheme].from_netloc(uri.netloc, uri.path)
