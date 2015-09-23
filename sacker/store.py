class Store(object):
  class Error(Exception): pass
  class Exists(Error): pass
  class DoesNotExist(Error): pass

  def init(self):
    pass

  def upload(self, sha, filename):
    """returns sha, raises ObjectExists"""
    raise NotImplemented

  def download(self, sha, filename):
    """saves sha to filename"""
    raise NotImplemented

  def delete(self, sha):
    """returns nothing, raises ObjectDoesNotExist"""
    raise NotImplemented


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
