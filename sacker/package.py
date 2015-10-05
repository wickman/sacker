class Package(object):
  def __init__(self, name, version, sha, basename, mode, metadata=None):
    self.name, self.version, self.sha, self.basename, self.mode, self.metadata = (
        name, version, sha, basename, mode, metadata or {})

  def __str__(self):
    return 'Package(name: %r, version: %d, sha: %s..., filename: %s, mode: %o)' % (
        self.name, self.version, self.sha[:8], self.basename, self.mode)
