class Package(object):
  def __init__(self, name, generation, sha, basename, mode, metadata=None):
    self.name, self.generation, self.sha, self.basename, self.mode, self.metadata = (
        name, generation, sha, basename, mode, metadata or {})
