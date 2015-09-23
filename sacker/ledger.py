class Ledger(object):
  def init(self):
    pass

  def list_packages(self):
    raise NotImplemented

  def list_package_versions(self, package_name):
    raise NotImplemented

  def add(self, package_name, basename, sha, metadata=None):
    raise NotImplemented

  def remove(self, package_name, generation):
    raise NotImplemented

  def info(self, package_name, generation):
    raise NotImplemented

  def tag(self, package_name, generation, tag_name):
    raise NotImplemented

  def untag(self, package_name, tag_name):
    raise NotImplemented

  def tags(self, package_name):
    raise NotImplemented
