import json
import os


class Config(object):
  @classmethod
  def from_file(cls, filename):
    with open(filename, 'rb') as fp:
      config = json.load(fp)
      return cls(config.get('ledger'), config.get('store'))

  @classmethod
  def from_environment(cls):
    global_config = cls(None, None)

    for path in os.environ.get('SACKER_CONFIG'), os.path.expanduser('~/.sacker.json'):
      if not path:
        continue
      try:
        config = cls.from_file(path)
      except IOError:
        continue

      if config.ledger_uri:
        global_config.ledger_uri = config.ledger_uri
      if config.store_uri:
        global_config.store_uri = config.store_uri

    return global_config

  def __init__(self, ledger_uri=None, store_uri=None):
    self.ledger_uri = ledger_uri
    self.store_uri = store_uri
