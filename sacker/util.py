from __future__ import print_function

import hashlib
import sys


def compute_hash(filelike, chunksize=65536, hasher=hashlib.sha256):
  hash = hasher()

  while True:
    data = filelike.read(chunksize)
    if data:
      hash.update(data)
    else:
      break

  return hash.hexdigest()


def die(msg, rc=1):
  print(msg, file=sys.stderr)
  sys.exit(rc)
