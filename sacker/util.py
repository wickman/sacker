import hashlib


def compute_hash(filelike, chunksize=65536, hasher=hashlib.sha256):
  hash = hasher()

  while True:
    data = filelike.read(chunksize)
    if data:
      hash.update(data)
    else:
      break

  return hash.hexdigest()
