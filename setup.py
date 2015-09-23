from setuptools import setup

setup(
  name = 'sacker',
  version = '0.1.0',
  description = 'a simple cloud blob manager',
  zip_safe = True,
  entry_points = {
    'console_scripts': [
      'sacker = sacker.bin.sacker:main',
    ]
  },
  packages = [
    'sacker',
    'sacker.bin',
    'sacker.ledgers',
    'sacker.stores',
  ],
  # todo use extras_require
  install_requires = [
    'boto3',
  ]
)
