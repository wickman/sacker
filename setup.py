from setuptools import setup

setup(
  name = 'sacker',
  version = '0.1.0',
  description = 'a simple cloud blob manager',
  zip_safe = True,
  entry_points = {
    'console_scripts': [
      'sacker = sacker.bin.sacker:main',
    ],
    'apache.aurora.client.cli.plugin': [
        'SackerBindingHelperPlugin = sacker.extensions.aurora.binding_helper:SackerBindingHelperPlugin',
        'SackerDeployCommandPlugin = sacker.extensions.aurora.deploy_noun:DeployCommandPlugin',
    ]
  },
  packages = [
    'sacker',
    'sacker.bin',
    'sacker.extensions',
    'sacker.extensions.aurora',
    'sacker.ledgers',
    'sacker.stores',
  ],
  # todo use extras_require
  install_requires = [
    'boto3',
  ]
)
