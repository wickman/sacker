# checkstyle: noqa
from textwrap import dedent

from apache.thermos.config.schema import Process
from pystachio import (
    Default,
    Map,
    Required,
    String,
    Struct,
)


DEFAULT_COPY_COMMAND = (
"""
curl --retry 5 -o "{{filename}}~" {{uri}}
if [[ "{{sha}}" == $(openssl sha -sha256 < "{{filename}}~" | awk '{ print $NF }') ]]; then
  mv -f "{{filename}}~" "{{filename}}"
  chmod {{mode}} {{filename}}
else
  echo "Package appears to be corrupt."
  exit 1
fi
""")


class SackerObject(Struct):
  sha = Required(String)
  filename = Required(String)
  version = Required(String)
  mode = Required(String)
  bucket = Required(String)
  uri = Default(String, 'http://{{bucket}}.s3.amazonaws.com/{{sha}}')
  metadata = Default(Map(String, String), {})
  copy_command = Default(String, DEFAULT_COPY_COMMAND)


class SackerClientTrait(Struct):
  sacker_ledger_uri = Required(String)
  sacker_store_uri = Required(String)
  sacker_uri_override = String
  sacker_download_command = String


class Sacker(object):
  COPY_COMMAND = "{{{{pkg}}.copy_command}}"
  UNPACK_COMMAND = COPY_COMMAND + dedent("""
     function _delete_pkg() { rm -f {{{{pkg}}.package}}; }

     if [[ "{{{{pkg}}.package}}" == *".tar" ]]; then
       tar -xf {{{{pkg}}.package}} && _delete_pkg
     elif [[ "{{{{pkg}}.package}}" == *".tar.gz" || "{{{{pkg}}.package}}" == *".tgz" ]]; then
       tar -xzf {{{{pkg}}.package}} && _delete_pkg
     elif [[ "{{{{pkg}}.package}}" == *".tar.bz2" || "{{{{pkg}}.package}}" == *".tbz2" ]]; then
       tar -xjf {{{{pkg}}.package}} && _delete_pkg
     elif [[ "{{{{pkg}}.package}}" == *".zip" ]]; then
       unzip -qo {{{{pkg}}.package}} && _delete_pkg
     elif [[ "{{{{pkg}}.package}}" == *".shar" ]]; then
       sh {{{{pkg}}.package}} && _delete_pkg
     fi
  """)

  PROCESS = Process(
      name = 'sacker_{{__package_name}}',
  ).bind(pkg = 'sacker[{{__package_name}}][{{__package_version}}]')

  @classmethod
  def copy(cls, name, version="latest", unpack=False):
    return cls.PROCESS(
       cmdline = cls.UNPACK_COMMAND if unpack else cls.COPY_COMMAND
    ).bind(
       __package_name = name,
       __package_version = version
    )

  @classmethod
  def install(cls, name, version="latest"):
    return cls.copy(name, version, unpack=True)
