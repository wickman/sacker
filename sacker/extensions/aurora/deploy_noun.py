import getpass
import hashlib
import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
import webbrowser
from contextlib import contextmanager
from pipes import quote

from sacker import ledger as sacker_ledger
from sacker import store as sacker_store
from sacker.ledgers.s3 import S3Ledger
from sacker.ledgers.dynamo import DynamoLedger
from sacker.stores.s3 import S3Store

from apache.aurora.common.clusters import CLUSTERS
from apache.aurora.config import AuroraConfig
from apache.aurora.client.base import get_update_page
from apache.aurora.client.cli import (
    Plugin,
    Noun,
    Verb,
    EXIT_COMMAND_FAILURE,
    EXIT_OK,
    EXIT_INVALID_PARAMETER,
    EXIT_INVALID_CONFIGURATION,
    EXIT_API_ERROR,
    EXIT_UNKNOWN_ERROR,
)
from apache.aurora.client.cli.context import AuroraCommandContext
from apache.aurora.client.cli.options import (
    CommandOption,
    BIND_OPTION,
    JSON_READ_OPTION,
    CONFIG_ARGUMENT,
    JOBSPEC_ARGUMENT,
)
from pystachio import Struct, Required, String
from gen.apache.aurora.api.constants import ACTIVE_STATES, ACTIVE_JOB_UPDATE_STATES
from gen.apache.aurora.api.ttypes import JobUpdateStatus, JobUpdateKey


STAGE_KEY = 'config version'


class DeployClientTrait(Struct):
  deploy_ledger_uri = Required(String)  # noqa
  deploy_store_uri = Required(String)  # noqa


def get_ledger(cluster):
  cluster = cluster.with_trait(DeployClientTrait)
  return sacker_ledger.parse_ledger(cluster.deploy_ledger_uri)


def get_store(cluster):
  cluster = cluster.with_trait(DeployClientTrait)
  return sacker_store.parse_store(cluster.deploy_store_uri)


def jobkey_to_config_name(jobkey):
  return '%s/configs' % jobkey


def jobkey_to_release_name(jobkey):
  return '%s/releases' % jobkey


OPTIONAL_VERSION_ARGUMENT = CommandOption(
    'version',
    default=None,
    metavar='VERSION',
    help='Version number, "live", or "latest" tag.',
    nargs='?',
)

METADATA_OPTIONS = CommandOption(
    '--metadata',
    metavar='KEY=VALUE',
    default=[],
    action='append',
)


def get_metadata(context):
  def iterate():
    for keypair in context.options.metadata:
      try:
        key, val = keypair.split('=')
      except ValueError:
        raise context.CommandError(EXIT_INVALID_PARAMETER, 'Metadata must be key=value pairs.')
      yield (key, val)
  return dict(iterate())


@contextmanager
def temporary_dir():
  dirname = tempfile.mkdtemp()
  yield dirname
  shutil.rmtree(dirname)


def get_config(jobkey, version='latest'):
  config_ledger = get_ledger(CLUSTERS[jobkey.cluster])
  config_store = get_store(CLUSTERS[jobkey.cluster])
  config_package_name = jobkey_to_config_name(jobkey)
  package = config_ledger.info(config_package_name, version)

  with temporary_dir() as dirname:
    config_store.download(package.sha, os.path.join(dirname, 'config.json'))
    with open(os.path.join(dirname, 'config.json')) as fp:
      return package, fp.read()


class StageCommand(Verb):
  @property
  def name(self):
    return 'stage'

  @property
  def help(self):
    return 'Stage a compiled config into the ledger.'

  def get_options(self):
    return [
        BIND_OPTION,
        JSON_READ_OPTION,
        METADATA_OPTIONS,
        JOBSPEC_ARGUMENT,
        CONFIG_ARGUMENT,
    ]

  def execute(self, context):
    config_ledger = get_ledger(CLUSTERS[context.options.jobspec.cluster])
    config_store = get_store(CLUSTERS[context.options.jobspec.cluster])

    # get config, embed version metadata and compute sha
    config = context.get_job_config(context.options.jobspec, context.options.config_file)
    json_raw = json.loads(config.raw().json_dumps())
    json_pretty = json.dumps(json_raw, indent=4, sort_keys=True)
    json_sha = hashlib.sha256(json_pretty).hexdigest()

    # get user-supplied metadata
    metadata = get_metadata(context)

    # add our own
    metadata.update(stage_timestamp=str(time.time()))

    # write to disk since the api requires actual file and not file-like
    with temporary_dir() as dirname:
      path = os.path.join(dirname, 'config.json')
      with open(path, 'wb') as fp:
        fp.write(json_pretty)
      mode = os.stat(path).st_mode

      # upload
      config_store.upload(json_sha, path)

      # commit to ledger
      config_package_name = jobkey_to_config_name(context.options.jobspec)
      actual_version = config_ledger.add(
          config_package_name, 'config.json', json_sha, mode, metadata=metadata)

    context.print_out('Staged %s version %d' % (context.options.jobspec, actual_version))

    return EXIT_OK


class ReleaseCommand(Verb):
  @property
  def name(self):
    return 'release'

  @property
  def help(self):
    return 'Release a staged config.'

  def get_options(self):
    return [
        JOBSPEC_ARGUMENT,
        OPTIONAL_VERSION_ARGUMENT,
        METADATA_OPTIONS,
    ]

  def _job_diff(self, api, context, config_content):
    # return true if jobs are diff, false if not
    config = AuroraConfig.loads_json(config_content)
    role, env, name = config.role(), config.environment(), config.name()
    resp = api.query(api.build_query(role, name, env=env, statuses=ACTIVE_STATES))
    context.log_response_and_raise(resp, err_code=EXIT_INVALID_PARAMETER,
        err_msg="Could not find job to diff against")
    if resp.result.scheduleStatusResult.tasks is None:
      context.print_err("No tasks found for job %s" % context.options.jobspec)
      return True
    else:
      remote_tasks = [t.assignedTask.task for t in resp.result.scheduleStatusResult.tasks]
    resp = api.populate_job_config(config)
    context.log_response_and_raise(resp, err_code=EXIT_INVALID_CONFIGURATION,
          err_msg="Error loading configuration")
    local_tasks = [resp.result.populateJobResult.taskConfig] * config.instances()
    if len(remote_tasks) != len(local_tasks):
      return True
    for task1, task2 in zip(remote_tasks, local_tasks):
      if task1 != task2:
        return True
    return False

  def _reconcile_release(self, api, context, terminal, nonterminal):
    # determine if either the terminal or nonterminal releases are live
    # if so, reconcile and return, otherwise panic.
    context.print_out('Reconciling state with unfinished update.')

    _, nonterminal_config_content = get_config(
        context.options.jobspec, version=nonterminal.metadata['deploy_version'])

    if not self._job_diff(api, context, nonterminal_config_content):
      self._set_release_rollforward(context, terminal, nonterminal)
      return EXIT_OK

    # it is possible for the terminal release to be None.
    if terminal:
      _, terminal_config_content = get_config(
          context.options.jobspec, version=terminal.metadata['deploy_version'])

      if not self._job_diff(api, context, terminal_config_content):
        self.set_release_rollback(context, terminal, nonterminal)
        return EXIT_OK

    raise context.CommandError(EXIT_COMMAND_FAILURE,
        'Cannot determine the outcome of the previous update.')

  def _roll_release(self, context, release_package, state):
    # update the release ledger for (possibly null) release.
    config_ledger = get_ledger(CLUSTERS[context.options.jobspec.cluster])
    config_key = jobkey_to_config_name(context.options.jobspec)

    if release_package:
      config_package = config_ledger.info(config_key, release_package.metadata['deploy_version'])
      release_metadata = release_package.metadata.copy()
    else:
      config_package = None
      release_metadata = {}

    release_metadata.update(
        deploy_state=state,
        deploy_version=config_package.version if config_package else None,
        deploy_timestamp=str(time.time()),
    )

    config_ledger.add(
        release_package.name,
        config_package.basename if config_package else 'config.json',
        config_package.sha if config_package else '0' * 64,
        config_package.mode if config_package else 0644,
        metadata=release_metadata)

  def _set_release_rollforward(self, context, terminal_release, nonterminal_release):
    self._roll_release(context, nonterminal_release, 'RELEASED')

  def _set_release_rollback(self, context, terminal_release, nonterminal_release):
    # terminal release can be None
    self._roll_release(context, terminal_release, 'REVERTED')

  def _set_release_live(self, context, config_package, uuid):
    # return nonterminal release_package
    config_ledger = get_ledger(CLUSTERS[context.options.jobspec.cluster])
    release_key = jobkey_to_release_name(context.options.jobspec)
    release_metadata = get_metadata(context)
    release_metadata.update(
        deploy_state='RELEASING',
        deploy_uuid=uuid,
        deploy_version=config_package.version,
        deploy_timestamp=str(time.time()),
    )
    version = config_ledger.add(
        release_key,
        config_package.basename,
        config_package.sha,
        config_package.mode,
        metadata=release_metadata,
    )
    # TODO(wickman) Make sure the dynamo ledger does consistent reads.
    return config_ledger.info(release_key, version)

  def _start_release(self, api, context):
    package, config_content = get_config(
        context.options.jobspec, context.options.version or 'latest')

    config = AuroraConfig.loads_json(config_content)
    resp = api.start_job_update(config, message='Release started by %s.' % getpass.getuser())

    if not resp.result:
      return package, None

    return package, resp.result.startJobUpdateResult.key.id

  def _resume_release(self, api, context, terminal, nonterminal):
    # nonterminal is not none
    # terminal could be none
    #
    #    resume_release(terminal, nonterminal)
    #      while True:
    #        active:
    #          continue
    #        terminal:
    #          status = SUCCESS
    #            set_release_rollforward(terminal, nonterminal)
    #          status = ROLLED BACK
    #            set_release_rollback(terminal, nonterminal)
    #        unknown:
    #          reconcile_release
    cur_state = None

    update_key = JobUpdateKey(
        job=context.options.jobspec.to_thrift(),
        id=nonterminal.metadata['deploy_uuid'])

    while True:
      resp = api.query_job_updates(update_key=update_key)
      context.log_response_and_raise(resp)
      summaries = resp.result.getJobUpdateSummariesResult.updateSummaries

      if len(summaries) == 0:
        # there is no in flight update, so we must go back to reconciliation
        return self._reconcile_release(api, context, terminal, nonterminal)

      if len(summaries) > 1:
        raise context.CommandError(EXIT_API_ERROR, 'Multiple in-flight updates.')

      new_state = summaries[0].state.status
      if new_state != cur_state:
        cur_state = new_state
        context.print_out('Current state %s' % JobUpdateStatus._VALUES_TO_NAMES[cur_state])
        if cur_state not in ACTIVE_JOB_UPDATE_STATES:
          if cur_state == JobUpdateStatus.ROLLED_FORWARD:
            self._set_release_rollforward(context, terminal, nonterminal)
            return EXIT_OK
          elif cur_state == JobUpdateStatus.ROLLED_BACK:
            self._set_release_rollback(context, terminal, nonterminal)
            return EXIT_OK
          else:
            return EXIT_UNKNOWN_ERROR
      time.sleep(5)

  def _previous_release_pair(self, context):
    config_ledger = get_ledger(CLUSTERS[context.options.jobspec.cluster])
    release_key = jobkey_to_release_name(context.options.jobspec)

    versions = sorted(config_ledger.list_package_versions(release_key))

    if not versions:
      return (None, None)

    try:
      latest_version = config_ledger.info(release_key, versions[-1])
    except config_ledger.Error:
      raise context.CommandError(EXIT_API_ERROR, 'Corrupted ledger.')

    if latest_version.metadata['deploy_state'] != 'RELEASING':
      return (latest_version, None)

    if len(versions) == 1:
      raise context.CommandError(EXIT_API_ERROR, 'Corrupted ledger.')

    try:
      previous_version = config_ledger.info(release_key, versions[-2])
    except config_ledger.Error:
      raise context.CommandError(EXIT_API_ERROR, 'Corrupted ledger.')

    if previous_version.metadata['deploy_state'] not in ('RELEASED', 'REVERTED'):
      raise context.CommandError(EXIT_API_ERROR, 'Unknown ledger state.')

    return (previous_version, latest_version)

  def execute(self, context):
    #       release_package = get_latest_release()
    #       if release_package is not terminal:
    #         resume_release(release_package)
    #
    #       old_config = get_old_config(...)  # most recent terminal
    #       new_config, uuid = start_release(new_config)
    #       release_package = set_release_live(new_config, uuid)
    #       resume_release(release_package)
    api = context.get_api(context.options.jobspec.cluster)
    terminal, nonterminal = self._previous_release_pair(context)

    # determine if we have an in-flight release and resume if necessary
    if nonterminal:
      return self._resume_release(api, context, terminal, nonterminal)

    # if not, start a new release
    config_package, uuid = self._start_release(api, context)
    if uuid is None:
      context.print_out('Update would be a no-op.')
      return EXIT_OK

    # launch update browser
    update_page = get_update_page(api, context.options.jobspec, uuid)
    context.print_out('Update: %s' % update_page)
    webbrowser.open_new_tab(update_page)

    # record the active release in the ledger
    nonterminal = self._set_release_live(context, config_package, uuid)

    # wait until terminal
    return self._resume_release(api, context, terminal, nonterminal)


class LogCommand(Verb):
  @property
  def name(self):
    return 'log'

  @property
  def help(self):
    return 'Show a history of releases.'

  def get_options(self):
    return [
        JOBSPEC_ARGUMENT,
        OPTIONAL_VERSION_ARGUMENT,
    ]

  def execute(self, context):
    config_ledger = get_ledger(CLUSTERS[context.options.jobspec.cluster])
    release_package_name = jobkey_to_release_name(context.options.jobspec)

    for version in config_ledger.list_package_versions(release_package_name):
      package = config_ledger.info(release_package_name, version)
      metadata = package.metadata.copy()

      deploy_timestamp = metadata.pop('deploy_timestamp', None)
      deploy_version = metadata.pop('deploy_version')
      deploy_state = metadata.pop('deploy_state')
      deploy_uuid = metadata.pop('deploy_uuid')

      context.print_out('%s %s %4d %s %s %s %s' % (
          time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(float(deploy_timestamp)))
          if deploy_timestamp else '????/??/?? ??:??:??',
          context.options.jobspec,
          int(deploy_version),
          package.sha[:8],
          deploy_state,
          deploy_uuid,
          json.dumps(metadata, indent=4)))

    return EXIT_OK


class VersionsCommand(Verb):
  @property
  def name(self):
    return 'versions'

  @property
  def help(self):
    return 'List staged config versions.'

  def get_options(self):
    return [
        JOBSPEC_ARGUMENT,
        CommandOption(
            '--full',
            default=False,
            action='store_true',
            help='Show full configuration metadata.')
    ]

  def execute(self, context):
    config_ledger = get_ledger(CLUSTERS[context.options.jobspec.cluster])
    config_package_name = jobkey_to_config_name(context.options.jobspec)

    for version in config_ledger.list_package_versions(config_package_name):
      if not context.options.full:
        context.print_out('%s %4d' % (context.options.jobspec, version))
        continue

      package = config_ledger.info(config_package_name, version)
      metadata = package.metadata.copy()
      stage_timestamp = metadata.pop('stage_timestamp', None)

      context.print_out('%s %s %4d %s %s' % (
          time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(float(stage_timestamp)))
          if stage_timestamp else '????/??/?? ??:??:??',
          context.options.jobspec,
          version,
          package.sha[:8],
          json.dumps(metadata, indent=4)))

    return EXIT_OK


class ShowCommand(Verb):
  @property
  def name(self):
    return 'show'

  @property
  def help(self):
    return 'Show a staged config.'

  def get_options(self):
    return [JOBSPEC_ARGUMENT, OPTIONAL_VERSION_ARGUMENT]

  def execute(self, context):
    _, content = get_config(context.options.jobspec, context.options.version or 'latest')
    context.print_out(content)
    return EXIT_OK


class DiffCommand(Verb):
  @property
  def name(self):
    return 'diff'

  @property
  def help(self):
    return 'Diff staged configs against the latest release.'

  def get_options(self):
    return [
        JOBSPEC_ARGUMENT,
        OPTIONAL_VERSION_ARGUMENT,
    ]

  def execute(self, context):
    config_ledger = get_ledger(CLUSTERS[context.options.jobspec.cluster])
    release_key = jobkey_to_release_name(context.options.jobspec)
    latest_release_package = config_ledger.info(release_key, 'latest')

    _, latest_release_config = get_config(
        context.options.jobspec,
        latest_release_package.metadata['deploy_version'])

    _, staged_release_config = get_config(
        context.options.jobspec,
        context.options.version or 'latest')

    diff_program = os.environ.get('DIFF_VIEWER', 'diff')

    with tempfile.NamedTemporaryFile() as new:
      with tempfile.NamedTemporaryFile() as old:
        new.write(staged_release_config)
        new.flush()

        old.write(latest_release_config)
        old.flush()

        subprocess.call('%s %s %s' % (diff_program, quote(old.name), quote(new.name)),
            shell=True)

    return EXIT_OK


class DeployNoun(Noun):
  @property
  def name(self):
    return 'deploy'

  @property
  def help(self):
    return 'Staged deployment of configs.'

  @classmethod
  def create_context(cls):
    return AuroraCommandContext()

  def __init__(self):
    super(DeployNoun, self).__init__()
    self.register_verb(StageCommand())
    self.register_verb(ReleaseCommand())
    self.register_verb(LogCommand())
    self.register_verb(VersionsCommand())
    self.register_verb(ShowCommand())
    self.register_verb(DiffCommand())


class DeployCommandPlugin(Plugin):
  def get_nouns(self):
    return [DeployNoun()]

  def before_dispatch(self, raw_args):
    # register backends
    sacker_store.register_store('s3', S3Store)
    sacker_ledger.register_ledger('s3', S3Ledger)
    sacker_ledger.register_ledger('dynamo', DynamoLedger)

    # blackhole boto logging unless verbosity is enabled
    if '-v' not in raw_args and '--verbose' not in raw_args:
      logging.getLogger('boto3').setLevel(logging.WARN)
      logging.getLogger('botocore').setLevel(logging.WARN)

    return raw_args
