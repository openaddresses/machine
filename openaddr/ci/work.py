import logging; _L = logging.getLogger('openaddr.ci.work')

from .. import util
from ..jobs import JOB_TIMEOUT
from .objects import RunState

import os, json, tempfile, shutil, base64, subprocess
from urllib.parse import urlparse, urljoin

MAGIC_OK_MESSAGE = 'Everything is fine'

def upload_file(s3, keyname, filename):
    ''' Create a new S3 key with filename contents, return its URL and MD5 hash.
    '''
    key = s3.new_key(keyname)

    kwargs = dict(policy='public-read', reduced_redundancy=True)
    key.set_contents_from_filename(filename, **kwargs)
    url = util.s3_key_url(key)

    return url, key.md5.decode('ascii')

def make_source_filename(source_name):
    '''
    '''
    return source_name.replace(u'/', u'--') + '.txt'

def assemble_runstate(s3, input, source_name, run_id, index_dirname):
    ''' Convert worker index dictionary to RunState.
    '''
    output = {k: v for (k, v) in input.items()}
    output['run id'] = run_id

    if input['cache']:
        # e.g. /runs/0/cache.zip
        cache_path = os.path.join(index_dirname, input['cache'])
        key_name = '/runs/{run}/{cache}'.format(run=run_id, **input)
        url, fingerprint = upload_file(s3, key_name, cache_path)
        output['cache'], output['fingerprint'] = url, fingerprint

    if input['sample']:
        # e.g. /runs/0/sample.json
        sample_path = os.path.join(index_dirname, input['sample'])
        key_name = '/runs/{run}/{sample}'.format(run=run_id, **input)
        url, _ = upload_file(s3, key_name, sample_path)
        output['sample'] = url

    if input['processed']:
        # e.g. /runs/0/fr/paris.zip
        processed_path = os.path.join(index_dirname, input['processed'])
        package_args = input.get('website') or 'Unknown', input.get('license') or 'Unknown'
        archive_path = util.package_output(source_name, processed_path, *package_args)
        key_name = u'/runs/{run}/{name}.zip'.format(run=run_id, name=source_name)
        url, hash = upload_file(s3, key_name, archive_path)
        output['processed'], output['process hash'] = url, hash

        if os.path.exists(archive_path):
            os.remove(archive_path)

    if input['output']:
        # e.g. /runs/0/output.txt
        output_path = os.path.join(index_dirname, input['output'])
        key_name = '/runs/{run}/{output}'.format(run=run_id, **input)
        url, _ = upload_file(s3, key_name, output_path)
        output['output'] = url

    if input['preview']:
        # e.g. /runs/0/preview.png
        preview_path = os.path.join(index_dirname, input['preview'])
        key_name = '/runs/{run}/{preview}'.format(run=run_id, **input)
        url, _ = upload_file(s3, key_name, preview_path)
        output['preview'] = url

    if input['slippymap']:
        # e.g. /runs/0/slippymap.mbtiles
        slippymap_path = os.path.join(index_dirname, input['slippymap'])
        key_name = '/runs/{run}/{slippymap}'.format(run=run_id, **input)
        url, _ = upload_file(s3, key_name, slippymap_path)
        output['slippymap'] = url

    return RunState(output)

def do_work(s3, run_id, source_name, job_contents_b64, render_preview, output_dir, mapbox_key=None):
    ''' Do the actual work of running a source file in job_contents.
    '''
    _L.info('Doing work on source {}'.format(repr(source_name)))

    # Make a directory to run the whole job
    workdir = tempfile.mkdtemp(prefix='work-', dir=output_dir)

    # Write the user input to a file
    out_fn = os.path.join(workdir, make_source_filename(source_name))
    with open(out_fn, 'wb') as out_fp:
        out_fp.write(base64.b64decode(job_contents_b64))

    # Make a directory in which to run openaddr
    oa_dir = os.path.join(workdir, 'out')
    os.mkdir(oa_dir)

    # Invoke the job to do
    logfile_path = os.path.join(workdir, 'logfile.txt')
    cmd = 'openaddr-process-one', '-l', logfile_path, out_fn, oa_dir

    if render_preview and mapbox_key:
        cmd += ('--render-preview', '--mapbox-key', mapbox_key)
    else:
        cmd += ('--skip-preview', )

    try:
        known_error, cmd_status = False, 0
        timeout_seconds = JOB_TIMEOUT.seconds + JOB_TIMEOUT.days * 86400
        with open('/dev/null', 'a') as devnull:
            result_stdout = subprocess.check_output(cmd, timeout=timeout_seconds, stderr=devnull)
    except subprocess.TimeoutExpired as e:
        known_error, cmd_status, result_stdout = True, None, e.output
    except subprocess.CalledProcessError as e:
        known_error, cmd_status, result_stdout = True, e.returncode, e.output
    except Exception:
        known_error, cmd_status, result_stdout = False, None, None
        raise
    finally:
        if hasattr(result_stdout, 'decode'):
            # "The actual encoding of the output data may depend on the command
            # being invoked" - https://docs.python.org/3/library/subprocess.html
            result_stdout = result_stdout.decode('utf8', 'replace')

        if known_error:
            # Something went wrong; throw back an error result.
            key_name = '/runs/{run}/logfile.txt'.format(run=run_id)
            try:
                url, _ = upload_file(s3, key_name, logfile_path)
            except IOError:
                output = dict()
            else:
                output = dict(output=url)

            return dict(result_code=cmd_status, result_stdout=result_stdout,
                        message='Something went wrong in {0}'.format(*cmd),
                        output=output)

    # openaddr-process-one prints a path to index.json
    state_paths = json.loads(result_stdout.strip())

    results = []
    for state_path in state_paths:
        result = dict(
            result_code=0,
            result_stdout=state_path,
            message=MAGIC_OK_MESSAGE
        )

        with open(state_path) as file:
            index = dict(zip(*json.load(file)))

            for key in ('processed', 'sample', 'cache'):
                if not index[key] and not index.get('skipped'):
                    result.update(result_code=-1, message='Failed to produce {} data'.format(key))

            index_dirname = os.path.dirname(state_path)
            result['state'] = assemble_runstate(s3, index, source_name, run_id, index_dirname)

        shutil.rmtree(workdir)

        results.append(result)

    return results
