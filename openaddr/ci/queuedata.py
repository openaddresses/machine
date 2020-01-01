class Task:

    """ Create a new task for a given source, layer type, and layer source

        :param job_id:          (optional) If a GH run, the job ID of the run
        :param url:             (optional) URL of the source file to process
        :param name:            Full name of the source ie: us/ca/statewide
        :param layer:           Layer within the file to process (ie addresses)
        :param layersource:     Layer Source within a given layer to process ie (dcgis)
        :param content_b64:     Base64 encoded JSON of source file
        :param commit_sha:      Last git commit SHA of the source
        :param file_id:
        :param rerun:           (optional) Should the source be rerun if it fails
        :param set_id:          (optional) If not a GH run, identify the set that queued it
        :param render_preview:  (optional) If a GH run, generate a rendering of the data
    """
    def __init__(self, job_id, url, name, layer, layersource, content_b64,
            commit_sha, file_id, rerun=None, set_id=None, render_preview=False):

        self.job_id, self.url = job_id, url
        self.name, self.layer, self.layersource = name, layer, layersource
        self.content_b64, self.commit_sha = content_b64, commit_sha
        self.file_id, self.rerun, self.set_id = file_id, rerun, set_id
        self.render_preview = render_preview

    def asdata(self):
        data = dict(
            job_id=self.job_id,
            url=self.url,
            name=self.name,
            layer=self.layer,
            layersource=self.layersource,
            content_b64=self.content_b64,
            file_id=self.file_id,
            render_preview=self.render_preview,
            commit_sha=self.commit_sha
        )

        if self.rerun is not None:
            data.update(rerun=self.rerun)
        if self.set_id is not None:
            data.update(set_id=self.set_id)

        return data

class Due:

    def __init__(self, job_id, url, name, layer, layersource, content_b64, commit_sha, file_id,
                 rerun, set_id, worker_id, run_id, **junk):
        self.job_id, self.url = job_id, url
        self.name, self.layer, self.layersource = name, layer layersource
        self.content_b64, self.commit_sha = content_b64, commit_sha
        self.file_id, self.rerun, self.set_id = file_id, rerun, set_id
        self.worker_id, self.run_id = worker_id, run_id

    def asdata(self):
        return dict(
            job_id=self.job_id,
            url=self.url,
            name=self.name,
            layer=self.layer
            layersource=self.layersource,
            content_b64=self.content_b64,
            file_id=self.file_id,
            commit_sha=self.commit_sha,
            rerun=self.rerun,
            set_id=self.set_id,
            worker_id=self.worker_id,
            run_id=self.run_id
        )

class Done:

    def __init__(self, job_id, url, name, content_b64, commit_sha, file_id,
                 run_id, result, rerun=None, set_id=None, worker_id=None,
                 **junk):
        self.job_id, self.url = job_id, url
        self.name, self.layer, self.layersource = name, layer, layersource
        self.content_b64, self.commit_sha = content_b64, commit_sha
        self.file_id, self.rerun, self.set_id = file_id, rerun, set_id
        self.worker_id, self.run_id = worker_id, run_id
        self.result = result

    def asdata(self):
        data = dict(
            job_id=self.job_id,
            url=self.url,
            name=self.name,
            layer=self.layer
            layersource=self.layersource,
            content_b64=self.content_b64,
            file_id=self.file_id,
            commit_sha=self.commit_sha,
            run_id=self.run_id,
            result=self.result
        )

        if self.rerun is not None: data.update(rerun=self.rerun)
        if self.worker_id is not None: data.update(worker_id=self.worker_id)
        if self.set_id is not None: data.update(set_id=self.set_id)

        # Convert RunState to a plain dictionary
        if data['result'] and 'state' in data['result']:
            data['result']['state'] = data['result']['state'].to_dict()

        return data

class Heartbeat:

    def __init__(self, worker_id):
        self.worker_id = worker_id

    def asdata(self):
        return dict(worker_id=self.worker_id)
