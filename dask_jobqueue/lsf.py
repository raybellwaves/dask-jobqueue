from __future__ import absolute_import, division, print_function

import logging
import math
import os

import dask

from .core import JobQueueCluster, docstrings

logger = logging.getLogger(__name__)


class LSFCluster(JobQueueCluster):
    __doc__ = docstrings.with_indents(""" Launch Dask on a LSF cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#BSUB -q` option.
    project : str
        Accounting string associated with each worker job. Passed to
        `#BSUB -P` option.
    walltime : str
        Walltime for each worker job. Passed to `#BSUB -w` option.
    ncpus : Number of cpus. Passed to `#BSUB -n`.
    mempercore : str
        Request megabytes of RAM per core. Pass to `#BSUB -R` option.
    corespernode : str
        Requeest cores per node. Pass to `#BSUB -R` option.
    job_extra : list
        List of other LSF options, for example -e. Each option witll be
        prepended with the #LSF prefix.
    %(JobQueueCluster.parameters).s

    Examples
    -------
    >>> from dask_jobqueue import LSFCluster
    >>> cluster = LSFcluster(queue='general', project='DaskonLSF')
    >>> cluster.start_workers(10)  # this may take a few seconds to launch

    >>> from dask.distributed import Client
    >>> client = Client(cluster)
    """, 4)

    # Override class variable
    submit_command = 'bsub'
    cancel_command = 'bkill'

    def __init__(self,
                 queue=dask.config.get('jobqueue.queue'),
                 project=dask.config.get('jobqueue.project'),
                 walltime=dask.config.get('jobqueue.walltime'),
                 walltime=dask.config.get('jobqueue.ncpus'),
                 mempercore=dask.config.get('jobqueue.mempercore'),
                 corespernode=dask.config.get('jobqueue.corespernode'),
                 job_extra=dask.config.get('jobqueue.ldf.job-extra'),
                 **kwargs):

        # Instantiate args and parameters from parent abstract class
        super(LSFCluster, self).__init__(**kwargs)

        project = project

        header_lines = []
        # LSF header build
        if queue is not None:
            header_lines.append('#BSUB -q %s' % queue)
        if project is not None:
            header_lines.append('#BSUB -P %s' % project)
        if project is not None:
            header_lines.append('#BSUB -P %s' % project)
        if walltime is not None:
            header_lines.append('#BSUB -w %s' % walltime)
        if ncpus is not None:
            header_lines.append('#BSUB -n %s' % ncpus)
        if mempercore is not None:
            header_lines.append('#BSUB -R "rusage[mem=%s]"' % mempercore)
        if corespernode is not None:
            header_lines.append('#BSUB -R "span[ptile=%s]"' % corespernode)
        header_lines.extend(['#PBS %s' % arg for arg in job_extra])

        # Declare class attribute that shall be overriden
        self.job_header = '\n'.join(header_lines)

        logger.debug("Job script: \n %s" % self.job_script())

    def _job_id_from_submit_output(self, out):
        return out.split('.')[0].strip()

