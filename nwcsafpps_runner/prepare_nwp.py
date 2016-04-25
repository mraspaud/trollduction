#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015, 2016 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c20671.ad.smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Prepare NWP data for PPS
"""

import logging
from glob import glob
import os
from datetime import datetime
import ConfigParser
import tempfile
from subprocess import Popen, PIPE

LOG = logging.getLogger(__name__)

CONFIG_PATH = os.environ.get('PPSRUNNER_CONFIG_DIR', './')
CONF = ConfigParser.ConfigParser()
ppsconf_path = os.path.join(CONFIG_PATH, "pps_config.cfg")
LOG.debug("Path to config file = " + str(ppsconf_path))
CONF.read(ppsconf_path)

MODE = os.getenv("SMHI_MODE")
if MODE is None:
    MODE = "offline"

LOG.debug('MODE = ' + str(MODE))

OPTIONS = {}
for option, value in CONF.items(MODE, raw=True):
    OPTIONS[option] = value


try:
    nhsp_path = OPTIONS['nhsp_path']
except KeyError:
    LOG.exception('Parameter not set in config file: ' + 'nhsp_path')
try:
    nhsp_prefix = OPTIONS['nhsp_prefix']
except KeyError:
    LOG.exception('Parameter not set in config file: ' + 'nhsp_prefix')

nhsf_path = OPTIONS.get('nhsf_path', None)
nhsf_prefix = OPTIONS.get('nhsf_prefix', None)
nwp_outdir = OPTIONS.get('nwp_outdir', None)
nwp_lsmz_filename = OPTIONS.get('nwp_static_surface', None)
nwp_output_prefix = OPTIONS.get('nwp_output_prefix', None)
nwp_req_filename = OPTIONS.get('pps_nwp_requirements', None)

import threading


class NwpPrepareError(Exception):
    pass


def logreader(stream, log_func):
    while True:
        s = stream.readline()
        if not s:
            break
        log_func(s.strip())
    stream.close()


def run_command(cmdstr):
    """Run system command"""

    import shlex
    myargs = shlex.split(str(cmdstr))

    LOG.debug("Command: " + str(cmdstr))
    LOG.debug('Command sequence= ' + str(myargs))
    try:
        proc = Popen(myargs, shell=False, stderr=PIPE, stdout=PIPE)
    except NwpPrepareError:
        LOG.exception("Failed when preparing NWP data for PPS...")

    out_reader = threading.Thread(
        target=logreader, args=(proc.stdout, LOG.info))
    err_reader = threading.Thread(
        target=logreader, args=(proc.stderr, LOG.info))
    out_reader.start()
    err_reader.start()
    out_reader.join()
    err_reader.join()

    return proc.returncode


def update_nwp(starttime, nlengths):
    """Prepare NWP grib files for PPS.  Consider only analysis times newer than
    *starttime*. And consider only the forecast lead times in hours given by
    the list *nlengths* of integers

    """

    tempfile.tempdir = nwp_outdir

    filelist = glob(os.path.join(nhsf_path, nhsf_prefix + "*"))
    if len(filelist) == 0:
        LOG.info("No input files! dir = " + str(nhsf_path))
        return

    LOG.debug('NHSF NWP files found = ' + str(filelist))
    for filename in filelist:
        timeinfo = filename.rsplit("_", 1)[-1]
        timestamp, step = timeinfo.split("+")
        analysis_time = datetime.strptime(timestamp, '%Y%m%d%H%M')
        if analysis_time < starttime:
            continue
        if int(step[:3]) not in nlengths:
            continue

        LOG.info("timestamp, step: " + str(timestamp) + ' ' + str(step))
        result_file = os.path.join(
            nwp_outdir, nwp_output_prefix + timestamp + "+" + step)
        if os.path.exists(result_file):
            LOG.info("File: " + str(result_file) + " already there...")
            continue

        tmp_file = os.path.join(nwp_outdir, "tmp." + timestamp + "+" + step)
        LOG.info("result and tmp files: " +
                 str(result_file) + " " + str(tmp_file))
        nhsp_file = os.path.join(nhsp_path, nhsp_prefix + timeinfo)
        if not os.path.exists(nhsp_file):
            LOG.warning("Corresponding nhsp-file not there: " + str(nhsp_file))
            continue

        cmd = ("grib_copy -w gridType=regular_ll " +
               nhsp_file + " " + tmp_file)
        retv = run_command(cmd)
        LOG.debug("Returncode = " + str(retv))

        if not os.path.exists(nwp_lsmz_filename):
            LOG.exception("No static grib file with land-sea mask and " +
                          "topography available. Can't prepare NWP data")
            raise

        tmpresult = tempfile.mktemp()
        cmd = ('cat ' + tmp_file + " " +
               os.path.join(nhsf_path, nhsf_prefix + timeinfo) +
               " " + nwp_lsmz_filename + " > " + tmpresult)
        LOG.debug("Add topography and land-sea mask to data:")
        LOG.debug("Command = " + str(cmd))
        retv = os.system(cmd)
        LOG.debug("Returncode = " + str(retv))
        if retv != 0:
            raise IOError("Failed adding topography and land-sea " +
                          "mask data to grib file")
        os.remove(tmp_file)

        if check_nwp_content(tmpresult):
            LOG.info('A check of the NWP file content has been attempted: %s',
                     result_file)
            os.rename(tmpresult, result_file)
        else:
            LOG.warning("Missing important fields. No nwp file %s written to disk",
                        result_file)

    return


def check_nwp_content(gribfile):
    """Check the content of the NWP file. If all fields required for PPS is
    available, then return True

    """
    import pygrib

    grbs = pygrib.open(gribfile)
    entries = []
    for grb in grbs:
        entries.append("%s %s %s %s" % (grb['paramId'],
                                        grb['name'],
                                        grb['level'],
                                        grb['typeOfLevel']))
    entries.sort()

    try:
        with open(nwp_req_filename, 'r') as fpt:
            lines = fpt.readlines()
    except IOError:
        LOG.exception(
            "Failed reading nwp-requirements file: %s", nwp_req_filename)
        LOG.warning("Cannot check if NWP files is ok!")
        return True

    srplines = [ll.strip('M ').strip('\n')
                for ll in lines if str(ll).startswith('M')]

    file_ok = True
    for item in srplines:
        if not item in entries:
            LOG.warning("Mandatory field missing in NWP file: %s", str(item))
            file_ok = False

    LOG.info("NWP file has all required fields for PPS: %s", gribfile)
    return file_ok

if __name__ == "__main__":

    #: Default time format
    _DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    #: Default log format
    _DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

    import sys
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('posttroll').setLevel(logging.INFO)

    LOG = logging.getLogger('test_update_nwp')

    from datetime import timedelta
    now = datetime.utcnow()
    update_nwp(now - timedelta(days=1), [9])
