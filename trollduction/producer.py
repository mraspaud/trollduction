# -*- coding: utf-8 -*-
#
# Copyright (c) 2014, 2015
#
# Author(s):
#
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
#   Martin Raspaud <martin.raspaud@smhi.se>
#
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

'''Trollduction module

TODO:
 - adjust docstrings to sphinx (see check_sunzen())
 - change messages to follow the posttroll way of doing
 - save data/images in a separate writer thread to avoid the I/O bottleneck
 - write a command receiver (for messages like reload_config/shutdown/restart)
 - implement a command sender also
 - load default config in case some parameters are missing in the config.ini
 - allow adding custom options per file for saving (eg format,
   tiles/stripes/tifftags)
 - add area-by-area selection for projection method
   (crude/nearest/<something new>)
'''

from .listener import ListenerContainer
from mpop.satellites import GenericFactory as GF
import time
import datetime
import glob
from mpop.projector import get_area_def
from threading import Thread
from pyorbital import astronomy
import numpy as np
import os
import Queue
import logging
import logging.handlers
from fnmatch import fnmatch
from trollduction import helper_functions
from trollsift import compose
from trollsift import Parser
from urlparse import urlparse, urlunsplit
import socket
import shutil
from mpop.satout.cfscene import CFScene
from posttroll.publisher import Publish
from posttroll.message import Message
from pyresample.utils import AreaNotFound
from trollsched.satpass import Pass
from trollsched.boundary import Boundary, AreaDefBoundary
import errno
import netifaces
try:
    from mipp import DecodeError
except ImportError:
    DecodeError = IOError


try:
    from mem_top import mem_top
except ImportError:
    mem_top = None
else:
    import gc
    import pprint

from xml.etree.ElementTree import tostring
from struct import error as StructError

from dwd_extensions.tools.view_zenith_angle import ViewZenithAngleCacheManager

LOGGER = logging.getLogger(__name__)

# Config watcher stuff

import pyinotify


def get_local_ips():
    inet_addrs = [netifaces.ifaddresses(iface).get(netifaces.AF_INET)
                  for iface in netifaces.interfaces()]
    ips = []
    for addr in inet_addrs:
        if addr is not None:
            for add in addr:
                ips.append(add['addr'])
    return ips


def is_uri_on_server(uri, strict=False):
    """Checks if the *uri* is designating a place on the server. If *strict* is True, the hostname has to be specified
    in the *uri* for the path to be considered valid.
    """
    url = urlparse(uri)
    try:
        url_ip = socket.gethostbyname(url.hostname)
    except (socket.gaierror, TypeError):
        if strict:
            return False
        try:
            os.stat(url.path)
        except OSError:
            return False
    else:
        if url.hostname == '':
            if strict:
                return False
            try:
                os.stat(url.path)
            except OSError:
                return False
        elif url_ip not in get_local_ips():
            return False
    return True


def check_uri(uri):
    """Check that the provided *uri* is on the local host and return the
    file path.
    """
    if isinstance(uri, (list, set, tuple)):
        paths = [check_uri(ressource) for ressource in uri]
        return paths
    url = urlparse(uri)
    try:
        if url.hostname:
            url_ip = socket.gethostbyname(url.hostname)

            if url_ip not in get_local_ips():
                try:
                    os.stat(url.path)
                except OSError:
                    raise IOError(
                        "Data file %s unaccessible from this host" % uri)

    except socket.gaierror:
        LOGGER.warning("Couldn't check file location, running anyway")

    return url.path

# Generic event handler


class EventHandler(pyinotify.ProcessEvent):

    """Handle events with a generic *fun* function.
    """

    def __init__(self, fun, file_to_watch=None, item=None):
        pyinotify.ProcessEvent.__init__(self)
        self._file_to_watch = file_to_watch
        self._item = item
        self._fun = fun

    def process_file(self, pathname):
        '''Process event *pathname*
        '''
        if self._file_to_watch is None:
            self._fun(pathname, self._item)
        elif fnmatch(self._file_to_watch, os.path.basename(pathname)):
            self._fun(pathname, self._item)

    def process_IN_CLOSE_WRITE(self, event):
        """On closing after writing.
        """
        self.process_file(event.pathname)

    def process_IN_CREATE(self, event):
        """On closing after linking.
        """
        try:
            if os.stat(event.pathname).st_nlink > 1:
                self.process_file(event.pathname)
        except OSError:
            return

    def process_IN_MOVED_TO(self, event):
        """On closing after moving.
        """
        self.process_file(event.pathname)


class ConfigWatcher(object):

    """Watch a given config file and run reload_config.
    """

    def __init__(self, config_file, config_item, reload_config):
        mask = (pyinotify.IN_CLOSE_WRITE |
                pyinotify.IN_MOVED_TO |
                pyinotify.IN_CREATE)
        self.config_file = config_file
        self.config_item = config_item
        self.watchman = pyinotify.WatchManager()

        LOGGER.debug("Setting up watcher for %s", config_file)

        self.notifier = \
            pyinotify.ThreadedNotifier(self.watchman,
                                       EventHandler(reload_config,
                                                    os.path.basename(config_file
                                                                     ),
                                                    self.config_item
                                                    )
                                       )
        self.watchman.add_watch(os.path.dirname(config_file), mask)

    def start(self):
        """Start the config watcher.
        """
        LOGGER.info("Start watching %s", self.config_file)
        self.notifier.start()

    def stop(self):
        """Stop the config watcher.
        """
        LOGGER.info("Stop watching %s", self.config_file)
        self.notifier.stop()


def covers(overpass, area_item):
    try:
        area_def = get_area_def(area_item.attrib['id'])
        min_coverage = float(area_item.attrib.get('min_coverage', 0))
        if min_coverage == 0 or overpass is None:
            return True
        min_coverage /= 100.0
        coverage = overpass.area_coverage(area_def)
        if coverage <= min_coverage:
            LOGGER.info("Coverage too small %.1f%% (out of %.1f%%) with %s",
                        coverage * 100, min_coverage * 100,
                        area_item.attrib['name'])
            return False
        else:
            LOGGER.info("Coverage %.1f%% with %s",
                        coverage * 100, area_item.attrib['name'])

    except AttributeError:
        LOGGER.warning("Can't compute area coverage with %s!",
                       area_item.attrib['name'])
    return True


def get_polygons_positions(datas, frequency=1):

    mask = None
    for data in datas:
        if mask is None:
            mask = data.mask
        else:
            mask = np.logical_or(mask, data.mask)

    polygons = []
    polygon_left = []
    polygon_right = []
    count = 0
    last_valid_line = None
    for line in range(mask.shape[0]):
        if np.any(1 - mask[line, :]):
            indices = np.nonzero(1 - mask[line, :])[0]

            if last_valid_line is not None and last_valid_line != line - 1:
                # close the polygon and start a new one.
                last_indices = np.nonzero(1 - mask[last_valid_line, :])[0]
                if count % frequency != 0:
                    polygon_right.append((last_valid_line, last_indices[-1]))
                    polygon_left.append((last_valid_line, last_indices[0]))
                for indice in last_indices[1:-1:frequency]:
                    polygon_left.append((last_valid_line, indice))
                polygon_left.reverse()
                polygons.append(polygon_left + polygon_right)
                polygon_left = []
                polygon_right = []
                count = 0

                for indice in indices[1::frequency]:
                    polygon_right.append((line, indice))
                polygon_left.append((line, indices[0]))

            elif count % frequency == 0:
                polygon_left.append((line, indices[0]))
                polygon_right.append((line, indices[-1]))
            count += 1
            last_valid_line = line

    if (count - 1) % frequency != 0 and last_valid_line is not None:
        polygon_left.append((last_valid_line, indices[0]))
        polygon_right.append((last_valid_line, indices[-1]))

    polygon_left.reverse()
    result = polygon_left + polygon_right
    if result:
        polygons.append(result)

    return polygons


def get_polygons(datas, area, frequency=1):
    """Get a list of polygons describing the boundary of the area.
    """
    polygons = get_polygons_positions(datas, frequency)

    llpolygons = []
    for poly in polygons:
        lons = []
        lats = []
        for line, col in poly:
            lon, lat = area.get_lonlat(line, col)
            lons.append(lon)
            lats.append(lat)
        lons = np.array(lons)
        lats = np.array(lats)
        llpolygons.append(Boundary(lons, lats))

    return llpolygons


def coverage(scene, area):
    """Calculate coverages.
    """
    shapes = set()
    for channel in scene.channels:
        if channel.is_loaded():
            shapes.add(channel.shape)

    coverages = []

    # from trollsched.satpass import Mapper

    for shape in shapes:

        datas = []
        areas = []
        for channel in scene.channels:
            if channel.is_loaded() and channel.shape == shape:
                datas.append(channel.data)
                areas.append(channel.area)

        disk_polys = [poly.contour_poly
                      for poly
                      in get_polygons(datas, areas[0], frequency=100)]

        area_poly = AreaDefBoundary(area, frequency=100).contour_poly

        inter_area = 0

        for poly in disk_polys:
            inter = poly.intersection(area_poly)
            if inter is not None:
                inter_area += inter.area()

        coverages.append(inter_area / area_poly.area())
    try:
        return min(*coverages)
    except TypeError:
        return coverages[0]


def generic_covers(scene, area_item):
    """Check if scene covers area_item with high enough percentage.
    """
    area_def = get_area_def(area_item.attrib['id'])
    min_coverage = float(area_item.attrib.get('min_coverage', 0))
    if min_coverage == 0:
        return True
    min_coverage /= 100.0
    cov = coverage(scene, area_def)
    if cov <= min_coverage:
        LOGGER.info("Coverage too small %.1f%% (out of %.1f%%) with %s",
                    cov * 100, min_coverage * 100,
                    area_item.attrib['name'])
        return False
    else:
        LOGGER.info("Coverage %.1f%% with %s",
                    cov * 100, area_item.attrib['name'])
        return True


class DataProcessor(object):

    """Process the data.
    """

    def __init__(self, publish_topic=None,
                 nameservers=[],
                 process_num=0,
                 wait_for_channel_cfg={},
                 viewZenCacheManager=None):
        self.global_data = None
        self.local_data = None
        self.product_config = None
        self._publish_topic = publish_topic
        self._data_ok = True
        self.wait_for_channel_cfg = wait_for_channel_cfg
        self.writer = DataWriter(publish_topic=self._publish_topic,
                                 nameservers=nameservers)
        self.writer.start()
        self.process_num = process_num
        self.viewZenCacheManager = viewZenCacheManager

    def set_publish_topic(self, publish_topic):
        '''Set published topic.'''
        self._publish_topic = publish_topic
        self.writer.set_publish_topic(publish_topic)

    def stop(self):
        '''Stop data writer.
        '''
        self.writer.stop()

    def create_scene_from_message(self, msg):
        """Parse the message *msg* and return a corresponding MPOP scene.
        """
        if msg.type in ["file", 'collection', 'dataset']:
            return self.create_scene_from_mda(msg.data)

    def create_scene_from_mda(self, mda):
        """Read the metadata *mda* and return a corresponding MPOP scene.
        """
        time_slot = (mda.get('start_time') or
                     mda.get('nominal_time') or
                     mda.get('end_time'))
        
        scene_time_slot = time_slot
        if 'end_time' in mda:
            scene_time_slot = (scene_time_slot, mda['end_time'])

        # orbit is not given for GEO satellites, use None

        if 'orbit_number' not in mda:
            mda['orbit_number'] = None

        platform = mda["platform_name"]

        LOGGER.info("platform %s time %s",
                    str(platform), str(time_slot))

        if isinstance(mda['sensor'], (list, tuple, set)):
            sensor = mda['sensor'][0]
        else:
            sensor = mda['sensor']

        # Create satellite scene
        global_data = GF.create_scene(satname=str(platform),
                                      satnumber='',
                                      instrument=str(sensor),
                                      time_slot=scene_time_slot,
                                      orbit=mda['orbit_number'],
                                      variant=mda.get('variant', ''))
        LOGGER.debug("Creating scene for satellite %s and time %s",
                     str(platform), str(time_slot))

        check_coverage = \
            self.product_config.attrib.get("check_coverage",
                                           "true").lower() in ("true", "yes",
                                                               "1")

        if check_coverage and (mda['orbit_number'] is not None or
                               mda.get('orbit_type') == "polar"):
            global_data.overpass = Pass(platform,
                                        mda['start_time'],
                                        mda['end_time'],
                                        instrument=sensor)
        else:
            global_data.overpass = None

        # Update missing information to global_data.info{}
        # TODO: this should be fixed in mpop.
        global_data.info.update(mda)
        global_data.info['time'] = time_slot
        global_data.info['time_eos'] = time_slot + datetime.timedelta(minutes=15)
        return global_data

    def save_to_netcdf(self, data, item, params):
        """Save data to netCDF4.
        """
        LOGGER.debug("Save full data to netcdf4")
        try:
            params["time_slot"] = data.time_slot
            params["area"] = data.area
            data.add_to_history(
                "Saved as netcdf4/cf by pytroll/mpop.")
            cfscene = CFScene(data)

            self.writer.write(cfscene, item, params)
            LOGGER.info("Sent netcdf/cf scene to writer.")
        except IOError:
            LOGGER.error("Saving unprojected data to NetCDF failed!")
        finally:
            if item.attrib.get("unload_after_saving", "False") == "True":
                loaded_channels = [ch.name
                                   for ch in data.channels]
                LOGGER.info("Unloading data after netcdf4 conversion.")
                data.unload(*loaded_channels)

    def set_wait_for_channel_cfg(self, wait_for_channel_cfg):
        self.wait_for_channel_cfg = wait_for_channel_cfg
        
    def run(self, product_config, msg):
        """Process the data
        """

        self.product_config = product_config

        if msg.type == "file":
            uri = msg.data['uri']
        elif msg.type == "dataset":
            uri = [mda['uri'] for mda in msg.data['dataset']]
        elif msg.type == 'collection':
            all_areas = self.get_area_def_names()
            if not msg.data['collection_area_id'] in all_areas:
                LOGGER.info('Collection does not contain data for '
                            'current areas. Skipping.')
                return
            if 'dataset' in msg.data['collection'][0]:
                uri = []
                for dataset in msg.data['collection']:
                    uri.extend([mda['uri'] for mda in dataset['dataset']])
            else:
                uri = [mda['uri'] for mda in msg.data['collection']]
        else:
            LOGGER.warning("Can't run on %s messages", msg.type)
            return
        # TODO collections and collections of datasets

        LOGGER.info('New data available: %s', uri)
        t1a = time.time()

        try:
            filename = check_uri(uri)
            LOGGER.debug(str(filename))
        except IOError as err:
            LOGGER.info(str(err))
            LOGGER.info("Skipping...")
            return

        self.global_data = self.create_scene_from_message(msg)
        self._data_ok = True

        nprocs = int(self.product_config.attrib.get("nprocs", 1))
        proj_method = self.product_config.attrib.get("proj_method", "nearest")
        LOGGER.info("Using %d CPUs for reprojecting with method %s.",
                    nprocs, proj_method)
        precompute = \
            self.product_config.attrib.get("precompute", "").lower() in \
            ["true", "yes", "1"]
        if precompute:
            LOGGER.debug("Saving projection mapping for re-use")
        use_extern_calib = \
            self.product_config.attrib.get("use_extern_calib", "").lower() in \
            ["true", "yes", "1"]
        keywords = {"use_extern_calib": use_extern_calib}

        for area_item in self.product_config.prodlist:
            if area_item.tag == "dump":
                try:
                    self.global_data.load(filename=filename, **keywords)
                    self.save_to_netcdf(self.global_data,
                                        area_item,
                                        self.get_parameters(area_item))
                except (IndexError, IOError, DecodeError, StructError):
                    LOGGER.exception("Incomplete or corrupted input data.")

        for group in self.product_config.groups:
            LOGGER.debug("processing %s", group.info['id'])
            area_def_names = self.get_area_def_names(group.data)
            if msg.type == 'collection' and \
               not msg.data['collection_area_id'] in area_def_names:
                LOGGER.info('Collection data does not cover this area group. '
                            'Skipping.')
                continue

            products = []
            skip = []
            skip_group = True
            do_generic_coverage = False

            for area_item in group.data:

                if 'process_num' in area_item.attrib:
                    if eval(area_item.attrib['process_num']) != self.process_num:
                        LOGGER.info('Skipping area %s assigned to process '
                                    'number %s (own num: %s)'
                                    % (area_item.attrib['id'],
                                       area_item.attrib['process_num'],
                                       self.process_num))
                        skip.append(area_item)
                        continue

                try:
                    if not covers(self.global_data.overpass, area_item):
                        skip.append(area_item)
                        continue
                    else:
                        skip_group = False
                except AttributeError:
                    LOGGER.exception("Can't compute coverage from "
                                     "unloaded data, continuing")
                    do_generic_coverage = True
                    skip_group = False
                for product in area_item:
                    products.append(product)
            if not products or skip_group:
                continue

            if group.get("unload", "").lower() in ["yes", "true", "1"]:
                loaded_channels = [chn.name for chn
                                   in self.global_data.loaded_channels()]
                self.global_data.unload(*loaded_channels)
                LOGGER.debug("unloading all channels before group %s",
                             group.id)
            try:
                req_channels = self.get_req_channels(products)
                LOGGER.debug("loading channels: %s", str(req_channels))
                keywords = {"filename": filename,
                            "area_def_names": area_def_names,
                            "use_extern_calib": use_extern_calib}
                try:
                    keywords["time_interval"] = (msg.data["start_time"],
                                                 msg.data["end_time"])
                except KeyError:
                    pass
                if "resolution" in group.info:
                    keywords["resolution"] = int(group.resolution)

                self.check_ready_to_read(req_channels)
                
                self.global_data.load(req_channels, **keywords)
                LOGGER.debug("loaded data: %s", str(self.global_data))
            except (IndexError, IOError, DecodeError, StructError):
                LOGGER.exception("Incomplete or corrupted input data.")
                self._data_ok = False
                break

            for area_item in group.data:
                if area_item in skip:
                    continue
                elif (do_generic_coverage and
                      not generic_covers(self.global_data, area_item)):
                    continue

                # retrieve the satellite zenith angles for the corresponding area
                self.viewZenCacheManager.prepare(msg,
                                                 area_item.attrib['id'],
                                                 self.global_data.info['time'])

                # update viewZenCacheManager with loaded channels to notify about
                # satellite position infos
                self.viewZenCacheManager.notify_channels_loaded(
                    self.global_data.loaded_channels())

                # reproject to local domain
                LOGGER.debug("Projecting data to area %s",
                             area_item.attrib['name'])
                try:
                    self.local_data = \
                        self.global_data.project(
                            area_item.attrib["id"],
                            channels=self.get_req_channels(area_item),
                            mode=proj_method, nprocs=nprocs,
                            precompute=precompute)
                except ValueError:
                    LOGGER.warning("No data in this area")
                    continue
                except AreaNotFound:
                    LOGGER.warning("Area %s not defined, skipping!",
                                   area_item.attrib['id'])
                    continue

                LOGGER.info('Data reprojected for area: %s',
                            area_item.attrib['name'])

                # create a shallow copy of the info dictionary in local_data
                # to provide information which should be local only
                # independent from the global_data object
                self.local_data.info = self.local_data.info.copy()
                # do the same for each channel in local_data
                for chn in self.local_data.channels:
                    chn.info = chn.info.copy()

                # wait for the satellite zenith angle calculation process
                vza_chn = self.viewZenCacheManager.waitForViewZenithChannel()
                self.local_data.channels.append(vza_chn)

                # Draw requested images for this area.
                self.draw_images(area_item)
                del self.local_data
                self.local_data = None

            if group.get("unload", "").lower() in ["yes", "true", "1"]:
                loaded_channels = [chn.name for chn
                                   in self.global_data.loaded_channels()]
                self.global_data.unload(*loaded_channels)
                LOGGER.debug("unloading all channels after group %s",
                             group.id)

        # Wait for the writer to finish
        if self._data_ok:
            LOGGER.debug("Waiting for the files to be saved")
        self.writer.prod_queue.join()

        self.release_memory()

        if self._data_ok:
            LOGGER.debug("All files saved")
            LOGGER.info("File %s processed in %.1f s", uri,
                        time.time() - t1a)

        if not self._data_ok:
            LOGGER.warning("File %s not processed due to "
                           "incomplete/missing/corrupted data.",
                           uri)
            raise IOError

    def release_memory(self):
        """Run garbage collection for diagnostics"""
        if mem_top is not None:
            LOGGER.debug(mem_top())
        # Release memory
        del self.local_data
        del self.global_data
        self.local_data = None
        self.global_data = None

        if mem_top is not None:
            gc_res = gc.collect()
            LOGGER.debug("Unreachable objects: %d", gc_res)
            LOGGER.debug('Remaining Garbage: %s', pprint.pformat(gc.garbage))
            del gc.garbage[:]
            LOGGER.debug(mem_top())

    def get_req_channels(self, products):
        """Get a list of required channels
        """
        reqs = set()
        for product in products:
            if product.tag == "dump":
                return None
            try:
                composite = getattr(self.global_data.image,
                                    product.attrib['id'])
                reqs |= composite.prerequisites
            except AttributeError:
                LOGGER.info("Composite %s not available",
                            product.attrib['id'])
        return reqs

    def get_area_def_names(self, group=None):
        '''Collect and return area definition names from product
        config to a list.
        '''

        prodlist = group or self.product_config.prodlist

        def_names = [item.attrib["id"]
                     for item in prodlist
                     if item.tag == "area"]

        return def_names

    def check_ready_to_read(self, channels_to_load):
        lcase_channels_to_load = [str(x).lower() for x in channels_to_load]
        LOGGER.debug(
            'check if ready to load:: %s', ', '.join(lcase_channels_to_load))
        for ch_name, wait_for_ch_cfg in self.wait_for_channel_cfg.iteritems():
            if ch_name in lcase_channels_to_load:
                par = Parser(wait_for_ch_cfg['pattern'])
                info_dict = self.get_parameters()
                pattern = par.compose(info_dict)
                if self.wait_until_exists(pattern,
                                     wait_for_ch_cfg['timeout'],
                                     wait_for_ch_cfg['wait_after_found']):
                    LOGGER.debug('found %s', pattern)
                else:
                    LOGGER.error('timeout! did not found %s', pattern)

    def wait_until_exists(self, pattern, timeout_sec, wait_after_found_sec):
        ''' waits for files matching the given pattern with
        '''
        waited = 0
        wait_period = 5

        while waited < timeout_sec:
            if glob.glob(pattern):
                time.sleep(wait_after_found_sec)
                return True
            time.sleep(wait_period)
            waited += wait_period

        return False

    def check_satellite(self, config):
        '''Check if the current configuration allows the use of this
        satellite.
        '''

        # Check the list of valid satellites
        if 'valid_satellite' in config.keys():
            if self.global_data.info['platform_name'] not in +\
                    config.attrib['valid_satellite']:

                info = 'Satellite %s not in list of valid ' \
                    'satellites, skipping product %s.' % \
                    (self.global_data.info['platform_name'],
                     config.attrib['name'])
                LOGGER.info(info)

                return False

        # Check the list of invalid satellites
        if 'invalid_satellite' in config.keys():
            if self.global_data.info['platform_name'] in \
                    config.attrib['invalid_satellite']:

                info = 'Satellite %s is in the list of invalid ' \
                    'satellites, skipping product %s.' % \
                    (self.global_data.info['platform_name'],
                     config.attrib['name'])
                LOGGER.info(info)

                return False

        return True

    def get_parameters(self, item = None):
        """Get the parameters for filename sifting.
        """

        params = self.product_config.attrib.copy()

        params.update(self.global_data.info)
        if item is not None:
            for key, attrib in item.attrib.items():
                params["".join((item.tag, key))] = attrib
            params.update(item.attrib)

        params['aliases'] = self.product_config.aliases.copy()

        return params

    
    def draw_images(self, area):
        '''Generate images from local data using given area name and
        product definitions.
        '''

        params = self.get_parameters(area)
        # Create images for each color composite
        for product in area:
            params.update(self.get_parameters(product))
            if product.tag == "dump":
                try:
                    self.save_to_netcdf(self.local_data,
                                        product,
                                        params)
                except IOError:
                    LOGGER.error("Saving projected data to NetCDF failed!")
                continue
            elif product.tag != "product":
                continue
            # TODO
            # Check if satellite is one that should be processed
            if not self.check_satellite(product):
                # Skip this product, if the return value is True
                continue

            # Check if Sun zenith angle limits match this product
            if 'sunzen_night_minimum' in product.attrib or \
                    'sunzen_day_maximum' in product.attrib:
                if 'sunzen_xy_loc' in product.attrib:
                    xy_loc = [int(x) for x in
                              product.attrib['sunzen_xy_loc'].split(',')]
                    lonlat = None
                else:
                    xy_loc = None
                    if 'sunzen_lonlat' in product.attrib:
                        lonlat = [float(x) for x in
                                  product.attrib['sunzen_lonlat'].split(',')]
                    else:
                        lonlat = None
                if not self.check_sunzen(product.attrib,
                                         area_def=get_area_def(
                                             area.attrib['id']),
                                         xy_loc=xy_loc, lonlat=lonlat):
                    # If the return value is False, skip this product
                    continue

            try:
                # Collect optional composite parameters from config
                composite_params = {}
                cp = product.find('composite_params')
                if cp is not None:
                    composite_params = dict(
                        (item.tag, helper_functions.eval_default(item.text))
                        for item in cp.getchildren())

                # Check if this combination is defined
                func = getattr(self.local_data.image, product.attrib['id'])
                LOGGER.debug("Generating composite \"%s\"",
                             product.attrib['id'])
                img = func(**composite_params)
                img.info.update(self.global_data.info)
                img.info["product_name"] = \
                    product.attrib.get("name", product.attrib["id"])
            except AttributeError as err:
                # Log incorrect product funcion name
                LOGGER.error('Incorrect product id: %s for area %s (%s)',
                             product.attrib['id'], area.attrib['name'], str(err))
            except KeyError as err:
                # log missing channel
                LOGGER.warning('Missing channel on product %s for area %s: %s',
                               product.attrib['name'], area.attrib['name'],
                               str(err))
            except Exception:
                # log other errors
                LOGGER.exception('Error on product %s for area %s',
                                 product.attrib['name'],
                                 area.attrib['name'])
            else:
                file_items = [x for x in product if x.tag == 'file']
                self.writer.write(img, file_items, params)

        # log and publish completion of this area def
        LOGGER.info('Area %s completed', area.attrib['name'])

    def check_sunzen(self, config, area_def=None, xy_loc=None, lonlat=None,
                     data_name='local_data'):
        '''Check if the data is within Sun zenith angle limits.

        :param config: configuration options for this product
        :type config: dict
        :param area_def: area definition of the data
        :type area_def: areadef
        :param xy_loc: pixel location (2-tuple or 2-element list with x- and y-coordinates) where zenith angle limit is checked
        :type xy_loc: tuple
        :param lonlat: longitude/latitude location (2-tuple or 2-element list with longitude and latitude) where zenith angle limit is checked.
        :type lonlat: tuple
        :param data_name: name of the dataset to get data from
        :type data_name: str

        If both *xy_loc* and *lonlat* are None, image center is used as reference point. *xy_loc* overrides *lonlat*.
        '''

        LOGGER.info('Checking Sun zenith angle limits')
        try:
            data = getattr(self, data_name)
        except AttributeError:
            LOGGER.error('No such data: %s', data_name)
            return False

        if area_def is None and xy_loc is None:
            LOGGER.error('No area definition or pixel location given')
            return False

        # Check availability of coordinates, load if necessary
        if data.area.lons is None:
            LOGGER.debug('Load coordinates for %s', data_name)
            data.area.lons, data.area.lats = data.area.get_lonlats()

        # Check availability of Sun zenith angles, calculate if necessary
        try:
            data.__getattribute__('sun_zen')
        except AttributeError:
            LOGGER.debug('Calculating Sun zenith angles for %s', data_name)
            data.sun_zen = astronomy.sun_zenith_angle(data.time_slot,
                                                      data.area.lons,
                                                      data.area.lats)

        if xy_loc is not None and len(xy_loc) == 2:
            # Use the given xy-location
            x_idx, y_idx = xy_loc
        else:
            if lonlat is not None and len(lonlat) == 2:
                # Find the closest pixel to the given coordinates
                dists = (data.area.lons - lonlat[0]) ** 2 + \
                    (data.area.lats - lonlat[1]) ** 2
                y_idx, x_idx = np.where(dists == np.min(dists))
                y_idx, x_idx = int(y_idx), int(x_idx)
            else:
                # Use image center
                y_idx = int(area_def.y_size / 2)
                x_idx = int(area_def.x_size / 2)

        # Check if Sun is too low (day-only products)
        try:
            LOGGER.debug('Checking Sun zenith-angle limit at '
                         '(lon, lat) %3.1f, %3.1f (x, y: %d, %d)',
                         data.area.lons[y_idx, x_idx],
                         data.area.lats[y_idx, x_idx],
                         x_idx, y_idx)

            if float(config['sunzen_day_maximum']) < \
                    data.sun_zen[y_idx, x_idx]:
                LOGGER.info('Sun too low for day-time product.')
                return False
        except KeyError:
            pass

        # Check if Sun is too high (night-only products)
        try:
            if float(config['sunzen_night_minimum']) > \
                    data.sun_zen[y_idx, x_idx]:
                LOGGER.info('Sun too high for night-time '
                            'product.')
                return False
        except KeyError:
            pass

        return True


def _create_message(obj, filename, uri, params, publish_topic=None, uid=None, source_uri=None):
    """Create posttroll message.
    """
    to_send = obj.info.copy()

    for key in ['collection', 'dataset']:
        if key in to_send:
            del to_send[key]

    to_send["nominal_time"] = getattr(obj, "time_slot",
                                      params.get("time_slot"))
    area = getattr(obj, "area", params.get("area"))

    to_send["area"] = {}
    try:
        to_send["area"]["name"] = area.name
        to_send["area"]["id"] = area.area_id
        try:
            to_send["area"]["proj_id"] = area.proj_id
            to_send["area"]["proj4"] = area.proj4_string
            to_send["area"]["area_extent"] = area.area_extent
            to_send["area"]["shape"] = area.x_size, obj.area.y_size
        except AttributeError:
            pass
    except AttributeError:
        del to_send["area"]

    # FIXME: fishy: what if the uri already has a scheme ?
    to_send["uri"] = urlunsplit(("file", "", uri, "", ""))
    to_send["uid"] = uid or os.path.basename(filename)
    to_send["source_uri"] = source_uri
    
    # we should have more info on format...
    fformat = os.path.splitext(filename)[1][1:]
    if fformat.startswith("tif"):
        fformat = "TIFF"
    elif fformat.startswith("png"):
        fformat = "PNG"
    elif fformat.startswith("jp"):
        fformat = "JPEG"
    elif fformat.startswith("nc"):
        fformat = "NetCDF4"
    elif fformat in ["hdf", "h5"]:
        fformat = "HDF5"
    to_send["type"] = fformat
    if fformat not in ["NetCDF4", "HDF5", "TIFF"]:
        to_send["format"] = "raster"
        to_send["data_processing_level"] = "2"
        to_send["product_name"] = obj.info["product_name"]
    elif fformat == "TIFF":
        to_send["format"] = "GeoTIFF"
        to_send["data_processing_level"] = "2"
        to_send["product_name"] = obj.info["product_name"]
    elif fformat == "NetCDF4":
        to_send["format"] = "CF"
        to_send["data_processing_level"] = "1b"
        to_send["product_name"] = "dump"
    elif fformat == "HDF5":
        to_send["format"] = "PPS"
        to_send["data_processing_level"] = "3"
        to_send["product_name"] = "cloudproduct"

    if publish_topic is None:
        subject = "/".join(("",
                            to_send["format"],
                            to_send["data_processing_level"]))
    else:
        # TODO: this is ugly, but still the easiest way to get area id
        # for the compose as dict key "id"
        compose_dict = {}
        for key in to_send:
            if isinstance(to_send[key], dict):
                for key2 in to_send[key]:
                    compose_dict[key2] = to_send[key][key2]
            else:
                compose_dict[key] = to_send[key]
        subject = compose(publish_topic, compose_dict)

    msg = Message(subject, "file", to_send)

    return msg


def link_or_copy(src, dst, retry=1):
    """Create a hardlink from *src* to *dst*, or if that fails, copy.
    """
    if src == dst:
        LOGGER.warning("Trying to copy a file over itself: %s", src)
        return
    try:
        os.link(src, dst)
    except OSError as err:
        if err.errno not in [errno.EXDEV]:
            LOGGER.exception("Could not link: %s -> %s", src, dst)
            return
        try:
            shutil.copy(src, dst)
        except shutil.Error:
            LOGGER.exception("Something went wrong in copying a file")
        except IOError as err:
            LOGGER.info("Error copying file: %s", str(err))
            if retry:
                LOGGER.info("Retrying...")
                link_or_copy(src, dst, retry - 1)
            else:
                LOGGER.exception("Could not copy: %s -> %s", src, dst)



def thumbnail(filename, thname, size, fformat):
    """Create a thumbnail image and save it.
    """
    from PIL import Image
    img = Image.open(filename)
    img.thumbnail(size, Image.ANTIALIAS)
    img.save(thname, fformat)


def hash_color(colorstring):
    """ convert #RRGGBB to an (R, G, B) tuple """
    colorstring = colorstring.strip()
    if colorstring[0] == '#':
        colorstring = colorstring[1:]
    if len(colorstring) != 6:
        raise ValueError("input #%s is not in #RRGGBB format" % colorstring)
    r_col, g_col, b_col = colorstring[:2], colorstring[2:4], colorstring[4:]
    r_col, g_col, b_col = [int(n, 16) for n in (r_col, g_col, b_col)]
    return (r_col, g_col, b_col)


class DataWriter(Thread):

    """Writes data to disk.

    This is separate from the DataProcessor since it takes IO time and
    we don't want to block processing.
    """

    def __init__(self, publish_topic=None, nameservers=[]):
        Thread.__init__(self)
        self.prod_queue = Queue.Queue()
        self._publish_topic = publish_topic
        self._nameservers = nameservers
        self._loop = True

    def set_publish_topic(self, publish_topic):
        '''Set published topic.'''
        self._publish_topic = publish_topic

    def run(self):
        """Run the thread.
        """
        with Publish("l2producer", nameservers=self._nameservers) as pub:
            while self._loop:
                try:
                    obj, file_items, params = self.prod_queue.get(True, 1)
                except Queue.Empty:
                    continue
                local_params = params.copy()
                try:
                    # Sort the file items in categories, to allow copying
                    # similar ones.
                    sorted_items = {}
                    for item in file_items:
                        attrib = item.attrib.copy()
                        for key in ["output_dir",
                                    "thumbnail_name",
                                    "thumbnail_size"]:
                            if key in attrib:
                                del attrib[key]
                        if 'format' not in attrib:
                            attrib.setdefault('format',
                                              os.path.splitext(item.text.strip())[1][1:])

                        key = tuple(sorted(attrib.items()))
                        sorted_items.setdefault(key, []).append(item)

                    local_aliases = local_params['aliases']
                    for key, aliases in local_aliases.items():
                        if key in local_params:
                            local_params[key] = aliases.get(params[key],
                                                            params[key])
                    for item, copies in sorted_items.items():
                        attrib = dict(item)
                        if attrib.get("overlay", "").startswith("#"):
                            obj.add_overlay(hash_color(attrib.get("overlay")))
                        elif len(attrib.get("overlay", "")) > 0:
                            LOGGER.debug("Adding overlay from config file")
                            obj.add_overlay_config(attrib["overlay"])
                        fformat = attrib.get("format")

                        # Actually save the data to disk.
                        saved = False
                        for copy in copies:
                            output_dir = copy.attrib.get("output_dir",
                                                         params["output_dir"])

                            fname = compose(os.path.join(output_dir, copy.text.strip()),
                                            local_params)
                            
                            save_params = self.get_save_arguments(copy, local_params)
                            obj.repl_fill_val_in_data = int(save_params.pop("repl_fill_val_in_data", None))
                            # LOGGER.info("!!! arealon %s arealat %s areasap %s", obj.area.lons.shape, obj.area.lats.shape, obj.area.shape)
                            LOGGER.debug("Saving %s", fname)
                            if not saved:
                                try:
                                    obj.save(fname,
                                             fformat=fformat,
                                             **save_params)
                                except IOError: # retry once
                                    try:
                                        obj.save(fname,
                                                 fformat=fformat,
                                                 **save_params)
                                    except IOError:
                                        LOGGER.exception("Can't save file %s", fname)
                                        continue

                                LOGGER.info("Saved %s to %s", str(obj), fname)
                                saved = fname
                                uid = os.path.basename(fname)
                            else:
                                link_or_copy(saved, fname)
                                LOGGER.info("Copied/Linked %s to %s", saved, fname)
                                saved = fname
                            if ("thumbnail_name" in copy.attrib and
                                    "thumbnail_size" in copy.attrib):
                                thsize = [int(val) for val
                                          in copy.attrib[
                                    "thumbnail_size"].split("x")]
                                copy.attrib["thumbnail_size"] = thsize
                                thname = \
                                    compose(os.path.join(
                                        output_dir,
                                        copy.attrib["thumbnail_name"]),
                                        local_params)
                                copy.attrib["thumbnail_name"] = thname
                                thumbnail(fname, thname, thsize, fformat)

                            if 'uri' in params:
                                source_uri = [params['uri']]
                            elif 'dataset' in params:
                                source_uri = [e['uri'] for e in params['dataset'] if 'uri' in e]
                            else:
                                source_uri = None

                            msg = _create_message(obj, os.path.basename(fname),
                                                  fname, params,
                                                  publish_topic=self._publish_topic,
                                                  uid=uid, source_uri=source_uri)
                            pub.send(str(msg))
                            LOGGER.debug("Sent message %s", str(msg))
                except Exception as e:
                    for item in file_items:
                        if "thumbnail_size" in item.attrib:
                            item.attrib["thumbnail_size"] = str(
                                item.attrib["thumbnail_size"])
                    LOGGER.exception("Something wrong happened saving "
                                     "%s to %s: %s (%s)",
                                     str(obj),
                                     str([tostring(item)
                                          for item in file_items]),
                                     e.message,
                                     local_params)
                finally:
                    self.prod_queue.task_done()

    def get_save_arguments(self, fileelem, params):
        save_kwords = {}

        fp = fileelem.find('format_params')
        if fp is not None:
            fpp = dict((item.tag, item.text) for item in fp.getchildren())
            save_kwords.update(fpp)

        if 'format_params' in fileelem:
            save_kwords.update(fileelem['format_params'])

        # set some defaults
        if 'compression' not in save_kwords:
            save_kwords['compression'] = 6

        if 'blocksize' not in save_kwords:
            save_kwords['blocksize'] = 0

        if 'nbits' in save_kwords:
            save_kwords['tags'] = {'NBITS': save_kwords['nbits']}
            del save_kwords['nbits']
        elif 'nbits' in params:
            save_kwords['tags'] = {'NBITS':
                                   params['nbits']}
            
        if 'repl_fill_val_in_data' not in save_kwords:
            save_kwords['repl_fill_val_in_data'] = params.get('repl_fill_val_in_data', None)
            
        return save_kwords
    
    def write(self, obj, item, params):
        '''Write to queue.
        '''
        self.prod_queue.put((obj, list(item), params.copy()))

    def stop(self):
        '''Stop the data writer.
        '''
        LOGGER.info("stopping data writer")
        self._loop = False


class Trollduction(object):

    """Trollduction takes in messages and generates DataProcessor jobs.
    """

    def __init__(self, config, managed=True):
        LOGGER.debug("Trollduction starting now")

        self.td_config = None
        self.product_config = None
        self.listener = None

        self.global_data = None
        self.local_data = None

        self._loop = True
        self.thr = None

        self.data_processor = None
        self.config_watcher = None
        self.viewZenCacheManager = None

        self._previous_pass = {"platform_name": None,
                               "start_time": None}

        # read everything from the Trollduction config file
        try:
            self.update_td_config_from_file(config['config_file'],
                                            config['config_item'])
            if not managed:
                self.config_watcher = \
                    ConfigWatcher(config['config_file'],
                                  config['config_item'],
                                  self.update_td_config_from_file)
                self.config_watcher.start()

        except AttributeError:
            LOGGER.error('Error during initialization: %s', e)
            self.td_config = config
            self.update_td_config()

        aliases = helper_functions.parse_aliases(self.td_config)
        self.viewZenCacheManager = ViewZenithAngleCacheManager(
                self.td_config.get('tle_path', ''), aliases)

        nameservers = self.td_config.get('nameservers','').split(',')
        
        self.data_processor = \
            DataProcessor(publish_topic=self.td_config.get('publish_topic',
                                                           None),
                          nameservers=nameservers,
                          process_num = config["process_num"],
                          wait_for_channel_cfg=self.wait_for_channel_cfg,
                          viewZenCacheManager=self.viewZenCacheManager)

    def update_td_config_from_file(self, fname, config_item=None):
        '''Read Trollduction config file and use the new parameters.
        '''
        self.td_config = helper_functions.read_config_file(fname, config_item)
        self.update_td_config()

    def update_td_config(self):
        '''Setup Trollduction with the loaded configuration.
        '''

        LOGGER.info('Trollduction configuration read successfully.')

        # Initialize/restart listener
        if self.listener is None:
            self.listener = \
                ListenerContainer(topics=self.td_config['topics'].split(','))
#            self.listener = ListenerContainer()
            LOGGER.info("Listener started")
        else:
            #            self.listener.restart_listener('file')
            self.listener.restart_listener(self.td_config['topics'].split(','))
            LOGGER.info("Listener restarted")
        self.set_wait_for_channel_cfg()
        try:
            self.update_product_config(self.td_config['product_config_file'])
        except KeyError:
            LOGGER.exception("Key 'product_config_file' is "
                             "missing from Trollduction config")

    def update_product_config(self, fname):
        '''Update area definitions, associated product names, output
        filename prototypes and other relevant information from the
        given file.
        '''
        import xml_read

        self.product_config = xml_read.ProductList(fname)

        # add checks, or do we just assume the config to be valid at
        # this point?
        # self.product_config = product_config
        if self.td_config['product_config_file'] != fname:
            self.td_config['product_config_file'] = fname

        LOGGER.info('Product config read from %s', fname)

    def set_wait_for_channel_cfg(self):
        '''Parses configuration to waiting for a channel
        '''
        key_prefix = 'wait_for_channel_'
        wait_for_channel_cfg = {}
        for key, value in self.td_config.iteritems():
            if key.startswith(key_prefix):
                ch_name = key[len(key_prefix):]
                vals = value.split('|')
                pattern = vals[0]
                timeout = eval(vals[1])
                wait_after_found = eval(vals[2])
                wait_for_channel_cfg[ch_name] = {
                    'pattern': pattern,
                    'timeout': timeout,
                    'wait_after_found': wait_after_found}
        self.wait_for_channel_cfg = wait_for_channel_cfg
        if self.data_processor is not None:
            self.data_processor.set_wait_for_channel_cfg(wait_for_channel_cfg)

    def cleanup(self):
        '''Cleanup Trollduction before shutdown.
        '''

        # more cleanup needed?
        if self._loop:
            LOGGER.info('Shutting down Trollduction.')
            self._loop = False
            if self.data_processor is not None:
                self.data_processor.stop()
            if self.config_watcher is not None:
                self.config_watcher.stop()
            if self.listener is not None:
                self.listener.stop()

            self.data_processor = None
            
            if self.viewZenCacheManager is not None:
                self.viewZenCacheManager.shutdown()
                self.viewZenCacheManager = None

    def stop(self):
        """Stop running.
        """
        self.cleanup()

    def shutdown(self):
        '''Shutdown trollduction.
        '''
        self.stop()

    def run_single(self):
        """Run trollduction.
        """
        try:
            while self._loop:
                # wait for new messages
                try:
                    msg = self.listener.queue.get(True, 5)
                except KeyboardInterrupt:
                    self.stop()
                    raise
                except Queue.Empty:
                    continue
                LOGGER.debug(str(msg))
                if isinstance(msg.data['sensor'], (list, tuple, set)):
                    sensors = set(msg.data['sensor'])
                else:
                    sensors = set((msg.data['sensor'], ))

                prev_pass = self._previous_pass
                if (msg.type in ["file", 'collection', 'dataset'] and
                    sensors.intersection(
                        self.td_config['instruments'].split(','))):
                    try:
                        if self.td_config.get('process_only_once',
                                              "false").lower() in \
                           ["true", "yes", "1"] and \
                           self._previous_pass["platform_name"] == \
                           msg.data["platform_name"] and \
                           self._previous_pass["start_time"] == \
                           msg.data["start_time"]:
                            LOGGER.info(
                                "File was already processed. Skipping.")
                            continue
                        else:
                            self._previous_pass["platform_name"] = \
                                msg.data["platform_name"]
                            self._previous_pass["start_time"] = \
                                msg.data["start_time"]
                    except TypeError:
                        self._previous_pass["platform_name"] = \
                            msg.data["platform_name"]
                        self._previous_pass["start_time"] = \
                            msg.data["start_time"]

                    except KeyError:
                        LOGGER.info("Can't check if file is already processed, "
                                    "so let's do it anyway.")

                    self.update_product_config(
                        self.td_config['product_config_file'])

                    retried = False
                    while True:
                        try:
                            self.data_processor.run(self.product_config, msg)
                            break
                        except IOError:
                            if retried:
                                LOGGER.debug("History of processed files not "
                                             "updated due to "
                                             "missing/corrupted/incomplete "
                                             "data.")
                                self._previous_pass = prev_pass
                                break
                            else:
                                retried = True
                                LOGGER.info("Retrying once in 2 seconds.")
                                time.sleep(2)
        finally:
            self.shutdown()
