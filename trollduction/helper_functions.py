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

'''Helper functions for Trollduction
'''

import numpy as np
import os
import xml_read
from mpop.projector import get_area_def
from pyresample.geometry import Boundary
import logging
from ConfigParser import ConfigParser

LOGGER = logging.getLogger(__name__)


def read_config_file(fname, config_item=None):
    '''Read config file to dictionary.
    '''

    endswith = os.path.splitext(fname)[1]
    if endswith == ".xml":
        return xml_read.parse_xml(xml_read.get_root(fname))
    elif endswith in ['.ini', '.cfg']:
        config = ConfigParser()
        config.read(fname)
        conf_dict = dict(config.items(config_item))
        conf_dict["config_file"] = fname
        conf_dict["config_item"] = config_item
        return conf_dict
    else:
        raise NotImplementedError("Can only parse xml and .ini config files"
                                  "for now")


def get_maximum_extent(area_def_names):
    '''Get maximum extend needed to produce all defined areas.
    '''
    maximum_area_extent = [None, None, None, None]
    for area in area_def_names:
        extent = get_area_def(area)

        if maximum_area_extent[0] is None:
            maximum_area_extent = list(extent.area_extent)
        else:
            if maximum_area_extent[0] > extent.area_extent[0]:
                maximum_area_extent[0] = extent.area_extent[0]
            if maximum_area_extent[1] > extent.area_extent[1]:
                maximum_area_extent[1] = extent.area_extent[1]
            if maximum_area_extent[2] < extent.area_extent[2]:
                maximum_area_extent[2] = extent.area_extent[2]
            if maximum_area_extent[3] < extent.area_extent[3]:
                maximum_area_extent[3] = extent.area_extent[3]

    return maximum_area_extent


def get_maximum_extent_ll(area_def_names):
    '''Get maximum extend needed to produce all the defined areas
    given in *area_def_names*.
    '''
    maximum_area_extent = [None, None, None, None]

    for area in area_def_names:
        area_def = get_area_def(area['definition'])

        lons, lats = get_area_boundaries(area_def)
        left_lon, down_lat, right_lon, up_lat = \
            np.min(lons.side4), \
            np.min(lats.side3), \
            np.max(lons.side2), \
            np.max(lats.side1)

        if maximum_area_extent[0] is None:
            maximum_area_extent = [left_lon, down_lat, right_lon, up_lat]
        else:
            if maximum_area_extent[0] > left_lon:
                maximum_area_extent[0] = left_lon
            if maximum_area_extent[1] > down_lat:
                maximum_area_extent[1] = down_lat
            if maximum_area_extent[2] < right_lon:
                maximum_area_extent[2] = right_lon
            if maximum_area_extent[3] < up_lat:
                maximum_area_extent[3] = up_lat

    return maximum_area_extent


def get_maximum_ll_borders(area_def_names):
    '''Get maximum extend needed to produce all the defined areas
    given in *area_def_names*.
    '''
    maximum_area_extent = [None, None, None, None]

    for area in area_def_names:
        area_def = get_area_def(area['definition'])

        lons, lats = get_area_boundaries(area_def)
        left_lon, down_lat, right_lon, up_lat = \
            np.min(lons.side4), \
            np.min(lats.side3), \
            np.max(lons.side2), \
            np.max(lats.side1)

        if maximum_area_extent[0] is None:
            maximum_area_extent = [left_lon, down_lat, right_lon, up_lat]
        else:
            if maximum_area_extent[0] > left_lon:
                maximum_area_extent[0] = left_lon
            if maximum_area_extent[1] > down_lat:
                maximum_area_extent[1] = down_lat
            if maximum_area_extent[2] < right_lon:
                maximum_area_extent[2] = right_lon
            if maximum_area_extent[3] < up_lat:
                maximum_area_extent[3] = up_lat

    return maximum_area_extent


def get_area_boundaries(area_def):
    '''Get area boundaries from area definition.
    '''

    # upper boundary
    lonlat = np.array([area_def.get_lonlat(0, i)
                       for i in range(area_def.x_size)])
    up_lons = lonlat[:, 0]
    up_lats = lonlat[:, 1]

    # lower boundary
    lonlat = np.array([area_def.get_lonlat(area_def.y_size - 1, i)
                       for i in range(area_def.x_size)])
    down_lons = lonlat[:, 0]
    down_lats = lonlat[:, 1]

    # left boundary
    lonlat = np.array([area_def.get_lonlat(i, 0)
                       for i in range(area_def.y_size)])
    left_lons = lonlat[:, 0]
    left_lats = lonlat[:, 1]

    # right boundary
    lonlat = np.array([area_def.get_lonlat(i, area_def.x_size - 1)
                       for i in range(area_def.y_size)])
    right_lons = lonlat[:, 0]
    right_lats = lonlat[:, 1]

    return (Boundary(up_lons, right_lons, down_lons, left_lons),
            Boundary(up_lats, right_lats, down_lats, left_lats))


def get_indices_from_boundaries(boundary_lons, boundary_lats,
                                lons, lats, radius_of_influence):
    """Find relevant indices from grid boundaries using the 
    winding number theorem"""

    valid_index = _get_valid_index(boundary_lons.side1, boundary_lons.side2,
                                   boundary_lons.side3, boundary_lons.side4,
                                   boundary_lats.side1, boundary_lats.side2,
                                   boundary_lats.side3, boundary_lats.side4,
                                   lons, lats, radius_of_influence)

    return valid_index


def get_angle_sum(lons_side1, lons_side2, lons_side3, lons_side4):
    '''Calculate angle sum for winding number theorem.  Note that all
    the sides need to be connected, that is:

    lons_side[-1] == lons_side2[0], 
    ...
    lons_side4[-1] == lons_side1[0]
    '''
    angle_sum = 0
    for side in (lons_side1, lons_side2, lons_side3, lons_side4):
        side_diff = np.sum(np.diff(side))
        idxs, = np.where(np.abs(side_diff) > 180)
        if idxs:
            side_diff[idxs] = (np.abs(side_diff[idxs]) - 360) * \
                np.sign(side_diff[idxs])
        angle_sum += np.sum(side_diff)

    return angle_sum


def _get_valid_index(lons_side1, lons_side2, lons_side3, lons_side4,
                     lats_side1, lats_side2, lats_side3, lats_side4,
                     lons, lats, radius_of_influence):
    """Find relevant indices from grid boundaries using the 
    winding number theorem"""

    earth_radius = 6370997.0

    # Coarse reduction of data based on extrema analysis of the boundary
    # lon lat values of the target grid
    illegal_lons = (((lons_side1 < -180) | (lons_side1 > 180)).any() or
                    ((lons_side2 < -180) | (lons_side2 > 180)).any() or
                    ((lons_side3 < -180) | (lons_side3 > 180)).any() or
                    ((lons_side4 < -180) | (lons_side4 > 180)).any())

    illegal_lats = (((lats_side1 < -90) | (lats_side1 > 90)).any() or
                    ((lats_side2 < -90) | (lats_side2 > 90)).any() or
                    ((lats_side3 < -90) | (lats_side3 > 90)).any() or
                    ((lats_side4 < -90) | (lats_side4 > 90)).any())

    if illegal_lons or illegal_lats:
        # Grid boundaries are not safe to operate on
        return np.ones(lons.size, dtype=np.bool)

    # Find sum angle sum of grid boundary
    angle_sum = get_angle_sum(lons_side1, lons_side2,
                              lons_side3[::-1], lons_side4[::-1])

    # Buffer min and max lon and lat of interest with radius of interest
    lat_min = min(lats_side1.min(), lats_side2.min(),
                  lats_side3.min(), lats_side4.min())
    lat_min_buffered = lat_min - np.degrees(float(radius_of_influence) /
                                            earth_radius)
    lat_max = max(lats_side1.max(), lats_side2.max(), lats_side3.max(),
                  lats_side4.max())
    lat_max_buffered = lat_max + np.degrees(float(radius_of_influence) /
                                            earth_radius)

    max_angle_s2 = max(abs(lats_side2.max()), abs(lats_side2.min()))
    max_angle_s4 = max(abs(lats_side4.max()), abs(lats_side4.min()))
    lon_min_buffered = lons_side4.min() - \
        np.degrees(float(radius_of_influence) /
                   (np.sin(np.radians(max_angle_s4)) * earth_radius))

    lon_max_buffered = lons_side2.max() + \
        np.degrees(float(radius_of_influence) /
                   (np.sin(np.radians(max_angle_s2)) * earth_radius))

    # From the winding number theorem follows:
    # angle_sum possiblilities:
    # -360: area covers north pole
    #  360: area covers south pole
    #    0: area covers no poles
    # else: area covers both poles
    if round(angle_sum) == -360:
        LOGGER.debug("Area covers north pole")
        # Covers NP
        valid_index = (lats >= lat_min_buffered)
    elif round(angle_sum) == 360:
        LOGGER.debug("Area covers south pole")
        # Covers SP
        valid_index = (lats <= lat_max_buffered)
    elif round(angle_sum) == 0:
        LOGGER.debug("Area covers no poles")
        # Covers no poles
        valid_lats = (lats >= lat_min_buffered) * (lats <= lat_max_buffered)

        if lons_side2.min() > lons_side4.max():
            # No date line crossing
            valid_lons = (lons >= lon_min_buffered) * \
                (lons <= lon_max_buffered)
        else:
            # Date line crossing
            seg1 = (lons >= lon_min_buffered) * (lons <= 180)
            seg2 = (lons <= lon_max_buffered) * (lons >= -180)
            valid_lons = seg1 + seg2

        valid_index = valid_lats * valid_lons
    else:
        LOGGER.debug("Area covers both poles")
        # Covers both poles, don't reduce
        return True
        # valid_index = np.ones(lons.size, dtype=np.bool)

    return valid_index


def overlapping_timeinterval(start_end_times, timelist):
    """From a list of start and end times check if the current time interval
    overlaps with one or more"""

    starttime, endtime = start_end_times
    for tstart, tend in timelist:
        if ((tstart <= starttime and tend > starttime) or
                (tstart < endtime and tend >= endtime)):
            return tstart, tend
        elif (tstart >= starttime and tend <= endtime):
            return tstart, tend

    return False


def parse_aliases(config):
    '''Parse aliases from the config.

    Aliases are given in the config as:

    {'alias_<name>': 'value:alias'}, or
    {'alias_<name>': 'value1:alias1|value2:alias2'},

    where <name> is the name of the key which value will be
    replaced. The later form is there to support several possible
    substitutions (eg. '2' -> '9' and '3' -> '10' in the case of MSG).

    '''
    aliases = {}

    for key in config:
        if 'alias' in key:
            alias = config[key]
            new_key = key.replace('alias_', '')
            if '|' in alias or ':' in alias:
                parts = alias.split('|')
                aliases2 = {}
                for part in parts:
                    key2, val2 = part.split(':')
                    aliases2[key2] = val2
                alias = aliases2
            aliases[new_key] = alias
    return aliases


def parse_vars(config):
    '''Parse custom variables from the config.

    Aliases are given in the config as:

    {'var_<name>': 'value'}

    where <name> is the name of the key which value will be
    added to metadata. <value> is a trollsift pattern.

    '''
    vars = {}

    for key in config:
        if 'var_' in key:
            new_key = key.replace('var_', '')
            var = config[key]
            vars[new_key] = var
    return vars


def eval_default(expression, default_res=None):
    """Calls eval on expression and returns default_res if it throws
    exceptions
    """
    if default_res is None:
        default_res = expression
    try:
        return eval(expression)
    except:
        pass
    return default_res
