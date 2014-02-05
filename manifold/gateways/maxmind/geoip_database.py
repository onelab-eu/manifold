#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# GeoIP databases 
#   http://dev.maxmind.com/geoip/legacy/geolite/
#
# Note: This is not GeoIP2.
#   http://dev.maxmind.com/geoip/geoip2/whats-new-in-geoip2/
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC 

import os
from types                              import StringTypes

from manifold.util.filesystem           import check_readable_file, gunzip, mkdir, wget
from manifold.util.log                  import Log
from manifold.util.type                 import returns, accepts 

# Note that Debian package only provides:
#  /usr/share/GeoIP/GeoIP.dat
#  /usr/share/GeoIP/GeoIPv6.dat

MAXMIND_DIR              = "/usr/local/share/geolite/"
MAXMIND_DAT_URL          = "http://geolite.maxmind.com/download/geoip/database"

MAXMIND_DAT_IPV4_ASN     = "GeoIPASNum.dat"     # GeoLite ASN
MAXMIND_DAT_IPV6_ASN     = "GeoIPASNumv6.dat"   # GeoLite ASN IPv6
MAXMIND_DAT_IPV4_COUNTRY = "GeoIP.dat"          # GeoLite Country
MAXMIND_DAT_IPV6_COUNTRY = "GeoIPv6.dat"        # GeoLite Country IPv6 (beta)
MAXMIND_DAT_IPV4_CITY    = "GeoLiteCity.dat"    # GeoLite City
MAXMIND_DAT_IPV6_CITY    = "GeoLiteCityv6.dat"  # GeoLite City IPv6

MAP_MAXMIND_DAT_URL = {
    MAXMIND_DAT_IPV4_ASN     : "%s/asnum/%s.gz"              % (MAXMIND_DAT_URL, MAXMIND_DAT_IPV4_ASN),
    MAXMIND_DAT_IPV6_ASN     : "%s/asnum/%s.gz"              % (MAXMIND_DAT_URL, MAXMIND_DAT_IPV6_ASN),
    MAXMIND_DAT_IPV4_COUNTRY : "%s/GeoLiteCountry/%s.gz"     % (MAXMIND_DAT_URL, MAXMIND_DAT_IPV4_COUNTRY),
    MAXMIND_DAT_IPV6_COUNTRY : "%s/%s.gz"                    % (MAXMIND_DAT_URL, MAXMIND_DAT_IPV6_COUNTRY),
    MAXMIND_DAT_IPV4_CITY    : "%s/%s.gz"                    % (MAXMIND_DAT_URL, MAXMIND_DAT_IPV4_CITY),
    MAXMIND_DAT_IPV6_CITY    : "%s/GeoLiteCityv6-beta/%s.gz" % (MAXMIND_DAT_URL, MAXMIND_DAT_IPV6_CITY),
}

@accepts(StringTypes)
def check_filename_dat(filename_dat):
    """
    Test whether a dat file is available.
    Args:
        filename_dat: A String containing the absolute path of a dat file.
    Raises:
        ValueError: If filename_dat refers to an dat filename not supported.
    """
    assert isinstance(filename_dat, StringTypes),\
        "Invalid filename_dat = %s (%s) " % (filename_dat, type(filename_dat))

    basename = os.path.basename(filename_dat)
    allowed_basenames = get_dat_basenames()
    if not basename in allowed_basenames:
        raise ValueError("%s is not supported. Supported values are {%s}" % ", ".join(allowed_basenames))

@returns(list)
def get_dat_basenames():
    """
    Returns:
        A list of String where each String corresponds to a
        supported MaxMind dat files.
    """
    return MAP_MAXMIND_DAT_URL.keys()

@returns(bool)
@accepts(StringTypes)
def is_dat_available(filename_dat):
    """
    Test whether a dat file is available.
    Args:
        filename_dat: A String containing the absolute path of a dat file.
    Raises:
        ValueError: If filename_dat refers to an dat filename not supported.
    Returns:
        True iif this dat file is available, False otherwise.
    """
    ret = True
    check_filename_dat(filename_dat)
    try:
        check_readable_file(filename_dat)
    except:
        ret = False
    return ret

@accepts(StringTypes, bool)
def install_dat(basename_dat, overwrite):
    """
    Install a MaxMind dat file on the local filesystem.
    Args:
        basename_dat: A String contained in MaxMind.get_dat_basenames() 
        overwrite: Pass False if the eventual existing file can be kept,
            True otherwise.
    Raises:
        RuntimeError: in case of failure
        TimeoutException: in case of timeout
    """
    check_filename_dat(basename_dat)

    # wget
    url = MAP_MAXMIND_DAT_URL[basename_dat]
    basename_dl = os.path.split(url)[-1]
    filename_dl = os.path.join(MAXMIND_DIR, basename_dl)
    wget(url, filename_dl, overwrite)

    # gunzip
    if basename_dl.endswith(".gz"):
        filename_gz  = filename_dl
        filename_dat = filename_gz[:-3]
        gunzip(filename_gz, filename_dat, False)

