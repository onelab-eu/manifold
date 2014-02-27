#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Set of functions useful to manage the local filesystem.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from __future__         import with_statement # Required in 2.5

import errno, os, tempfile
from types              import StringTypes

from ..util.certificate import Keypair, Certificate
from ..util.log         import Log
from ..util.timeout     import time_limit, TimeoutException
from ..util.type        import accepts, returns

#-------------------------------------------------------------------------------
# Shell like commands
#-------------------------------------------------------------------------------

@returns(StringTypes)
def hostname():
    """
    Returns:
        The hostname of this machine.
    """
    from subprocess import Popen, PIPE
    return Popen(["uname", "-n"], stdout = PIPE).communicate()[0].strip()

@accepts(StringTypes)
def mkdir(directory):
    """
    Create a directory (mkdir -p).
    Args:
        directory: A String containing an absolute path.
    Raises:
        OSError: If the directory cannot be created.
    """
    # http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
    try:
        if not os.path.exists(directory):
            Log.info("Creating '%s' directory" % directory)
        os.makedirs(directory)
    except OSError as e: # Python >2.5
        if e.errno == errno.EEXIST and os.path.isdir(directory):
            pass
        else:
            raise OSError("Cannot mkdir %s: %s" % (directory, e))

@accepts(StringTypes, StringTypes, bool)
def wget(url, filename_out, overwrite, timeout = 5):
    """
    Download a file using http into a directory with a given path.
    The target directory is created if required.
    Args:
        url: A String containing the URL of the file to download.
        filename_out: A String corresponding to the path of the downloaded file.
        overwrite: Pass True if this function must overwrites filename_out file
            if it already exists, False otherwise.
        timeout: The timeout, expressed in seconds.
    Raises:
        RuntimeError: in case of failure
        TimeoutException: in case of timeout
    """
    # Do not the file if not required
    if not overwrite:
        try:
            check_readable_file(filename_out)
            return
        except:
            pass

    try:
        from urllib import urlretrieve

        # mkdir output directory
        mkdir(os.path.dirname(filename_out))

        # wget dat_filename
        try:
            with time_limit(timeout): # XXX This will break a download too slow!
                Log.info("Downloading '%(url)s' into '%(filename_out)s'" % locals())
                urlretrieve(url, filename_out)
        except TimeoutException, msg:
            raise TimeoutException("Cannot download '%(url)s' (timeout)" % locals())
    except Exception, e:
        raise RuntimeError(e)

@accepts(StringTypes, StringTypes, bool)
def gunzip(filename_gz, filename_out, overwrite):
    """
    Uncompress a .gz file.
    Args:
        filename_gz: Input .gz absolute path.
        filename_out: Output absolute path.
        overwrite: Pass True if this function must overwrites filename_out file
            if it already exists, False otherwise.
    Raises:
        ImportError: if gzip is not installed
        IOError: if the input file is not a gzip file
        RuntimeError: in case of failure
    """
    # Do not the file if not required
    if not overwrite:
        try:
            check_readable_file(filename_out)
            return
        except:
            pass

    import gzip

    Log.info("Gunzip %(filename_gz)s into %(filename_out)s" % locals())
    f_gz = None
    f_out = None
    try:
        f_gz = gzip.open(filename_gz, "rb")
        f_out = open(filename_out, "wb")
        f_out.write(f_gz.read())
    except IOError, e:
        raise IOError("gunzip('%(filename_gz)s' -> '%(filename_out)s'): %(e)s" % locals())
    finally:
        if f_out: f_out.close()
        if f_gz:  f_gz.close()

#-------------------------------------------------------------------------------
# File management
#-------------------------------------------------------------------------------

@accepts(StringTypes)
def check_readable_file(filename):
    """
    Test whether a file can be read.
    Args:
        filename: A String containing the absolute path of this file.
    Raises:
        RuntimeError: If the directory cannot be created.
    """
    if not os.path.isfile(filename):
        raise RuntimeError("%s is not a regular file" % filename)
    try:
        f = open(filename, "r")
        f.close()
    except:
        raise RuntimeError("%s cannot be read" % filename)

#-------------------------------------------------------------------------------
# Directory management
#-------------------------------------------------------------------------------

@accepts(StringTypes)
def check_writable_directory(directory):
    """
    Tests whether a directory is writable.
    Args:
        directory: A String containing an absolute path.
    Raises:
        RuntimeError: If the directory does not exists or isn't writable.
    """
    if not os.path.exists(directory):
        raise RuntimeError("Directory '%s' does not exists" % directory)
    if not os.access(directory, os.W_OK | os.X_OK):
        raise RuntimeError("Directory '%s' is not writable" % directory)
    try:
        with tempfile.TemporaryFile(dir = directory):
            pass
    except Exception, e:
        raise RuntimeError("Cannot write into directory '%s': %s" % (directory, e))

@accepts(StringTypes)
def make_writable_directory(directory):
    """
    See mkdir()
    """
    mkdir(directory)

@accepts(StringTypes)
def ensure_writable_directory(directory):
    """
    Tests whether a directory exists and is writable. If not,
    try to create such a directory.
    Args:
        directory: A String containing an absolute path.
    Raises:
        RuntimeError: If the directory does not exists and cannot be created.
    """
    try:
        check_writable_directory(directory)
    except RuntimeError, e:
        make_writable_directory(directory)

#-------------------------------------------------------------------------------
# Keypair management
#-------------------------------------------------------------------------------

# openssl genrsa 1024 > /etc/manifold/keys/server.key
# chmod 400 /etc/manifold/keys/server.key

@accepts(StringTypes)
def check_keypair(filename):
    """
    Tests whether a filename contains a valid Keypair.
    Args:
        filename: A String containing the absolute path of the private key.
    Raises:
        RuntimeError: If the file does not exists or cannot be loaded.
    """
    if not os.path.exists(filename):
        # See make_keypair
        raise RuntimeError("Private key file does not exists '%s': %s" % (filename, e))
    else:
        try:
            _ = Keypair(filename = filename)
        except Exception, e:
            raise RuntimeError("Cannot load private key '%s': %s" % (FN_PRIVATE_KEY, e))

#@returns(Keypair)
@accepts(StringTypes)
def make_keypair(filename):
    """
    Create a Keypair and stores it into a file.
    Args:
        filename: The absolute path of the output file.
    Raises:
        RuntimeError: If the file does not exists or cannot be loaded.
    """
    keypair = Keypair(create = True)
    keypair.save_to_file(filename)
    return keypair

#@returns(Keypair)
@accepts(StringTypes)
def ensure_keypair(filename):
    """
    Test whether a file contains a valid private key. If not,
    try to create it in the specified target file.
    Args:
        filename: The absolute path of the file containing
            the private key.
    Raises:
        Exception: In case of failure.
    """
    try:
        keypair = Keypair(filename = filename)
        Log.info("Private key found: %s" % filename)
    except:
        Log.info("Private key not found, creating a new one in: %s" % filename)
        keypair = make_keypair(filename)
    return keypair

#-------------------------------------------------------------------------------
# Certificate management
#-------------------------------------------------------------------------------

# openssl req -new -x509 -nodes -sha1 -days 365 -key /etc/manifold/keys/server.key > /etc/manifold/keys/server.cert

#@returns(Certificate)
@accepts(StringTypes)
def check_certificate(filename):
    """
    Tests whether a filename contains a valid Certificate.
    Args:
        filename: A String containing the absolute path of this file.
    Raises:
        IOError: If this file does not exists.
        RuntimeError: If this file cannot be loaded.
    """
    if not os.path.exists(filename):
        raise IOError("Certificate file does not exists '%s': %s" % (filename, e))

    try:
        certificate = Certificate(filename = filename)
    except:
        raise RuntimeError("Cannot load certificate '%s' : %s" % (filename, e))

    return certificate

#@returns(Certificate)
#@accepts(StringTypes, StringTypes, Keypair)
def make_certificate(filename, subject, keypair):
    """
    Create a Certificate using the public key stored in a Keypair
    and stores it into a file.
    Args:
        filename: The absolute path of the output file.
        subject: A String encoded in latin1.
        keypair: A Keypair instance.
    Returns:
        The corresponding Certificate
    """
    assert isinstance(filename, StringTypes),\
        "Invalid filename = %s (%s)" % (filename, type(filename))
    assert isinstance(subject, StringTypes),\
        "Invalid subject = %s (%s)" % (subject, type(subject))
    assert isinstance(keypair, Keypair),\
        "Invalid keypair = %s (%s)" % (keypair, type(keypair))

    #subject = subject.encode("latin1"))
    certificate = Certificate(subject = subject)
    certificate.set_pubkey(keypair)
    certificate.set_issuer(keypair, subject = subject) # XXX subject specified twice ?
    certificate.sign()
    certificate.save_to_file(filename)

    return certificate

@returns(Certificate)
#@accepts(StringTypes, StringTypes, Keypair)
def ensure_certificate(filename, subject, keypair):
    """
    Test whether a file contains a valid Certificate, and if not, try to create it
    in the specified file according to an input Keypair.
    Args:
        filename: The absolute path of the Keypair file.
        subject: A String encoded in latin1.
        keypair: A Keypair instance.
    Raises:
        RuntimeError: If the file does not exists or cannot be loaded.
    """
    try:
        certificate = check_certificate(filename)
    except:
        certificate = make_certificate(filename, subject, keypair)
