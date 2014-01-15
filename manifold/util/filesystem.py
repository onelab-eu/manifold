#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Set of functions useful to manage the local filesystem.
# 
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import os, tempfile
from types              import StringTypes

from ..util.certificate import Keypair, Certificate
from manifold.util.type import accepts, returns

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
def mkdir(directory):
    """
    Create a directory (mkdir -p).
    Raises:
        RuntimeError: If the directory cannot be created.
    Args:
        directory: A String containing an absolute path.
    """
    # http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
    try:
        os.makedirs(directory)
    except OSError as e: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(directory):
            pass
        else:
            raise RuntimeError("Cannot mkdir %s: %s" % (directory, e))

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

@returns(Keypair)
@accepts(StringTypes)
def check_keypair(filename):
    """
    Tests whether a filename contains a valid Keypair.
    Args:
        filename: A String containing the absolute path of this file.
    Raises:
        RuntimeError: If the file does not exists or cannot be loaded.
    """
    if not os.path.exists(filename):
        raise RuntimeError("Private key file does not exists '%s': %s" % (filename, e))
    try:
        keypair = Keypair(filename=filename)
    except Exception, e:
        raise RuntimeError("Cannot load private key '%s': %s" % (FN_PRIVATE_KEY, e))

    return keypair

@returns(Keypair)
@accepts(StringTypes)
def make_keypair(filename):
    """
    Create a Keypair and stores it into a file.
    Args:
        filename: The absolute path of the output file. 
    Raises:
        RuntimeError: If the file does not exists or cannot be loaded.
    """
    try:
        keypair = Keypair(create = True)
        keypair.save_to_file(filename)
    except Exception, e:
        raise RuntimeError("Cannot generate private key '%s' : %s" % (filename, e))
    return keypair

@returns(Keypair)
@accepts(StringTypes)
def ensure_keypair(filename):
    """
    Test whether a file contains a valid Keypair, and if not, try to create it
    in the specified file.
    Args:
        filename: The absolute path of the Keypair file. 
    Raises:
        RuntimeError: If the file does not exists or cannot be loaded.
    """
    try:
        keypair = check_keypair(filename)
    except:
        keypair = make_keypair(filename)
    return keypair

#-------------------------------------------------------------------------------
# Certificate management
#-------------------------------------------------------------------------------

# openssl req -new -x509 -nodes -sha1 -days 365 -key /etc/manifold/keys/server.key > /etc/manifold/keys/server.cert

@returns(Certificate)
@accepts(StringTypes)
def check_certificate(filename):
    """
    Tests whether a filename contains a valid Certificate.
    Args:
        filename: A String containing the absolute path of this file.
    Raises:
        RuntimeError: If the file does not exists or cannot be loaded.
    """
    if not os.path.exists(filename):
        raise Exception, "Certificate file does not exists '%s': %s" % (filename, e)
    try:
        certificate = Certificate(filename=filename)
    except:
        raise Exception, "Cannot load certificate '%s' : %s" % (filename, e)
 
    return certificate

@returns(Certificate)
@accepts(StringTypes, StringTypes, Keypair)
def make_certificate(filename, subject, keypair):
    """
    Create a Certificate using the public key stored in a Keypair
    and stores it into a file.
    Args:
        filename: The absolute path of the output file. 
        subject: A String encoded in latin1.
        keypair: A Keypair instance. 
    Raises:
        RuntimeError: If the file does not exists or cannot be loaded.
    """
    try:
        #subject = subject.encode("latin1"))
        certificate = Certificate(subject = subject)
        certificate.set_pubkey(keypair)
        certificate.set_issuer(keypair, subject = subject) # XXX subject specified twice ?
        certificate.sign()
        certificate.save_to_file(filename)
    except Exception, e:
        raise Exception, "Cannot generate certificate '%s' : %s" % (filename, e)

    return certificate

@returns(Certificate)
@accepts(StringTypes, StringTypes, Keypair)
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
