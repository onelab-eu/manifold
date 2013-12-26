# -*- coding: utf-8 -*-

import os, tempfile
from ..util.certificate import Keypair, Certificate

#-------------------------------------------------------------------------------
# File and directory management
#-------------------------------------------------------------------------------

def check_writable_directory(directory):
    if not os.path.exists(directory):
        raise Exception, "Directory '%s' does not exists" % directory
    if not os.access(directory, os.W_OK | os.X_OK):
        raise Exception, "Directory '%s' is not writable" % directory
    try:
        with tempfile.TemporaryFile(dir=directory): pass
    except Exception, e:
        raise Exception, "Cannot write into directory '%s': %s" % (directory, e)


def make_writable_directory(directory):
    try:
        os.makedirs(directory)
    except OSError, e:
        raise Exception, "Could not create directory '%s': %s" % (directory, e)

def ensure_writable_directory(directory):
    try:
        check_writable_directory(directory)
    except Exception, e:
        print 'EXC', e
        make_writable_directory(directory) 

#-------------------------------------------------------------------------------
# Keypair management
#-------------------------------------------------------------------------------

# openssl genrsa 1024 > /etc/manifold/keys/server.key
# chmod 400 /etc/manifold/keys/server.key

def check_keypair(filename):
    if not os.path.exists(filename):
        raise Exception, "Private key file does not exists '%s': %s" % (filename, e)
    try:
        keypair = Keypair(filename=filename)
    except Exception, e:
        raise Exception, "Cannot load private key '%s': %s" % (FN_PRIVATE_KEY, e)

    return keypair

def make_keypair(filename):
    try:
        keypair = Keypair(create = True)
        keypair.save_to_file(filename)
    except Exception, e:
        raise Exception, "Cannot generate private key '%s' : %s" % (filename, e)
    return keypair

def ensure_keypair(filename):
    try:
        keypair = check_keypair(filename)
    except:
        keypair = make_keypair(filename)
    return keypair

#-------------------------------------------------------------------------------
# Certificate management
#-------------------------------------------------------------------------------

# openssl req -new -x509 -nodes -sha1 -days 365 -key /etc/manifold/keys/server.key > /etc/manifold/keys/server.cert

def check_certificate(filename):
    if not os.path.exists(filename):
        raise Exception, "Certificate file does not exists '%s': %s" % (filename, e)
    try:
        certificate = Certificate(filename=filename)
    except:
        raise Exception, "Cannot load certificate '%s' : %s" % (filename, e)
 
    return certificate


def make_certificate(filename, subject, keypair):
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



def ensure_certificate(filename, subject, keypair):
    try:
        certificate = check_certificate(filename)
    except:
        certificate = make_certificate(filename, subject, keypair)
