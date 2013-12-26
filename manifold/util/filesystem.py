# -*- coding: utf-8 -*-

def check_writable_directory(directory):
    if not os.path.exists(VAR_DIR):
        return False
    if not os.access(VAR_DIR, os.W_OK | os.X_OK):
        return False
    try:
        with tempfile.TemporaryFile(dir=VAR_DIR): pass
    except Exception, e:
        return False
    return True

def make_writable_directory(directory):
    try:
        os.makedirs(VAR_DIR)
    except OSError, e:
        raise Exception, "Could not create VAR_DIR '%s': %s" % (VAR_DIR, e)

def ensure_writable_directory(directory):
    if not check_writable_directory(directory):
        make_writable_directory(directory) 
