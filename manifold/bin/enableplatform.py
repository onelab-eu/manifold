#!/usr/bin/env python
#! -*- coding: utf-8 -*-
#
# Enable a platform in the Manifold Storage.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import sys

from manifold.bin.common    import check_num_arguments, run_command
from manifold.util.storage  import STORAGE_NAMESPACE 

DOC_ENABLE_PLATFORM = """
%(default_message)s

usage: %(program_name)s PLATFORM_NAME
    Enable a platform
"""

CMD_ENABLE_PLATFORM = """
UPDATE    %(namespace)s:platform
    SET   disabled = False
    WHERE platform == "%(platform_name)s"
"""

def main():
    check_num_arguments(DOC_ENABLE_PLATFORM, 2, 2)
    return run_command(CMD_ENABLE_PLATFORM, {
        "platform_name" : sys.argv[1],
        "namespace"     : STORAGE_NAMESPACE
    })

if __name__ == '__main__':
    main()
