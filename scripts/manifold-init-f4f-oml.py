#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Functions useful to write manifold scripts.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Loic Baron        <loic.baron@lip6.fr> 

import json

from manifold.core.local    import LOCAL_NAMESPACE
from manifold.bin.common    import check_num_arguments, run_command
from manifold.core.code     import ERROR, SUCCESS

#--------------------------------------------------------------
# PostgreSQL configuration
#--------------------------------------------------------------

DEFAULT_PGSQL_HOST     = "193.175.132.241"
DEFAULT_PGSQL_USER     = "reputation"
DEFAULT_PGSQL_PASSWORD = "fed4fire"
DEFAULT_PGSQL_PORT     = 5432

platform_configs = {
    # NOTE: Here you can define one dict per platform, you may override default parameters.
    # Example:
    #
    # "platform_name" : {
    #   "db_host"     : "my.host.com"
    #   "db_user"     : "user"
    #   "db_password" : "password"
    #   "db_name"     : "mydatabase"
    # }
    
    "fuseco"  : {"db_name" : "FUSECO"},
    "bonfire" : {"db_name" : "BonFIRE"},
    "netmode" : {"db_name" : "NETMODE"}
}

#--------------------------------------------------------------
# Platform configuration 
#--------------------------------------------------------------

DEFAULT_GATEWAY   = "oml"
DEFAULT_AUTH_TYPE = "none" # see manifold/bin/addplatform.py
DEFAULT_STATUS    = 0      # 0 means disabled, 1 means enabled

#--------------------------------------------------------------
# Script
#--------------------------------------------------------------

DOC_INIT_F4F_OML = """
%(default_message)s

usage: %(program_name)s

Populate the Manifold Storage to enable F4F OML repositories.
"""

CMD_ADD_PLATFORM = """
INSERT INTO %(namespace)s:platform
    SET
        platform          = '%(platform_name)s',
        platform_longname = '%(platform_longname)s',
        gateway_type      = '%(gateway_type)s',
        auth_type         = '%(auth_type)s',
        config            = '%(platform_config)s',
        disabled          = %(disabled)s
"""

def main():
    ret = SUCCESS
    for platform_name, platform_config in platform_configs.items():
        keys = platform_config.keys()
        if "db_host"     not in keys: platform_config["db_host"]     = DEFAULT_PGSQL_HOST
        if "db_user"     not in keys: platform_config["db_user"]     = DEFAULT_PGSQL_USER
        if "db_password" not in keys: platform_config["db_password"] = DEFAULT_PGSQL_PASSWORD
        if "db_port"     not in keys: platform_config["db_port"]     = DEFAULT_PGSQL_PORT
        assert "db_name" in keys

        # This is basically manifold-add-platform
        platform_longname = platform_name
        platform_config   = json.dumps(platform_config)
        gateway_type      = DEFAULT_GATEWAY
        auth_type         = DEFAULT_AUTH_TYPE
        disabled          = DEFAULT_STATUS
        namespace         = LOCAL_NAMESPACE
        if run_command(CMD_ADD_PLATFORM % locals()) != SUCCESS:
            ret = ERROR 
    return ret

if __name__ == "__main__":
    check_num_arguments(DOC_INIT_F4F_OML, 1, 1)
    main()

