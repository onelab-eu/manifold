#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Register every available Gateway.
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC 

import traceback
from manifold.util.log                  import Log

#-------------------------------------------------------------------------------
# List of gateways
#-------------------------------------------------------------------------------

#import os, glob
#from manifold.util.misc import find_local_modules

# XXX Remove __init__
# XXX Missing recursion for sfa
#__all__ = find_local_modules(__file__)
#[ os.path.basename(f)[:-3] for f in glob.glob(os.path.dirname(__file__)+"/*.py")]

def register():
    try:
        from manifold.gateways.postgresql       import PostgreSQLGateway
    except:
        Log.warning(traceback.format_exc())
        pass
    try:
        from manifold.gateways.tdmi             import TDMIGateway
    except:
        Log.warning(traceback.format_exc())
        pass
    try:
        from manifold.gateways.sfa              import SFAGateway
    except:
        Log.warning(traceback.format_exc())
        pass
    try:
        from manifold.gateways.maxmind          import MaxMindGateway
    except:
        Log.warning(traceback.format_exc())
        pass
    try:
        from manifold.gateways.csv              import CSVGateway
    except:
        Log.warning(traceback.format_exc())
        pass
    try:
        from manifold.gateways.manifold_xmlrpc  import ManifoldGateway
    except:
        Log.warning(traceback.format_exc())
        pass
    try:
        from manifold.gateways.sqlalchemy       import SQLAlchemyGateway
    except:
        Log.warning(traceback.format_exc())
        pass
    try:
        from manifold.gateways.oml              import OMLGateway
    except:
        Log.warning(traceback.format_exc())
        pass
    try:
        from manifold.gateways.perfsonar        import PerfSONARGateway
    except:
        Log.warning(traceback.format_exc())
        pass

register()

__all__ = ['Gateway']
