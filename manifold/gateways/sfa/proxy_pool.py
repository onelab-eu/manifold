#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A SFAProxyPool manages a set of SFAProxy used to contact
# a SFA server. Those connections may be permanent or temporary.
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
# Jordan Auge       <jordan.auge@lip6.fr>
#
# Copyright (C) 2013 UPMC

from types                              import StringTypes
from time                               import time

from manifold.gateways.sfa.proxy        import SFAProxy, make_sfa_proxy, DEFAULT_TIMEOUT
from manifold.util.log                  import Log 
from manifold.util.type                 import accepts, returns 

# TTL of a SFAProxy in the SFAProxyPool
TIME_TO_LIVE = 3600

class SFAProxyPool(object):
    def __init__(self):
        """
        Constructor
        """
        # { hash(interface, user) :
        #   {
        #       "proxy" : SFAProxy,
        #       "ttl"   : timestamp
        #   }
        # }
        self.proxies = dict()

    def clean(self):
        """
        Remove from this SFAProxyPool every outdated SFAProxy instances.
        """
        cur_ts = time()
        for key, data in self.proxies.items():
            max_ts = data["ttl"]
            if cur_ts > max_ts:
                del self.proxies[key]

    @returns(SFAProxy)
    def get(self, interface_url, user_email, account_config, cert_type, timeout = DEFAULT_TIMEOUT, store_in_cache = True):
        """
        Create (if required) a SFAProxy toward a given SFA interface (RM or AM).
        Args:
            interface_url: A String containing the URL of the SFA interface.
            user_email: 
            account_config: A dictionnary describing the User's Account.
            cert_type: A String among "gid" and "sscert".
            timeout: The timeout (in seconds).
            store_in_cache: A boolean set to True if this SFAProxy must be
                stored in the SFAProxyPool or only returned by this function.
        Returns:
            The corresponding SFAProxy.
        """ 
        assert isinstance(interface_url, StringTypes), \
            "SFAProxyPool::get(): Invalid interface_url: %s (%s)" % (interface_url, type(interface_url))
        assert isinstance(user_email, StringTypes),\
            "Invalid user_email = %s (%s)" % (user_email, type(user_email))
        assert isinstance(account_config, dict), \
            "Invalid account_config = %s (%s)" % (account_config, type(account_config))
        assert isinstance(cert_type, StringTypes) and cert_type in ["sscert", "gid"], \
            "Invalid cert_type = %s (%s)" % (cert_type, type(cert_type))
        assert isinstance(timeout, int), \
            "Invalid timeout = %s (%s)" % (timeout, type(timeout))
        assert isinstance(store_in_cache, bool), \
            "Invalid store_in_cache = %s (%s)" % (store_in_cache, type(store_in_cache))

        key = hash((interface_url, user_email))

        # Clean the pool to avoid to return an outdated proxy
        self.clean()

        # Search the correspoding SFAProxy in the cache (if any).
        try:
            return self.proxies[key]["proxy"]
        except KeyError:
            pass

        Log.info("Adding new proxy %s@%s" % (user_email, interface_url))
        #Log.tmp("interface_url  = %s" % interface_url)
        #Log.tmp("user           = %s" % user)
        #Log.tmp("account_config = %s" % account_config)
        #Log.tmp("cert_type      = %s" % cert_type)
        #Log.tmp("timeout        = %s" % timeout)
        proxy = make_sfa_proxy(interface_url, account_config, cert_type, timeout)

        if store_in_cache:
            ttl = time() + TIME_TO_LIVE
            self.proxies[key] = {
                "proxy" : proxy,
                "ttl"   : ttl 
            }

        return proxy
