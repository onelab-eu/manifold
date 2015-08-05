#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manifold git gateway
#
# Authors:
# Jordan Auge       <jordan.auge@free.fr>
#
# Copyright (C) UPMC 

import socket
from types                              import StringTypes
from manifold.core.announce             import Announces, announces_from_docstring
from manifold.core.field_names          import FieldNames
from manifold.core.filter               import Filter
from manifold.gateways                  import Gateway
from manifold.gateways.object           import ManifoldCollection
from manifold.util.reactor_thread       import ReactorThread
from manifold.util.type                 import accepts, returns 
from manifold.util.predicate            import eq, included

from subprocess import Popen, check_call, PIPE

GIT_COMMIT_FIELDS = ['id', 'author_name', 'author_email', 'date', 'message']
GIT_LOG_FORMAT = ['%H', '%an', '%ae', '%ad', '%s']

# Join the format fields together with "\x1f" (ASCII field separator) and
# delimit the records by "\x1e" (ASCII record separator). These characters are
# not likely to appear in your commit data, so they are pretty safe to use for
# parsing.
GIT_LOG_FORMAT = '%x1f'.join(GIT_LOG_FORMAT) + '%x1e'

def single_key_value(func):
    """
    Decorator for get functions that only support one key value
    """
    def wrap(self, packet):
        destination = packet.get_destination()
        filter = destination.get_filter()

        # Currently, we only support eq and included operators
        value_list = list()
        found = False
        for predicate in filter:
            key, op, value = predicate.get_tuple()
            if key != 'url':
                continue
            if found:
                raise Exception, "Only a single predicate with key is supported."
            found = True
            if op == eq:
                value_list.append(value)
            elif op == included:
                value_list.extend(value)
            else:
                raise Exception, "Only eq and included predicates are supported for keys."

        if not value_list:
            raise Exception, "Git Platform is on join"

        for value in value_list:
            record = func(self, value)
            self.get_gateway().record(record, packet)
        self.get_gateway().last(packet)
    return wrap


class GitLogCollection(ManifoldCollection):
    """
    class git {
        const string url;
        const string log;

        CAPABILITY(join);
        KEY(url);
    };
    """

    @single_key_value
    def get(self, url):
        p = Popen('git -C %s log -2 --format="%s"' % (url, GIT_LOG_FORMAT), shell=True, stdout=PIPE)
        (log, _) = p.communicate()
        log = log.strip('\n\x1e').split("\x1e")
        log = [row.strip().split("\x1f") for row in log]
        log = [dict(zip(GIT_COMMIT_FIELDS, row)) for row in log]
        print "log", log
        return {'url': url, 'log': log}

class GitStateCollection(ManifoldCollection):
    """
    class git {
        const string url;
        const string state;
        const bool has_local_changes;

        CAPABILITY(join);
        KEY(url);

    };
    """

    @single_key_value
    def get(self, url):
        p = Popen('git -C %s rev-parse @' % (url,), shell=True, stdout=PIPE)
        (local, _) = p.communicate()
        p = Popen('git -C %s rev-parse @{u}' % (url,), shell=True, stdout=PIPE)
        (remote, _) = p.communicate()
        p = Popen('git -C %s rev-parse @ @{u}' % (url,), shell=True, stdout=PIPE)
        (base, _) = p.communicate()

        if local == remote:
            state = 'UP TO DATE'
        elif local == base:
            state = 'NEED TO PULL'
        elif remote == base:
            state = 'NEED TO PUSH'
        else:
            state = 'DIVERGED'

        try:
            check_call(['git', '-C', url, 'diff-index', '--quiet', 'HEAD', '--'], shell=True, stdout=PIPE)
            has_local_changes = False
        except:
            has_local_changes = True

        print "state=", state
    
        return {'url': url, 'state': state, 'has_local_changes': has_local_changes}

class GitGateway(Gateway):

    __gateway_name__ = "git"

    def __init__(self, router = None, platform_name = None, **platform_config):
        Gateway.__init__(self, router, platform_name, **platform_config)

        self.register_collection(GitLogCollection())
        self.register_collection(GitStateCollection())
