#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pyparsing as pp
from .             import ProcessGateway, Argument, Parameter, FLAG_IN_ANNOTATION, FLAG_OUT_ANNOTATION, FLAG_ADD_FIELD
from ...util.log   import Log

class PingParser(object):
    """
    Parses the `ping_output` string into a dictionary containing the following
    fields:
        TODO
    """

    def __init__(self, params):
        pass # look into params to know which files to output
        # or maybe just look into fastoutput output

    def parse(self, string):
        return [{
            'file1': '',
            'file2': '',  # Eventually not this one depending on parameters
            'file3': '',
            'file4': ''
        }]

class PingGateway(ProcessGateway):
    __gateway_name__ = 'fastping_process'
    __tool__ = 'fastping'

    parameters = [
        Parameter(
            name        = 'ftp_url',
            type        = 'string',
            description = 'The URL of the FTP server where to store measurements',
            short       = '-U',
            flags       = FLAG_IN_ANNOTATION,
        ),
        # XXX ftp_login, ftp_password

        Parameter(
            name        = 'file1',
            type        = 'bool',
            description = 'Do we want file 1 ?',
            short       = '-r',
            flags       = FLAG_ADD_FIELD,
        ),
        # XXX file 2 3 4

        Parameter(
            name        = 'hostname', 
            type        = 'string', 
            description = '',
            short       = '-n',
            action      = '', 
            flags       = FLAG_ADD_FIELD
        ),
    ]

    # XXX
    arguments = [
        Argument(
            name        = 'destination',
            type        = 'ip'
        ),
    ]

    announces = """
    class probe_ping {
        float delay;
        CAPABILITY(join);
        LOCAL KEY();
    };

    class fastping {
        inet source;
        inet destination;
        string file1;
        string file2;
        string file3;
        string file4;
        CAPABILITY(join);
        KEY(source, destination);
    };
    """
    parser = PingParser
    path = '/bin/fastping'
