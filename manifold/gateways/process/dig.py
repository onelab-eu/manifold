#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .                      import ProcessGateway, Argument, FixedArgument, Parameter, FLAG_IN_ANNOTATION, FLAG_OUT_ANNOTATION, FLAG_ADD_FIELD
from ...util.log            import Log
from ...core.field_names    import FieldNames

class DigParser(object):

    def parse(self, string):
        """
        "ams1a.f.root-servers.org"
        """
        if string and string[0] == '"' and string[-1] == '"':
            name = string[1:-1]
        else:
            name = None
        return [{'instance_name': name}]

class DigGateway(ProcessGateway):
    """
    dig @f.root-servers.net hostname.bind  txt ch +short

    dig hostname.bind @f.root-servers.net chaos txt

    dig id.server @k.root-servers.net chaos txt

    # .org tld servers 
    dig whoareyou.ultradns.net @tld1.ultradns.net
    dig whoareyou.ultradns.net @tld2.ultradns.net

    """
    __gateway_name__ = 'dig_process'
    __tool__ = 'dig'
    __parser_expects_string__ = True

    parameters = []
    arguments = [
        Argument(
            name        = 'ip',
            type        = 'ip',
            prefix      = '@',
        ),
        FixedArgument(value='hostname.bind'),
        FixedArgument(value='txt'),
        FixedArgument(value='ch'),
        FixedArgument(value='+short'),
    ]

    announces = """
    class ip {
        const ip ip;
        const string instance_name;
        CAPABILITY(join);
        KEY(ip);
    };
    """
    parser = DigParser
    path = '/usr/bin/dig'

    def on_receive_query(self, query, annotations):
        if query.get_select() == FieldNames(['ip']):
            records = list()
            ip_list = query.get_filter().get_field_values('ip')
            for ip in ip_list:
                records.append({'ip': ip})
            return records
        return None
