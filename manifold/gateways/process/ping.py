#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pyparsing as pp
from manifold.util.filesystem import hostname
from .             import ProcessGateway, ProcessObject, Argument, Parameter, FLAG_IN_ANNOTATION, FLAG_OUT_ANNOTATION, FLAG_ADD_FIELD
from ...util.log   import Log

class PingParser(object):
    """
    Parses the `ping_output` string into a dictionary containing the following
    fields:

        `host`: *string*; the target hostname that was pinged
        `sent`: *int*; the number of ping request packets sent
        `received`: *int*; the number of ping reply packets received
        `minping`: *float*; the minimum (fastest) round trip ping request/reply
                    time in milliseconds
        `avgping`: *float*; the average round trip ping time in milliseconds
        `maxping`: *float*; the maximum (slowest) round trip ping time in
                    milliseconds
        `jitter`: *float*; the standard deviation between round trip ping times
                    in milliseconds

    see. https://github.com/gg/pingparser/blob/master/src/pingparser.py
    """

    def __init__(self):
        """
        Example:

        PING 8.8.8.8 (8.8.8.8) 56(84) bytes of data. 
        64 bytes from 8.8.8.8: icmp_seq=1 ttl=46 time=26.4 ms 
         
        --- 8.8.8.8 ping statistics --- 
        1 packets transmitted, 1 received, 0% packet loss, time 0ms 
        rtt min/avg/max/mdev = 26.464/26.464/26.464/0.000 ms

        """
        #(gromit@ns384702)(~)  ping 193.0.6.139
        #PING 193.0.6.139 (193.0.6.139) 56(84) bytes of data.
        #From 195.69.144.68 icmp_seq=2 Packet filtered
        #^C
        #--- 193.0.6.139 ping statistics ---
        #8 packets transmitted, 0 received, +1 errors, 100% packet loss, time
        #7047ms

        BPAR = pp.Literal('(')
        EPAR = pp.Literal(')')

        # Bof:   
        ip       = pp.Combine(pp.Word(pp.nums) + ('.' + pp.Word(pp.nums))*3)
        hostnamepart = pp.Word(pp.alphas, pp.alphanums+"_")
        hostname = pp.Combine( hostnamepart + pp.ZeroOrMore("." + hostnamepart) )
        hostref = ip | hostname
        integer = pp.Word(pp.nums)\
                .setParseAction(lambda t:int(t[0]))
        float   = pp.Regex(r'\d+(\.\d*)?([eE]\d+)?')
             
        header = (
                pp.Literal("PING").suppress() 
            +   hostref.setResultsName('hostname')
            +   BPAR.suppress()
            +   ip.setResultsName('ip')
            +   EPAR.suppress()
            +   integer.setResultsName('size1')
            +   BPAR.suppress()
            +   integer.setResultsName('size2')
            +   EPAR.suppress()
            +   pp.Literal("bytes of data.").suppress()
        ).setParseAction(lambda t: t.asDict())

        # https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=609853
        # Some versions of ping use "icmp_req" instead of "icmp_seq"
        probe = (
                integer.setResultsName('size3')
            +   pp.Literal("bytes from").suppress()
            +   hostref.setResultsName('ip2')
            +   pp.Optional( 
                    BPAR.suppress()
                +   ip.setResultsName('ip2')
                +   EPAR.suppress())
            +   pp.Literal(":").suppress()
            +   pp.Regex("icmp_(s|r)eq=")
            +   integer.setResultsName('seq')
            +   pp.Literal("ttl=").suppress()
            +   integer.setResultsName('ttl')
            +   pp.Literal("time=").suppress()
            +   float.setResultsName('delay')
            +   pp.Literal("ms").suppress()
        ).setParseAction(lambda t: t.asDict())

        probe_list = (
            pp.delimitedList(probe, delim='')
        ).setParseAction(lambda t: list([t.asList()]))

        stat_header  = (
                pp.Literal("---")
            +   hostref
            +   pp.Literal("ping statistics ---")
        ).suppress()

        stat_packets = (
                integer.setResultsName('sent')
            +   pp.Literal("packets transmitted,").suppress() 
            +   integer.setResultsName('received')
            +   pp.Literal("received,").suppress()
            +   integer.setResultsName('lost')
            +   pp.Literal("% packet loss, time ").suppress()
            +   integer.setResultsName('time')
            +   pp.Literal("ms").suppress()
        ).setParseAction(lambda t: t.asDict())

        stat_delays = (
                pp.Literal("rtt min/avg/max/mdev =").suppress()
            +   float.setResultsName('min_rtt')
            +   pp.Literal("/").suppress()
            +   float.setResultsName('avg_rtt')
            +   pp.Literal("/").suppress()
            +   float.setResultsName('max_rtt')
            +   pp.Literal("/").suppress()
            +   float.setResultsName('jitter_rtt')
            +   pp.Literal("ms").suppress()
        ).setParseAction(lambda t: t.asDict())
            
        statistics = (
                stat_header 
            +   stat_packets 
            +   stat_delays
        ).suppress()

        self.bnf = (
                header
            +   probe_list
            +   statistics.suppress()
        ).setParseAction(self._handle_ping)

    def _handle_ping(self, token):
        header, probes = token
        header['probes'] = probes
        header['source'] = hostname()
        return header

    def parse(self, string):
        try:
            result = self.bnf.parseString(string, parseAll=True)
            return result.asList()
        except pp.ParseException, e:
            Log.warning("Error line %s, column %s:" % (e.lineno, e.col))
            Log.warning(e.line)
            Log.warning(" " * e.col, "^--- syntax error")
        return []

class PingObject(ProcessObject):
    """
    class ping {
        inet source;
        inet destination;
        probe_ping probes[];
        CAPABILITY(join);
        KEY(source, destination);
    };
    """
    # Define record annotation, timestamp
    # WHERE tool = 
    # SOURCE ROUTING = annotations

    # 1 champ = multiple options + values
    # N champs = 1 options
    # N / M
    #
    # + default values
    # negation options
    # -vvvvv
    #
    # tout est const
    #
    # PARTITIONS ?
    #
    # Key = param obligatoires
    # Options = subtypes ?? eg. algo in paristraceroute

    # Si une option altere la mesure, c'est 2 outils différents
    #   eg. -c altere pas resultat
    # Valeurs par defaut

    # default
    #   Our default values override the tool default
    # 
    # flags:
    #  IN_PARAMS : The value of the option will not be found in filters but in params
    #  OUT_ANNOTATIONS
    #  ADD_FIELD : This flag adds a field to the resulting record
    #  OPTIONAL  : (for arguments only) This argument is optional
    __object_name__ = 'ping'
    __tool__ = 'ping'

    parameters = [
        Parameter(
            name        = 'count',
            type        = 'integer',
            description = '',
            short       = '-c',
            default     = 1,
            flags       = FLAG_IN_ANNOTATION | FLAG_OUT_ANNOTATION,
        ),
        Parameter(
            name        = 'hostname', 
            type        = 'string', 
            description = '',
            short       = '-n',
            action      = '', 
            # METADATA + IN_FIELDS
            flags       = FLAG_ADD_FIELD
        ),
    ]
    arguments = [
        Argument(
            name        = 'destination',
            type        = 'ip'
        ),
    ]
    parser = PingParser
    path = '/bin/ping'

class ProbePingObject(ProcessObject):
    """
    class probe_ping {
        float delay;
        CAPABILITY(join);
        LOCAL KEY();
    };
    """

class PingGateway(ProcessGateway):
    __gateway_name__ = 'ping'
    def __init__(self, router = None, platform_name = None, platform_config = None):
        """
        Constructor

        Args:
            router: None or a Router instance
            platform: A StringValue. You may pass u"dummy" for example
            platform_config: A dictionnary containing information to connect to the postgresql server
        """
        ProcessGateway.__init__(self, router, platform_name, platform_config)

        self.register_object(PingObject)
        self.register_object(ProbePingObject)
