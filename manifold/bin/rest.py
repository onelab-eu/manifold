#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
import tornado.ioloop
import tornado.web
from tornado                        import gen
from tornado.httpclient             import AsyncHTTPClient
from tornado.httpserver             import HTTPServer
from tornado.ioloop                 import IOLoop
from tornado.tcpserver              import TCPServer

from manifold.auth                  import *
from manifold.clients.router        import ManifoldRouterClient
from manifold.core.filter           import Filter
from manifold.core.packet           import Packet
from manifold.core.query            import Query
from manifold.core.query_factory    import QueryFactory
from manifold.input.sql             import SQLParser
import sys, pprint
import xmlrpclib
import json
import re

#srv = xmlrpclib.Server("http://127.0.0.1:7080/", allow_none = True)
#auth = {'authentication':{'AuthMethod': 'password', 'Username': 'demo', 'AuthString': 'demo'}}
router = ManifoldRouterClient(user_email="admin")

class TornadoRest(tornado.web.RequestHandler):

    @gen.coroutine
    def get(self, *args):
        try:
            print "args = "
            pprint.pprint(args)

            obj = args[0].split("/")[0]
            u_filters = self.get_arguments('filter')

            print "u_filters = "
            pprint.pprint(u_filters)

            l_filters = list()
            for f in u_filters:
                f = re.sub('[()]','',f)
                print "THIS IS MY FILTER = "
                print f
                if '_eq_' in f:
                    f = f.replace('_eq_','==')
                if '_lt_' in f:
                    f = f.replace('_lt_','<')
                if '_lte_' in f:
                    f = f.replace('_lte_','<=')
                if '_gt_' in f:
                    f = f.replace('_gt_','>')
                if '_gte_' in f:
                    f = f.replace('_gte_','>=')
                if '_ne_' in f:
                    f = f.replace('_ne_','!=')
                l_filters.extend(f.split("and"))
            #for i, item in enumerate(l_filters):
            #    l_filters[i] = item.split(",")

            u_fields = self.get_arguments('fields')
            print "u_fields = ", u_fields
            if len(u_fields) == 0:
                fields = "*"
            else:
                fields = u_fields[0]

            print "l_filters = "
            pprint.pprint(l_filters)
            d = {}
            ret = yield self.get_data(obj, fields, l_filters)
            if ret['code'] != 0:
                pprint.pprint(ret['description'])
                d['error_description'] = [desc.get_message() for desc in ret['description']][0]
                d['error'] = ret['code']
                self.set_status(status_code=400)
            else:
                v = ret['value']
                d['value'] = v.to_dict_list()
            self.write(json.dumps(d))
        except Exception, e:
            print "Exception: %s" % e
            import traceback
            traceback.print_exc()
            self.send_error(status_code=500)

    @gen.coroutine
    def print_err(self, err):
        res = '-'*80
        res += 'Exception', err['code'], 'raised by', err['origin'], ':', err['description']
        for line in err['traceback'].split("\n"):
            res += "\t", line
            res += ''
        return res

    @gen.coroutine
    def get_data(self, obj, fields="*", filters=None):
        command = "SELECT " + fields + " FROM " + obj
        if len(filters) > 0:
            command += " WHERE "
            f = " && ".join(filters)
            command += f
        print command
        dic = SQLParser().parse(command)
        pprint.pprint(dic)
        query = QueryFactory.from_dict(dic)
        print "Query = %s" % query
        ret = router.forward(query)
        return ret

application = tornado.web.Application([
    (r"/(.*)", TornadoRest),
])

def main():
    """
    Run Manifold REST API.
    """
    Log.info("Starting Manifold REST based on Tornado")

    # Advanced Multi-Process
    # http://www.tornadoweb.org/en/stable/tcpserver.html#tornado.tcpserver.TCPServer
    sockets = tornado.netutil.bind_sockets(80)

    # If num_processes is None or <= 0
    # we detect the number of cores available on this machine
    # and fork that number of child processes.
    # If num_processes is given and > 0
    # we fork that specific number of sub-processes.
    # http://www.tornadoweb.org/en/stable/process.html#tornado.process.fork_processes
    tornado.process.fork_processes(0)

    # http://www.tornadoweb.org/en/stable/httpserver.html#tornado.httpserver.HTTPServer
    server = HTTPServer(application)

    # add_sockets: advanced multi-process
    server.add_sockets(sockets)

    IOLoop.current().start()

    # Single Thread for Debug
    #application.listen(8888)
    #tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()
