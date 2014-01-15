#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# PatternParser class
#
# Jordan Augé      <jordan.auge@lip6.fr>
#
# Copyright (C) UPMC

import pyparsing as pp

class PatternParser(object):

    # XXX make this a singleton for avoiding concurrent access to pyparsing

    def __init__(self, query, basedir):
        """
        BNF
        """
        self.query = query
        self.basedir = basedir
        self.records = list() 
        self.found_fields = dict() 

        parameter = pp.Word(pp.alphas + '_' + '.')
        action    = pp.Word(pp.alphas, exact=1)
        command   = pp.Literal('%').suppress() + pp.Optional(pp.Literal('(').suppress() + parameter + pp.Literal(')').suppress()) + action
        command.setParseAction(self.handleCommand)
        path      = pp.delimitedList(command, delim='/')

        self.bnf = path.setParseAction(self.handleRecords)

    # OK : CRUD
    def command_t(self, parameter, context, query):
        """
        table = check dir, create if needed
        """
        newpath = os.path.join(context['path'], parameter)

        if not os.path.exists(newpath):
            if not query.get_action() == 'create':
                # "Interrupt parsing"
                return []
            os.mkdir(newpath)
        context['path'] = newpath
        return [context]

    # OK: R
    def command_f(self, parameter, context, query):
        """
        field = fail if not dir
        """
        # XXX assert parameter is a key field of table
        # XXX we need to find table in context
        path = context['path']
        contexts = []

        if query.get_action() == 'create':
            # XXX assert key should be present in the create query
            value = query.get_params()[parameter]
            newpath = os.path.join(path, value)
            if not os.path.isdir(newpath):
                os.mkdir(newpath)
                context['newkey'] = True # XXX check at the end
            if not 'newkey' in context:
                context['newkey'] = False
            if not 'found_fields' in context:
                context['found_fields'] = {}
            context['found_fields'][parameter] = value
            context['path'] = newpath
            contexts.append(context)

        elif query.get_action() == 'get':
            for d in os.listdir(path):
                newpath = os.path.join(path, d)
                if not os.path.isdir(newpath):
                    # XXX should not happen
                    continue
                # check if compatible with where
                # field = parameter, value = d
                if not query.get_where().match({parameter: d}, ignore_missing=True):
                    continue
                ctx = context.copy()
                # XXX maybe only keep needed found_fields... in records ???
                if not 'found_fields' in ctx:
                    ctx['found_fields'] = dict() 
                ctx['found_fields'][parameter] = d
                ctx['path'] = newpath
                contexts.append(ctx)
        return contexts

    def command_F(self, parameter, context, query):
        """
        file with key value inside
        """
        newpath = os.path.join(context['path'], parameter)
        
        if query.get_action() == 'create':
            # assert this file does not exists since we have a directory with
            # the key, unless the file stores multiple records... which is not
            # considered here
            # We open the file for writing
            f = open(newpath, 'w')
            # Write all params _but_ found_fields
            for k, v in query.get_params().items():
                if k not in context['found_fields']:
                    print >>f, "%s=%s" % (k, v)
            f.close()
        else: # GET
            try:
                f = open(newpath, 'r')
            except IOError:                     
                return []
            try:
                record = {}
                for line in f:
                    key, value = line.strip().split('=')
                    context['found_fields'][key] = value
                # We only append finished records. Could we do otherwise with keys ?
                # suppose context['found_fields'] exists since we have a subdir with the key
            finally:
                f.close()
        return [context] # XXX terminal !

    def handleCommand(self, args):
        # until we can return a default value for optional
        try:
            parameter, command = args
        except:
            command,  = args
            parameter = None

        try:
            cmd = getattr(self, "command_%s" % command)
        except Exception, e:
            print 'E:', e
            raise Exception, "Unknown command: '%s'" % command

        contexts = self.contexts[:]
        self.contexts = []
        for context in contexts:
            try:
                contexts = cmd(parameter, context, self.query)
            except Exception, e:
                print "EXC", e
                import traceback
                traceback.print_exc()
            self.contexts.extend(contexts)

    def handleRecords(self, args):
        return [context['found_fields'] for context in self.contexts]

    def parse(self, string):
        self.contexts = [{'path': self.basedir, 'records': []}]
        out = self.bnf.parseString(string, parseAll=True)
        return out.asList()
        


