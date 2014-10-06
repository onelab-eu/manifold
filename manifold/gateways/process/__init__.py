# -*- coding: utf-8 -*-

import threading, subprocess, uuid, os

from ...core.announce           import Announces
from manifold.core.field        import Field
from manifold.gateways          import Gateway
from manifold.core.key          import Key
from manifold.core.table        import Table
from manifold.gateways.object   import ManifoldCollection
from manifold.util.log          import Log
from manifold.util.misc         import is_iterable
from manifold.util.predicate    import eq, included
from manifold.util.type         import accepts, returns

class ProcessField(dict):
    def get_name(self):         return self.get('name')
    def get_type(self):         return self.get('type')
    def get_description(self):  return self.get('description')
    def get_default(self):      return self.get('default')
    def get_short(self):        return self.get('short')
    def get_flags(self):        return self.get('flags', FLAG_NONE)
    def get_prefix(self):       return self.get('prefix')

class Argument(ProcessField):
    pass

class FixedArgument(Argument):
    def get_value(self):        return self.get('value')

class Parameter(ProcessField):
    pass

FIELD_TYPE_ARGUMENT     = 1
FIELD_TYPE_PARAMETER    = 2
#FIELD_TYPE_OUTPUT       = 3

FLAG_NONE               = 0
FLAG_IN_PARAMS          = 1<<0
FLAG_IN_ANNOTATION      = 1<<1
FLAG_OUT_ANNOTATION     = 1<<2
FLAG_ADD_FIELD          = 1<<3

class ProcessCollection(ManifoldCollection):

    def __init__(self, *args, **kwargs):
        ManifoldCollection.__init__(self, *args, **kwargs)

        self._in_progress = dict()
        self._records = dict()
        self._process = None
        self._is_interrupted = False

    def get(self, packet):
        if not os.path.exists(self.get_fullpath()):
            Log.warning("Process does not exist, returning empty")
            self.get_gateway().records([], packet)
            return


        query       = packet.get_query()
        annotation = packet.get_annotation()

        Log.tmp("[PROCESS GATEWAY] received query", query)
        # We leave the process a chance to return records without executing
        output = self.on_receive_query(query, annotation)
        if output:
            print "return direct output=", output
            self.get_gateway().records(output, packet)
            return

        # We have a single table per tool gateway
        # table_name = query.get_table_name()

        # Compute process arguments from query
        # At the moment, we cannot have more than ONE value for each
        # parameter/filter XXX XXX XXX XXX

        output = list()

        # ( arg tuples, parameters )
        args_params_list = self.get_argtuples(query, annotation)
        Log.tmp("[PROCESS GATEWAY] argtuples", args_params_list)

        batch_id = str(uuid.uuid4())

        self._in_progress[batch_id] = len(args_params_list)

        for args_params in args_params_list:
            args = (self.get_fullpath(),) + args_params[0]

            Log.tmp("[PROCESS GATEWAY] execute args=%r, args_params[1]=%r" % (args, args_params[1],))
            self.execute_process(args, args_params[1], packet, batch_id)

    #---------------------------------------------------------------------------
    # Packet processing
    #---------------------------------------------------------------------------

    def parse(self, string):
        return self.parser().parse(string)

    def on_receive_query(self, query, annotation):
        return None

    #---------------------------------------------------------------------------
    # Specific methods
    #---------------------------------------------------------------------------

    def get_num_arguments(self):
        return len(self.arguments)
    get_min_arguments = get_num_arguments
    get_max_arguments = get_num_arguments

    def get_argtuples(self, query, annotation):
        """
        This function creates the list of arguments of the program by getting
        parameter and argument values from the query.

        Return value:
        A list of tuples (args, params)
            args = the command line to execute
            params = the manifold representation of the query associated to a
            command line
        """
        args = tuple()
        params = dict()
        ret = [(args, params,)]

        args = tuple()
        params = dict()

        for field_type, field_list in [
            (FIELD_TYPE_PARAMETER,  self.parameters),
            (FIELD_TYPE_ARGUMENT,   self.arguments)]:

            for process_field in field_list:
                if field_type not in [FIELD_TYPE_PARAMETER, FIELD_TYPE_ARGUMENT]:
                    continue

                # FixedArgument
                if isinstance(process_field, FixedArgument):
                    args += (process_field.get_value(), )
                    continue

                name  = process_field.get_name()
                flags = process_field.get_flags()

                value = None


                # Check whether the field is specified in params (or annotation)...
                if flags & FLAG_IN_PARAMS:
                    value = query.get_params().get(name)
                # ... or as a filter
                if flags & FLAG_IN_ANNOTATION:
                    value = annotation.get(name)
                else:
                    # XXX At the moment we are only supporting eq
                    # XXX We might have tuples
                    value = query.get_filter().get_op(name, (eq, included))

                if not value:
                    default = process_field.get_default()
                    if default:
                        value = default

                # XXX Argument with no value == ERROR
                prefix = process_field.get_prefix()

                if field_type == FIELD_TYPE_PARAMETER:
                    # Parameters
                    if not value:
                        continue
                    short = process_field.get_short()

                    oldret, ret = ret, list()
                    if is_iterable(value):
                        for args, params in oldret:
                            for v in value:
                                v = str(v)
                                argvalue = "%s%s" % (prefix, v) if prefix else v
                                params[name] = v
                                newargs = args + (short, argvalue,)
                                ret.append( (newargs, params,) )

                    else:
                        value = str(value)
                        argvalue = "%s%s" % (prefix, value) if prefix else value
                        for args, params in oldret:
                            params[name] = value
                            newargs = args + (short, argvalue,)
                            ret.append( (newargs, params,) )
                else:
                    # Arguments
                    if is_iterable(value):
                        oldret, ret = ret, list()
                        for args, params in oldret:
                            for v in value:
                                argvalue = "%s%s" % (prefix, v) if prefix else v
                                params[name] = v
                                newargs = args + (argvalue,)
                                ret.append( (newargs, params,) )
                    else:
                        argvalue = "%s%s" % (prefix, value) if prefix else value
                        for args, params in ret:
                            params[name] = value
                            newargs = args + (argvalue,)
                            ret.append( (newargs, params,) )
        return ret

    def execute_process(self, args, params, packet, batch_id):
        ret = 0
        #print "execute process", args, params, packet, batch_id

        def runInThread(args, params, packet, batch_id):
            self._records[batch_id] = []
            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr = None)
            try:
                output = process.stdout.read()

                ret = process.returncode
                if (ret != 0):
                    # -2 SIGINT (keyboard interrupt) : occured when I did a Ctrl+C
                    # -6 SIGABRT
                    # -11 SIGSEGV (reference mémoire invalide)
                    # -15 SIGTERM
                    if output:
                        # dig returns something even when return code is != 0
                        rows = self.parse(output)
                    else:
                        rows = []
                else:
                    rows = self.parse(output)

                if rows:
                    for row in rows:
                        record = dict()
                        record.update(params)
                        record.update(row)
                        self._records[batch_id].append(record)

            except OSError, e:
                if e.errno == 10:
                    if not self._is_interrupted:
                        Log.error("Process has been killed: %s" % e)
            except Exception, e:
                import traceback
                traceback.print_exc()
                Log.error("Execution failed: %s" % e)
                return
            finally:
                self._in_progress[batch_id] -= 1
                if self._in_progress[batch_id] == 0:
                    self.get_gateway().records(self._records[batch_id], packet)
                    del self._records[batch_id]
                    del self._in_progress[batch_id]


        thread = threading.Thread(target=runInThread, args=(args, params, packet, batch_id))
        thread.start()
        return thread

    def interrupt(self):
        """
        \brief Kill then ParisTraceroute instance
        """
        return # no more self._process
        self._is_interrupted = True
        try:
            if not self._process: return
            try:
                # Both terminations in effet
                self._process.terminate()
            except AttributeError:
                # BugFix: python<2.6: Popen::terminate() does not exists
                # See for example upmc_agent@planet0.jaist.ac.jp
                import signal
                os.kill(process.pid, signal.SIGKILL)
            except OSError, e:
                if e.errno == 3:
                    pass  # no such process
                else:
                    raise

            # BugFix: 'NoneType' object has no attribute 'wait'
            if self._process:
                try:
                    self._process.wait()
                except OSError, e:
                    if e.errno == 10:
                        pass  # no child process
                    else:
                        raise
                self._process = None
        except Exception, e:
            Log.warning('IGNORED EXCEPTION DURING PROCESS INTERRUPT/KILL: %s' % e)

    @staticmethod
    def get_ip_address(dst):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((dst, 0))
        return s.getsockname()[0]

    @classmethod
    def get_parser():
        return self.parser

    @classmethod
    def get_fullpath(cls):
        return cls.path

    @classmethod
    def check_path(cls):
        """
        \brief Check if paris-traceroute is properly installed
        \return True iif everything is fine
        """
        return os.path.exists(cls.get_fullpath()) #.split()[0])

    @classmethod
    def check_uid(cls):
        """
        \brief Check if paris-traceroute is properly instantiated
        \return True iif everything is fine
        """
        # XXX setuid bit / CAP_NET_RAW could be good also
        return os.getuid() == 0

#    @classmethod
#    def check_all(cls):
#        if need_uid...


class ProcessGateway(Gateway):
    __gateway_name__ = 'process'

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    # XXX Args should be made optional
    def __init__(self, router = None, platform_name = None, platform_config = None):
        """
        Constructor

        Args:
            router: None or a Router instance
            platform: A StringValue. You may pass u"dummy" for example
            platform_config: A dictionnary containing information to connect to the postgresql server
        """
        Gateway.__init__(self, router, platform_name, platform_config)

    #---------------------------------------------------------------------------
    # Metadata
    #---------------------------------------------------------------------------
    # This has to be kept synchronized with the parser and the Argument
    # Arguments could be mapped simply with fields
    #---------------------------------------------------------------------------

#DEPRECATED|    def make_field(self, process_field, field_type):
#DEPRECATED|        # name, type and description - provided by the process_field description
#DEPRECATED|        # qualifier - Since we cannot update anything, all fields will be const
#DEPRECATED|        field = Field(
#DEPRECATED|            name        = process_field.get_name(),
#DEPRECATED|            type        = process_field.get_type(),
#DEPRECATED|            qualifiers  = ['const'],
#DEPRECATED|            is_array    = False,
#DEPRECATED|            description = process_field.get_description()
#DEPRECATED|        )
#DEPRECATED|        return field
#DEPRECATED|
#DEPRECATED|    @returns(Announces)
#DEPRECATED|    def make_announces(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            The Announce related to this object.
#DEPRECATED|        """
#DEPRECATED|        platform_name = self.get_platform_name()
#DEPRECATED|        try:
#DEPRECATED|            announces = parse_string(self.output._announces_str, platform_name)
#DEPRECATED|        except AttributeError, e:
#DEPRECATED|            # self.output not yet initialized
#DEPRECATED|            raise AttributeError("In platform '%s': %s" % (platform_name, e))
#DEPRECATED|
#DEPRECATED|        # These announces should be complete, we only need to deal with argument
#DEPRECATED|        # and parameters to forge the command line corresponding to an incoming
#DEPRECATED|        # query.
#DEPRECATED|        return announces

#DEPRECATED|        # TABLE NAME
#DEPRECATED|        #
#DEPRECATED|        # The name of the tool might not be sufficient since some parameters
#DEPRECATED|        # might affect the type of the measurement being performed
#DEPRECATED|
#DEPRECATED|        table_name    = self.__tool__
#DEPRECATED|
#DEPRECATED|        t = Table(platform_name, table_name)
#DEPRECATED|
#DEPRECATED|        # FIELDS
#DEPRECATED|        #
#DEPRECATED|        # Fields are found in parameters, arguments and output
#DEPRECATED|        for field_type, field_list in [
#DEPRECATED|            (FIELD_TYPE_ARGUMENT,   self.arguments),
#DEPRECATED|            (FIELD_TYPE_PARAMETER,  self.parameters),
#DEPRECATED|            (FIELD_TYPE_OUTPUT,     self.output)]:
#DEPRECATED|
#DEPRECATED|            for process_field in field_list:
#DEPRECATED|                field = self.make_field(process_field, field_type)
#DEPRECATED|                t.insert_field(field)
#DEPRECATED|
#DEPRECATED|        # KEYS
#DEPRECATED|        #
#DEPRECATED|        # Keys will be fields that affect the measurement, so of course
#DEPRECATED|        # arguments, but also some parameters. We will typically have a single
#DEPRECATED|        # key.
#DEPRECATED|
#DEPRECATED|        key = set()
#DEPRECATED|        for argument in self.arguments:
#DEPRECATED|            key.add(t.get_field(argument.get_name()))
#DEPRECATED|        t.insert_key(Key(key))
#DEPRECATED|
#DEPRECATED|        # CAPABILITIES
#DEPRECATED|        #    . PROJECTION == si les champs peuvent etre pilotés par les options
#DEPRECATED|        #    . SELECTION  == jamais en pratique
#DEPRECATED|        #    . FULLQUERY == jamais
#DEPRECATED|
#DEPRECATED|        # As tools will generally take parameters (key for the measurement) it
#DEPRECATED|        # will not be possible to retrieve data from them directly (ON JOIN
#DEPRECATED|        # tables).
#DEPRECATED|        t.capabilities.retrieve     = False
#DEPRECATED|        t.capabilities.join         = True
#DEPRECATED|
#DEPRECATED|        # A tool will support almost never support selection
#DEPRECATED|        t.capabilities.selection    = False
#DEPRECATED|
#DEPRECATED|        # A tool will support projection if all fields can be controlled through
#DEPRECATED|        # options. In general, only partial projection will be supported (since
#DEPRECATED|        # output will always provide some fields that cannot be removed). Let's
#DEPRECATED|        # assume False for now for simplicity.
#DEPRECATED|        t.capabilities.projection   = False
#DEPRECATED|
#DEPRECATED|        announce = Announce(t)
#DEPRECATED|
#DEPRECATED|        return [announce]
