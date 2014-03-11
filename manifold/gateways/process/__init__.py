# -*- coding: utf-8 -*-

import subprocess

from manifold.core.announce     import Announce
from manifold.core.field        import Field
from manifold.gateways          import Gateway
from manifold.core.key          import Key
from manifold.core.table        import Table
from manifold.util.log          import Log
from manifold.util.type         import accepts, returns

class ProcessField(dict):
    def get_name(self):         return self.get('name')
    def get_type(self):         return self.get('type')
    def get_description(self):  return self.get('description')
    def get_default(self):      return self.get('default')
    def get_short(self):        return self.get('short')
    def get_flags(self):        return self.get('flags', FLAG_NONE)

class Argument(ProcessField):
    pass

class Parameter(ProcessField):
    pass

class Output(ProcessField):
    pass

FIELD_TYPE_ARGUMENT     = 1
FIELD_TYPE_PARAMETER    = 2
FIELD_TYPE_OUTPUT       = 3

FLAG_NONE               = 0
FLAG_IN_PARAMS          = 1<<0
FLAG_IN_ANNOTATION      = 1<<1
FLAG_OUT_ANNOTATION     = 1<<2
FLAG_ADD_FIELD          = 1<<3

class ProcessGateway(Gateway):
    __gateway_name__ = 'process'

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    # XXX Args should be made optional
    def __init__(self):
        """
        Constructor

        Args:
            router: None or a Router instance
            platform: A StringValue. You may pass u"dummy" for example
            platform_config: A dictionnary containing information to connect to the postgresql server
        """
        self._process = None
        self._is_interrupted = False

        Gateway.__init__(self)

    #---------------------------------------------------------------------------
    # Packet processing
    #---------------------------------------------------------------------------

    def receive_impl(self, packet): 
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        query       = packet.get_query()
        annotation  = packet.get_annotation()

        # We have a single table per tool gateway
        # table_name = query.get_from()

        # Compute process arguments from query
        args = (self.get_fullpath(),) + self.get_argtuple(query, annotation)

        tmp_filename = '/tmp/manifold-process'
        ret = self.execute_process(args, tmp_filename)
        rows = self.parse(tmp_filename) if ret >= 0 else None

        #if not out:
        #    if os.path.exists(filename_raw):
        #        os.unlink(filename_raw)
        #    # XXX LOG HERE FOR IMPROVING MODULE
        #    # XXX Shall we signal we lost track of the IP ?

        self.records(rows, packet)

    #---------------------------------------------------------------------------
    # Specific methods
    #---------------------------------------------------------------------------

    def get_num_arguments(self):
        return len(self.arguments)
    get_min_arguments = get_num_arguments
    get_max_arguments = get_num_arguments

    def get_argtuple(self, query, annotation):
        args = tuple()

        for field_type, field_list in [
            (FIELD_TYPE_PARAMETER,  self.parameters),
            (FIELD_TYPE_ARGUMENT,   self.arguments),
            (FIELD_TYPE_OUTPUT,     self.output)]:

            for process_field in field_list:
                if field_type not in [FIELD_TYPE_PARAMETER, FIELD_TYPE_ARGUMENT]:
                    continue

                name  = process_field.get_name()
                flags = process_field.get_flags()

                value = None


                # Check whether the field is specified in params (or annotations)...
                if flags & FLAG_IN_PARAMS:
                    value = query.get_params().get(name)
                # ... or as a filter
                if flags & FLAG_IN_ANNOTATION:
                    value = annotation.get(name)
                else:
                    # XXX At the moment we are only supporting eq
                    # XXX We might have tuples
                    value = query.get_filter().get_eq(name)

                if not value:
                    default = process_field.get_default()
                    if default:
                        value = default

                # XXX Argument with no value == ERROR

                # XXX We might have a list 
                if field_type == FIELD_TYPE_PARAMETER:
                    short = process_field.get_short()
                    if value:
                        args += (short, str(value))
                else:
                    args += (value,)

        return args

    def execute_process(self, args, filename):
        ret = 0
        try:
            f = None
            try:
                f = open(filename, 'w')
                self._process = subprocess.Popen(args, stdout=f, stderr=None)
                self._process.wait()
            finally:
                if f: f.close()

            try:
                ret = self._process.returncode
            except:
                # Process has been killed ?
                return -1

            self._process = None
            if (ret != 0):
                # -2 SIGINT (keyboard interrupt) : occured when I did a Ctrl+C
                #
                # -6 SIGABRT
                # need to change port number... how to change port number and keep the same flow ? XXX
                # Exception : [ERROR](Server.cc, 65)Can't bind Plab socket
                # terminate called after throwing an instance of 'TrException'
                # Aborted
                #
                # -11 SIGSEGV (reference mémoire invalide)
                # ???? maybe two at the same time ?
                #
                # -15 SIGTERM
                # 17  128.112.139.2  121.803 ms !? -> Fixed parser (unk icmp code for pt) !

                                # Riad
                                # Commented since agents send a lot of error messages of types -6 and -11
                # log.error("paris-traceroute [dest_id=%d] %r returned %d" % (dest_id, args,ret))
                return -1

        except OSError, e:
            if e.errno == 10:
                if not self._is_interrupted:
                    Log.error("Process has been killed: %s" % e)
            else:
                raise
        except Exception, e:
            import traceback
            traceback.print_exc()
            Log.error("Execution failed: %s" % e)
            return -1

        return ret

    def interrupt(self):
        """
        \brief Kill then ParisTraceroute instance
        """
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

    @staticmethod
    def get_hostname():
        return subprocess.Popen(["uname", "-n"], stdout=subprocess.PIPE).communicate()[0].strip()


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
        return os.path.exists(cls.get_fullpath().split()[0])

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

 
    #---------------------------------------------------------------------------
    # Metadata 
    #---------------------------------------------------------------------------
    # This has to be kept synchronized with the parser and the Argument
    # Arguments could be mapped simply with fields
    #---------------------------------------------------------------------------

    def make_field(self, process_field, field_type):
        # name, type and description - provided by the process_field description
        # qualifier - Since we cannot update anything, all fields will be const
        field = Field(
            name        = process_field.get_name(),
            type        = process_field.get_type(),
            qualifiers  = ['const'],
            is_array    = False,
            description = process_field.get_description()
        )
        return field

    @returns(Announce)
    def make_announce(self):
        """
        Returns:
            The Announce related to this object.
        """
        platform_name = 'dummy' #self.get_gateway().get_platform_name()https://code.google.com/p/paris-traceroute/wiki/Git#Push_a_local_branch_to_the_git_server

        # TABLE NAME
        #
        # The name of the tool might not be sufficient since some parameters
        # might affect the type of the measurement being performed

        table_name    = 'ping'

        t = Table(platform_name, table_name)

        # FIELDS
        #
        # Fields are found in parameters, arguments and output
        for field_type, field_list in [
            (FIELD_TYPE_ARGUMENT,   self.arguments),
            (FIELD_TYPE_PARAMETER,  self.parameters),
            (FIELD_TYPE_OUTPUT,     self.output)]:

            for process_field in field_list:
                field = self.make_field(process_field, field_type)
                t.insert_field(field)

        # KEYS
        #
        # Keys will be fields that affect the measurement, so of course
        # arguments, but also some parameters. We will typically have a single
        # key.

        key = set()
        for argument in self.arguments:
            key.add(t.get_field(argument.get_name()))
        t.insert_key(Key(key))

        # CAPABILITIES
        #    . PROJECTION == si les champs peuvent etre pilotés par les options
        #    . SELECTION  == jamais en pratique
        #    . FULLQUERY == jamais
        
        # As tools will generally take parameters (key for the measurement) it
        # will not be possible to retrieve data from them directly (ON JOIN
        # tables).
        t.capabilities.retrieve     = False
        t.capabilities.join         = True

        # A tool will support almost never support selection
        t.capabilities.selection    = False

        # A tool will support projection if all fields can be controlled through
        # options. In general, only partial projection will be supported (since
        # output will always provide some fields that cannot be removed). Let's
        # assume False for now for simplicity.
        t.capabilities.projection   = False

        announce = Announce(t)

        return announce
