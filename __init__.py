# -*- coding: utf-8 -*-
from restrek.errors import RestrekError
from restrek.core import RestrekCommand, DEFAULT_KEY
from restrek.utils import milli2sec

from sys import path, argv, modules
from os.path import abspath, dirname, join
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


SCHEMA_IN = {
    'host': {
        'desc': 'url to call',
        DEFAULT_KEY: 'localhost'
    },
    'port': {
        'desc': 'port to connect to',
        DEFAULT_KEY: '8001'
    },
    'gen_dir': {
        'desc': 'absolute path of the generated service'
    },
    'service_path': {
        'desc': 'service\'s python module path'
    },
    'timeout': {
        'desc': 'if supported the command should use it (in ms)',
        DEFAULT_KEY: 1000
    },
    'call': {
        'desc': 'which function to call'
    }
}

SCHEMA_OUT = {
    'status': 'the status code returned',
    'message': 'in case of an error, the message',
    'data': 'data return'
}


class ThriftCommand(RestrekCommand):

    def __init__(self, name, props, payload):
        RestrekCommand.__init__(self, name, props, payload, SCHEMA_IN, SCHEMA_OUT)

    def run(self):
        self.log('[REQUEST]')
        self.log('service: %s' % self.service_path)
        self.log('calling: %s' % self.call)
        self.log('arguments: %r ' % self.payload)

        status = 0
        message = None
        data = None

        try:
            tsocket = TSocket.TSocket(self.host, self.port)
            transport = TTransport.TBufferedTransport(tsocket)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)

            # load thrift generated service
            path.append(self.gen_dir)
            __import__(self.service_path)
            client = modules[self.service_path].Client(protocol)

            transport.open()
            ret = getattr(client, self.call)(*self.payload)
            transport.close()
            data = ret
        except Thrift.TException, tx:
            message = tx.message
            status = tx.type

        self.output = {
            'status': status,
            'message': message,
            'data': data
        }

        self.log('[RESPONSE]')
        self.log(self.output)

    def parse_registration_statements(self, registration_statements):
        vars_to_add = dict()
        return vars_to_add


command = ThriftCommand
