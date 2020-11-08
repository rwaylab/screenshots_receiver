"""
 Why this exists?
 There's existing python package that can send log to logstash through Kafka,
 but it has two problems:
 1. It's built on top of a specific python kafka package and it's outdated,
 which left user no choice on which Kafka they can use.
 2. It only uses default log formatting from python logging package,
 which doesn't provide much useful information.
 Usage:
    import logging
    import KafkaLoggingHandler
    logger = logging.getLogger('python-logstash-logger')
    logger.setLevel(logging.INFO)
    log_producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
    logger.addHandler(KafkaLoggingHandler(log_producer, 'app-log'))
"""

import logging
import traceback
import socket
import sys
from datetime import datetime
try:
    import json
except ImportError:
    import simplejson as json
import pytz


class KafkaLoggingHandler(logging.Handler):

    def __init__(self, kafka_producer, kafka_topic,  tags=None, fqdn=False):
        logging.Handler.__init__(self)

        self.formatter = LogstashFormatter(tags, fqdn)

        self.producer = kafka_producer
        self.topic = kafka_topic

    def emit(self, record):
        # drop kafka logging to avoid infinite recursion
        if record.name == 'kafka':
            return
        try:
            # format message
            msg = self.formatter.format(record)
            if isinstance(msg, str):
                msg = msg.encode("utf-8")

            # produce message
            self.producer.send(self.topic, msg)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def close(self):
        if self.producer is not None:
            self.producer.close()
        logging.Handler.close(self)

class LogstashFormatterBase(logging.Formatter):

    def __init__(self, message_type='SDPy Kafla Logger', tags=None, fqdn=False):
        # self.message_type = message_type
        self.tags = tags if tags is not None else []

        if fqdn:
            self.host = socket.getfqdn()
        else:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('10.255.255.255', 1))
            host = s.getsockname()[0]
            s.close()
            self.host = host

    def get_extra_fields(self, record):
        # The list contains all the attributes listed in
        # http://docs.python.org/library/logging.html#logrecord-attributes
        skip_list = (
            'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
            'funcName', 'id', 'levelname', 'levelno', 'lineno', 'module',
            'msecs', 'msecs', 'message', 'msg', 'name', 'pathname', 'process',
            'processName', 'relativeCreated', 'thread', 'threadName', 'extra')

        if sys.version_info < (3, 0):
            easy_types = (basestring, bool, dict, float, int, long, list, type(None))
        else:
            easy_types = (str, bool, dict, float, int, list, type(None))

        fields = {}

        for key, value in record.__dict__.items():
            if key not in skip_list:
                if isinstance(value, easy_types):
                    fields[key] = value
                else:
                    fields[key] = repr(value)

        return fields

    def get_debug_fields(self, record):
        fields = {
            'stack_trace': self.format_exception(record.exc_info),
            'lineno': record.lineno,
            'process': record.process,
            'thread_name': record.threadName,
        }

        # funcName was added in 2.5
        if not getattr(record, 'funcName', None):
            fields['funcName'] = record.funcName

        # processName was added in 2.6
        if not getattr(record, 'processName', None):
            fields['processName'] = record.processName

        return fields

    @classmethod
    def format_source(cls, host, path):
        return "%s://%s/%s" % (host, path)

    @classmethod
    def format_timestamp(cls, time):
        tstamp = datetime.now(pytz.timezone('Europe/Moscow'))
        return tstamp.strftime("%Y-%m-%dT%H:%M:%S") + ".%03d" % (tstamp.microsecond / 1000) + "+0300"

    @classmethod
    def format_exception(cls, exc_info):
        return ''.join(traceback.format_exception(*exc_info)) if exc_info else ''

    @classmethod
    def serialize(cls, message):
        if sys.version_info < (3, 0):
            return json.dumps(message)
        else:
            return bytes(json.dumps(message), 'utf-8')

class LogstashFormatter(LogstashFormatterBase):

    def format(self, record):
        # Create message dict
        message = {
            'timestamp': self.format_timestamp(record.created),
            'description': record.getMessage(),
            'host': self.host,
            'application': record.module,
            'dest': {},
            'task_id':  getattr(record, 'task_id', ''),
            'type_msg': getattr(record, 'type_msg', 'INFO'),
            'addressee': getattr(record, 'addressee', 'developer'),
            # Extra Fields
            'level': record.levelname
        }

        # Add extra fields
        message.update(self.get_extra_fields(record))

        # If exception, add debug info
        if record.exc_info:
            message.update(self.get_debug_fields(record))

        return self.serialize(message)