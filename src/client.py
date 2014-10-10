# -*- coding: utf-8 -*-
import msgpack
import gevent
from gevent import socket
from gevent.event import Event
from gevent.queue import Queue
import logging

MSGPACKRPC_REQ = 0
MSGPACKRPC_RSP = 1
class RPCClient(object):
    def __init__(self, host, port, timeout=5.0, poolsize=10, chunksize=1024*32, pack_encoding='utf-8', unpack_encoding='utf-8'):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._poolsize = poolsize
        self._sendqueue = Queue(maxsize=poolsize)
        self._msgpool = dict()
        self._pack_encoding = pack_encoding
        self._unpack_encoding = unpack_encoding
        self._packer = msgpack.Packer(encoding=pack_encoding.encode("utf-8"))
        self._unpacker = msgpack.Unpacker(encoding=unpack_encoding.encode("utf-8"), use_list=False)
        self._conn = None
        self._chunksize = chunksize
        self._connecting = False
        self._msgid = 0
        self._reading_worker = None
        self._sending_worker = None
        self._conn_worker = gevent.spawn(self._conn_watching)
        self._conn_worker.start()

    def _conn_watching(self):
        while True:
            self.try_open_connection()
            gevent.sleep(1.0)

    def try_open_connection(self):
        if self._connecting or self._conn or (self._sendqueue.empty() and len(self._msgpool) == 0):
            return
        self._connecting = True
        try:
            self._conn = socket.create_connection((self._host, self._port), timeout=self._timeout)
        except Exception as e:
            logging.warning("create connection error. {}".format(e))
            if self._conn:
                self._conn.close()
                self._conn = None
            self._connecting = False
            return
        if not self._conn:
            logging.warning("create connection to ({}:{}) failed.".format(self._host, self._port))
            self._connecting = False
            return
        self._reading_worker = gevent.spawn(self._reading)
        self._sending_worker = gevent.spawn(self._sending)
        self._reading_worker.start()
        self._sending_worker.start()
        self._connecting = False

    def _reading(self):
        while True:
            try:
                data = self._conn.recv(self._chunksize)
            except socket.timeout as t:
                continue
            if not data:
                logging.warning("connection closed")
                self._conn.close()
                self._conn = None
                self._connecting = False
                self._sending_worker.kill()
                gevent.spawn(self.try_open_connection).start()
                return
            self._unpacker.feed(data)
            while True:
                try:
                    rsp = self._unpacker.next()
                except StopIteration as e:
                    break
                self._parse_rsp(rsp)

    def _sending(self):
        while True:
            body = self._sendqueue.get()
            try:
                self._conn.sendall(body)
            except Exception as e:
                logging.warning("RPCClient._sending:error occured. {}".format(e))
                self._conn.close()
                self._conn = None
                self._connecting = False
                self._reading_worker.kill()
                if not self._sendqueue.full():
                    self._sendqueue.put(body)
                gevent.spawn(self.try_open_connection).start()
                return

    def _parse_rsp(self, rsp):
        if type(rsp) != tuple or len(rsp) != 4 or rsp[0] != MSGPACKRPC_RSP:
            raise Exception("invalid msgpack-rpc response")
        (_, msgid, error, result) = rsp
        if msgid not in self._msgpool:
            logging.warn("unexpected msgid. msgid = {}".format(msgid))
            return
        msgsit = self._msgpool[msgid]
        del self._msgpool[msgid]
        msgsit[1] = error
        msgsit[2] = result
        msgsit[0].set()
                    
    def call(self, method, *args):
        if type(method) != str or type(args) != tuple:
            raise Exception("invalid msgpack-rpc request, type(method)={}, type(args)={}".format(type(method), type(args)))
        self._msgid += 1
        msgid = self._msgid
        req = (MSGPACKRPC_REQ, msgid, method, args)
        body = self._packer.pack(req)
        msgsit = [Event(), None, None]
        self._msgpool[msgid] = msgsit
        self._sendqueue.put(body)
        self.try_open_connection()
        r = msgsit[0].wait(timeout=self._timeout)
        if not r:
            raise Exception("msgpack-rpc call timeout after {} seconds, msgid = {}".format(self._timeout, msgid))
        if msgsit[1]:
            raise Exception("msgpack-rpc call rsp error. {}".format(msgsit[1]))
        return msgsit[2]

    def close(self):
        if not self._reading_worker:
            self._conn_worker.kill()
            self._reading_worker.kill()
            self._sending_worker.kill()
            self._conn_worker = None
            self._reading_worker = None
            self._sending_worker = None
        self._conn.close()
        self._conn = None
        self._connecting = False
