# -*- coding: utf-8 -*-
import msgpack
import gevent
from gevent import socket
from gevent.event import Event
from gevent.queue import Queue
import logging

MSGPACKRPC_REQ = 0
MSGPACKRPC_RSP = 1
MSGPACKRPC_NOTIFY = 2

MODECLIENT = 1
MODESERVER = 2
MODEBOTH = 3

class IConnErrorHandle(object):
    def on_conn_error(self, conn, ep, exception):
        raise Exception("not implemented!")

class ConnErrorReconnect(IConnErrorHandle):
    def __init__(self, addr=None, interval=2.0, timeout=5.0):
        self.addr = addr
        self.interval = interval
        self.timeout = timeout

    def on_conn_error(self, conn, ep, exception):
        logging.warn("conn closed. error={}".format(exception))
        addr = self.addr
        if not self.addr:
            addr = conn.getpeername()
        conn.close()
        while True:
            try:
                conn = socket.create_connection(addr, timeout=self.timeout)
            except Exception as e:
                logging.warn("try to reconnect to {} failed. error={}".format(addr, e))
                gevent.sleep(self.interval)
                continue
            while not ep.attach_conn(conn):
                gevent.sleep(1.0)
            return

class RpcRouter(object):
    def __init__(self):
        self._notify_funcs = {}
        self._call_funcs = {}

    def route_notify(self, func, name=None):
        if name == None:
            name = func.__name__
        self._notify_funcs[name] = func

    def route_call(self, func, name=None):
        if name == None:
            name = func.__name__
        self._call_funcs[name] = func

    def get_notify(self, name):
        return self._notify_funcs.get(name)

    def get_call(self, name):
        return self._call_funcs.get(name)

    def get_calls(self):
        return self._call_funcs.keys()

    def get_notifies(self):
        return self._notify_funcs.keys()

class MsgpackEndpoint(object):
    def __init__(self, mode, conn, conn_error_handle, router=None, timeout=5.0, poolsize=10, chunksize=1024*32, pack_encoding='utf-8', unpack_encoding='utf-8'):
        self._router = router
        self._conn_error_handle = conn_error_handle
        self._mode = mode
        self._timeout = timeout
        self._poolsize = poolsize
        self._sendqueue = Queue(maxsize=poolsize)
        self._msgpool = dict()
        self._pack_encoding = pack_encoding
        self._unpack_encoding = unpack_encoding
        self._packer = msgpack.Packer(use_bin_type=True, encoding=pack_encoding.encode("utf-8"))
        self._unpacker = msgpack.Unpacker(encoding=unpack_encoding.encode("utf-8"))
        self._conn = conn
        self._chunksize = chunksize
        self._connecting = False
        self._msgid = 0
        self._reading_worker = gevent.spawn(self._reading)
        self._sending_worker = gevent.spawn(self._sending)
        self._reading_worker.start()
        self._sending_worker.start()

    def _reading(self):
        while True:
            if not self._conn:
                gevent.sleep(1.0)
                continue
            try:
                data = self._conn.recv(self._chunksize)
            except socket.timeout as t:
                continue
            if not data:
                logging.warning("connection closed")
                tmpconn = self._conn
                self._conn = None
                self._conn_error_handle.on_conn_error(tmpconn, self, Exception("connection closed"))
                continue
            self._unpacker.feed(data)
            while True:
                try:
                    msg = self._unpacker.next()
                except StopIteration as e:
                    break
                self._parse_msg(msg)

    def _sending(self):
        while True:
            if not self._conn:
                gevent.sleep(1.0)
                continue
            body = self._sendqueue.get()
            try:
                self._conn.sendall(body)
            except Exception as e:
                logging.warning("RPCClient._sending:error occured. {}".format(e))
                tmpconn = self._conn
                self._conn = None
                if not self._sendqueue.full():
                    self._sendqueue.put(body)
                self._conn_error_handle.on_conn_error(tmpconn, self, e)
                continue

    def _parse_msg(self, msg):
        if (type(msg) != list and type(msg) != tuple) or len(msg) < 3:
            logging.warn("invalid msgpack-rpc msg. type={}, msg={}".format(type(msg), msg))
            tmpconn = self._conn
            self._conn = None
            self._conn_error_handle.on_conn_error(tmpconn, self, Exception("invalid msgpack-rpc msg"))
            return
        if msg[0] == MSGPACKRPC_RSP and len(msg) == 4 and self._mode & MODECLIENT:
            (_, msgid, error, result) = msg
            if msgid not in self._msgpool:
                logging.warn("unexpected msgid. msgid = {}".format(msgid))
                return
            msgsit = self._msgpool[msgid]
            del self._msgpool[msgid]
            msgsit[1] = error
            msgsit[2] = result
            msgsit[0].set()
        elif msg[0] == MSGPACKRPC_REQ and len(msg) == 4 and self._mode & MODESERVER:
            (_, msgid, method, params) = msg
            func = self._router.get_call(method)
            result = None
            if not func:
                rsp = (MSGPACKRPC_RSP, msgid, "Method not found: {}".format(method), None)
                self._sendqueue.put(self._packer.pack(rsp))
                return
            if not hasattr(func, '__call__'):
                rsp = (MSGPACKRPC_RSP, msgid, "Method is not callable: {}".format(method), None)
                self._sendqueue.put(self._packer.pack(rsp))
                return
            try:
                result = func(*params)
            except Exception as e:
                rsp = (MSGPACKRPC_RSP, msgid, "{}".format(e), None)
                self._sendqueue.put(self._packer.pack(rsp))
                return
            rsp = (MSGPACKRPC_RSP, msgid, None, result)
            self._sendqueue.put(self._packer.pack(rsp))
        elif msg[0] == MSGPACKRPC_NOTIFY and len(msg) == 3 and self._mode & MODESERVER:
            (_, method, params) = msg
            func = self._router.get_notify(method)
            if not func:
                logging.warn("Method not found: {}".format(method))
                return
            if not hasattr(func, '__call__'):
                logging.warn("Method is not callable: {}".format(method))
                return
            try:
                func(*params)
            except Exception as e:
                logging.warn("Exception: {} in notify {}".format(e, method))
                return
        else:
            logging.warn("invalid msgpack-rpc msg {}".format(msg))
            tmpconn = self._conn
            self._conn = None
            self._conn_error_handle.on_conn_error(tmpconn, self, Exception("invalid msgpack-rpc msg"))
            return

    def attach_conn(self, conn):
        if self._conn:
            return False
        self._conn = conn
        return True
                    
    def call(self, method, *args):
        if not self._conn:
            logging.warn("rpc connection closed")
        if type(method) != str or type(args) != tuple:
            raise Exception("invalid msgpack-rpc request, type(method)={}, type(args)={}".format(type(method), type(args)))
        self._msgid += 1
        msgid = self._msgid
        req = (MSGPACKRPC_REQ, msgid, method, args)
        body = self._packer.pack(req)
        msgsit = [Event(), None, None]
        self._msgpool[msgid] = msgsit
        self._sendqueue.put(body)
        r = msgsit[0].wait(timeout=self._timeout)
        if not r:
            raise Exception("msgpack-rpc call timeout after {} seconds, msgid = {}".format(self._timeout, msgid))
        if msgsit[1]:
            raise Exception("msgpack-rpc call rsp error. {}".format(msgsit[1]))
        return msgsit[2]

    def notify(self, method, *args):
        if not self._conn:
            logging.warn("rpc connection closed")
        if type(method) != str or type(args) != tuple:
            raise Exception("invalid msgpack-rpc request, type(method)={}, type(args)={}".format(type(method), type(args)))
        notify = (MSGPACKRPC_NOTIFY, method, args)
        self._sendqueue.put(self._packer.pack(notify))

    def close(self):
        if self._reading_worker:
            self._reading_worker.kill()
            self._reading_worker = None
        if self._sending_worker:
            self._sending_worker.kill()
            self._sending_worker = None
        if self._conn:
            self._conn = None
