# -*- coding: utf-8 -*-

from endpoint import *
import gevent
from gevent import socket
from rpchandle import *

def timer_call(ep):
    while True:
        print(ep.call("echo", "call from client"))
        gevent.sleep(3.0)

if __name__ == "__main__":
    sh = RPCHandle("client")
    s = socket.create_connection(("127.0.0.1", 10000), timeout=10)
    ep = MsgpackEndpoint(MODEBOTH, s, sh)
    gevent.spawn(timer_call, ep).start()
    gevent.sleep(100000.0)
