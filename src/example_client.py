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
    router = RpcRouter()
    router.route_call(sh.echo)
    print(router.get_calls())
    s = socket.create_connection(("127.0.0.1", 11000), timeout=10)
    connerror = ConnErrorReconnect()
    ep = MsgpackEndpoint(MODEBOTH, s, connerror, router)
    gevent.spawn(timer_call, ep).start()
    gevent.sleep(100000.0)
