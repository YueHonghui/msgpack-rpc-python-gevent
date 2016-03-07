# -*- coding: utf-8 -*-
from gevent.server import StreamServer
import gevent
from endpoint import *
from rpchandle import *
import logging

gdispatcher = RPCHandle("server")

def handle(socket, addr):
    print("new msgpack endpoint from {}".format(addr))
    router = RpcRouter()
    router.route_call(gdispatcher.echo)
    print(router.get_calls())
    ep = MsgpackEndpoint(MODEBOTH, socket, router)
    gevent.spawn(callloop, ep).start()

def callloop(ep):
    gevent.sleep(10.0)
    while True:
        print(ep.call("echo", "loop from server"))
        gevent.sleep(3.0)

if __name__ == "__main__":
    server = StreamServer(('0.0.0.0', 11000), handle)
    server.serve_forever()
