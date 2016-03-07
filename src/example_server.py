# -*- coding: utf-8 -*-
from gevent.server import StreamServer
import gevent
from endpoint import MsgpackEndpoint
import endpoint
from rpchandle import *
import logging

gdispatcher = RPCHandle("server")

def handle(socket, addr):
    print("new msgpack endpoint from {}".format(addr))
    ep = MsgpackEndpoint(endpoint.MODEBOTH, socket, dispatcher=gdispatcher)
    gevent.spawn(callloop, ep).start()

def callloop(ep):
    gevent.sleep(10.0)
    while True:
        print(ep.call("echo", "loop from server"))
        gevent.sleep(3.0)

if __name__ == "__main__":
    server = StreamServer(('0.0.0.0', 10000), handle)
    server.serve_forever()
