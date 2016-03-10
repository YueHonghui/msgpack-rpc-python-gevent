# -*- coding: utf-8 -*-
from gevent.server import StreamServer
import gevent
from endpoint import *
import logging

class ExampleServerHandle(IConnErrorHandle):
    def __init__(self):
        self.callloop = None

    def echo(self, hello):
        return "echo from server: {}".format(hello)

    def on_conn_error(self, conn, ep, exception):
        print("conn closed. error={}".format(exception))
        conn.close()
        if self.callloop:
            self.callloop.close()
        ep.close()

def handle(socket, addr):
    print("new msgpack endpoint from {}".format(addr))
    router = RpcRouter()
    handler = ExampleServerHandle()
    router.route_call(handler.echo)
    print(router.get_calls())
    ep = MsgpackEndpoint(MODEBOTH, socket, handler, router)
    callinst = gevent.spawn(callloop, ep)
    handler.callloop = callinst
    callinst.start()

def callloop(ep):
    gevent.sleep(10.0)
    while True:
        print(ep.call("echo", "loop from server"))
        gevent.sleep(3.0)

if __name__ == "__main__":
    server = StreamServer(('0.0.0.0', 11000), handle)
    server.serve_forever()
