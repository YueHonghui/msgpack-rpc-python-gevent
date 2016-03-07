# -*- coding: utf-8 -*-

class RPCHandle(object):
    def __init__(self, name):
        self._name = name

    def echo(self, hello):
        print("{}:receive rpc call echo {}".format(self._name, hello))
        return "hi from {}".format(self._name)

    def sum(self, a, b):
        print("{}:receive rpc call sum {}, {}".format(self._name, a, b))
        return a+b
