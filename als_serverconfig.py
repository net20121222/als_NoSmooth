#!/usr/bin/python
#coding:utf-8
import os, sys
import configuration

from configuration import Boolean, Integer, String, StringList
class AlsServerConfig(configuration.Config):
    HANDLEFILEPATH = String("/home", "handle file path")
    HANDLEFILEPATH.comment_out_default = False
    RESULTFILEPATH = String("/home", "result file path")
    RESULTFILEPATH.comment_out_default = False
    HANDLELOGPATH = String("/home", "log path")
    HANDLELOGPATH.comment_out_default = False
    IPTABLE = String("192.168.1.1;", "ip list string")
    IPTABLE.comment_out_default = False
    THREAD_NUM = Integer(4, "worker num")
    THREAD_NUM.comment_out_default = False
    VERSION = Integer(0, "Version of the config file")
    VERSION.comment_out_default = False

    def __init__(self, path):
        configuration.Config.__init__(self)
        self.dir = os.path.dirname(path)
        if os.path.exists(path):
            self.load(path)
            if self.VERSION != self.__class__.VERSION.default:
                self.VERSION = self.__class__.VERSION.default
                self.save(path)
        else:
            self.save(path)
        self.path = path

    def save(self, path=None):
        configuration.Config.save(self, path or self.path)

