#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
@date: 2013-10-16
@author: shell.xu
'''
import os, sys, marshal, logging
from os import path

class ListCache(object):

    def __init__(self, bucket, filepath=None):
        self.bucket = bucket
        if filepath is None:
            filepath = '~/.qbox/cache/%s.db' % bucket.bucket
        self.filepath = path.expanduser(filepath)
        dirname = path.dirname(self.filepath)
        try: os.makedirs(dirname)
        except OSError: pass
        self.load()

    def update(self):
        self.dirmap = {}
        for i in self.bucket.list_prefix('', limit=1000):
            self.dirmap[i['key']] = i
        with open(self.filepath, 'wb') as fo:
            fo.write(marshal.dumps(self.dirmap))
        logging.debug('update successed')

    def load(self):
        self.dirmap = None
        try:
            with open(self.filepath, 'rb') as fi:
                self.dirmap = marshal.loads(fi.read())
            logging.debug('loading successed')
        except OSError: pass

    def list_prefix(self, prefix='', **kw):
        if self.dirmap is None:
            self.update()
        for i in self.dirmap.itervalues():
            if i['key'].startswith(prefix):
                yield i

    def listdir(self, pattern, listfunc=None, **kw):
        if listfunc is None: listfunc = self.list_prefix
        return self.bucket.listdir(pattern, listfunc, **kw)
