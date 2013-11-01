#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
@date: 2013-10-18
@author: shell.xu
'''
import os, sys, logging
from os import path
from datetime import datetime
import gevent, gevent.pool, gevent.monkey
import qiniu

gevent.monkey.patch_socket()

logger = logging.getLogger('ops')

pool = gevent.pool.Pool(qiniu.conf.getcfg('main', 'max_concurrency', int, 10))

def listdir(l, pattern=''):
    files, dirs, dirset = [], [], {}
    try:
        index = min(
            filter(
                lambda i: i != -1,
                (pattern.find(c) for c in ('.', '*', '[', ']'))))
    except ValueError: index = -1
    prefix = pattern if index == -1 else pattern[:index]
    for i in l.listdir(pattern, limit=1000):
        rkey = i['key'][len(prefix):].lstrip('/')
        if '/' in rkey:
            d = rkey.split('/', 1)[0]
            dirset.setdefault(d, [0, 0])
            dirset[d][0] += 1
            if dirset[d][1] < i['putTime']:
                dirset[d][1] = i['putTime']
        else:
            t = datetime.fromtimestamp(i['putTime'] / 10000000)
            files.append([
                i['fsize'], t.strftime("%Y-%m-%d %H:%M:%S"), rkey])
    for d, v in dirset.iteritems():
        t = datetime.fromtimestamp(v[1] / 10000000)
        dirs.append([v[0], t, d])
    return files, dirs

def openlocal(p, m):
    if p == '-':
        if 'w' in m: return sys.stdout
        elif 'r' in m: return sys.stdin
        else: raise Exception('std without mode')
    p = path.expanduser(p)
    return open(p, m)

def walk(p):
    if ':' in p:
        bucket, key = p.split(':', 1)
        b = qiniu.Bucket(bucket)
        for i in b.list_prefix(key):
            yield '%s:%s' % (bucket, i['key'])
    else:
        for dir, dirs, files in os.walk(p):
            for f in files:
                yield path.join(dir, f)

def delete(b, key):
    print 'delete:', key
    b.remove(key)

# TODO: src is prefix?
# TODO: dst is dir?
def copy(src, dst):
    print 'copy from %s to %s' % (src, dst)
    qiniusrc = ':' in src
    qiniudst = ':' in dst

    if dst.endswith('/'):
        dst = path.join(dst, path.basename(src))

    if qiniusrc and not qiniudst:
        bucket, key = src.split(':', 1)
        b = qiniu.Bucket(bucket, '%s.u.qiniudn.com' % bucket)
        dst = path.expanduser(dst)
        basedir = path.dirname(dst)
        if not path.exists(basedir):
            logger.debug('create directory %s' % basedir)
            os.makedirs(basedir)
        with openlocal(dst, 'wb') as fo:
            fo.write(b.url(key).get().read())

    elif not qiniusrc and qiniudst:
        bucket, key = dst.split(':', 1)
        b = qiniu.Bucket(bucket, '%s.u.qiniudn.com' % bucket)
        dst = path.expanduser(dst)
        with openlocal(src, 'rb') as fi:
            b.put(key, fi)

    elif qiniusrc and qiniudst:
        bucket, key_src = src.split(':', 1)
        bucket_dst, key_dst = dst.split(':', 1)
        b = qiniu.Bucket(bucket, '%s.u.qiniudn.com' % bucket)
        b.copy(key_src, key_dst, bucket_dst)

    else: raise Exception('normal copy?')

# def move(src, dst):
#     qiniusrc = ':' in src
#     qiniudst = ':' in dst
#     if qiniusrc and not qiniudst:
#         bucket, key = src.split(':', 1)
#         b = qiniu.Bucket(bucket, '%s.u.qiniudn.com' % bucket)
#         with openlocal(dst, 'wb') as fo:
#             fo.write(b.url(key).get().read())
#         b.remove(key)
#     elif not qiniusrc and qiniudst:
#         bucket, key = dst.split(':', 1)
#         b = qiniu.Bucket(bucket, '%s.u.qiniudn.com' % bucket)
#         with openlocal(src, 'rb') as fi:
#             b.put(key, fi)
#         os.remove(path.expanduser(src))
#     elif qiniusrc and qiniudst:
#         bucket, key_src = src.split(':', 1)
#         bucket_dst, key_dst = dst.split(':', 1)
#         b = qiniu.Bucket(bucket, '%s.u.qiniudn.com' % bucket)
#         b.rename(key_src, key_dst, bucket_dst)
#     else: raise Exception('normal copy?')
