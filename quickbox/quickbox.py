#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
@date: 2013-10-19
@author: shell.xu
'''
import os, sys, getopt, logging
from os import path
from datetime import datetime
import gevent
import qiniu
import ops, listcache

logger = logging.getLogger('qbox')

def update(args, optdict):
    if not args: logging.warning('buckets not exist')
    for bucket in args:
        listcache.ListCache(
            qiniu.Bucket(bucket)).update()
update.opt = ''

def ls(args, optdict):
    import prettytable
    if not args: logging.warning('buckets not exist')
    for p in args:
        bucket, key = p.split(':', 1)
        b = qiniu.Bucket(bucket)
        x = prettytable.PrettyTable(['attr', 'size', 'time', 'key'])
        x.border = False
        x.align = 'l'
        dirset = {}
        if not key or key.endswith('/'): key += '*'
        if '-c' in optdict:
            # FIXME: repeat load cache?
            b = listcache.ListCache(b)
        if '-r' in optdict:
            for i in b.listdir(key):
                t = datetime.fromtimestamp(i['putTime'] / 10000000)
                x.add_row([
                        '', i['fsize'],
                        t.strftime("%Y-%m-%d %H:%M:%S"),
                        i['key'][len(key):]])
        else:
            files, dirs = ops.listdir(b, key)
            for f in files: x.add_row(('', f[0], f[1], f[2]))
            for f in dirs: x.add_row(('d', f[0], f[1], f[2]))
        print x
ls.opt = 'cr'

def rm(args, optdict):
    for p in args:
        if ':' not in p:
            logger.error('rm only work in remote')
            continue
        if p.endswith('/'):
            if '-r' not in optdict:
                logger.error('rm %s without -r, no action take.' % p)
                continue
            bucket = p.split(':', 1)[0]
            b = qiniu.Bucket(bucket)
            keys = [f.split(':', 1)[1] for f in ops.walk(p)]
            b.removes(keys)
            for k in keys: print 'delete:', k
        else:
            bucket, key = p.split(':', 1)
            b = qiniu.Bucket(bucket)
            ops.delete(b, key)
rm.opt = 'r'

def cp(args, optdict):
    dst = args.pop(-1)
    for src in args:
        if src.endswith('/'):
            if '-r' not in optdict:
                logger.error('copy %s without -r, no action take.' % src)
                continue
            if not dst.endswith('/'):
                logger.error('copy to %s is not dir.' % dst)
                return
            # ??
            grs = []
            for f in ops.walk(src):
                grs.append(ops.pool.spawn(ops.copy, f, path.join(dst, f[len(src):])))
            gevent.joinall(grs)
        else: ops.copy(src, dst)
cp.opt = 'r'

# def mv(args, optdict):
#     dst = args.pop(-1)
#     for src in args:
#         copy(src, dst)

def cat(args, optdict):
    for p in args:
        if ':' not in p:
            logger.error('cat only work in remote')
            continue
        ops.copy(p, '-')
cat.opt = ''

def write(args, optdict):
    if len(args) > 1:
        raise Exception("can't work with more then one file")
    if ':' not in args[0]:
        raise Exception('write only work in remote')
    ops.copy('-', args[0])
write.opt = ''

def rsync(args, optdict):
    pass

def file(args, optdict):
    for p in args:
        bucket, key = p.split(':', 1)
        b = qiniu.Bucket(bucket)
        print 'key:', key
        for k, v in b.getstat(key).iteritems():
            print '    %s:\t%s' % (k, v)
file.opt = ''

def checksum(args, optdict):
    for p in args:
        if ':' not in p:
            logger.error('checksum only work in remote')
            continue
        if p.endswith('/'):
            if '-r' not in optdict:
                logger.error('check %s without -r, no action take.' % p)
                continue
            bucket = p.split(':', 1)[0]
            b = qiniu.Bucket(bucket)
            for f in ops.walk(p):
                key = f.split(':', 1)[1]
                print key
                i = b.getstat(key)
                print key, i['hash']
        else:
            bucket, key = p.split(':', 1)
            b = qiniu.Bucket(bucket)
            i = b.getstat(key)
            print key, i['hash']
checksum.opt = 'r'

def ssh(args, optdict):
    pass

def cmd_not_found(args, optdict):
    print 'command not found'
cmd_not_found.opt = ''

def main(args):
    cmd = args.pop(0)
    f = globals().get(cmd, cmd_not_found)
    if hasattr(f, 'lopt'):
        optlist, args = getopt.getopt(args, f.opt + 'd', f.lopt)
    else: optlist, args = getopt.getopt(args, f.opt + 'd')
    optdict = dict(optlist)
    logging.basicConfig(
        level=logging.DEBUG if '-d' in optdict else logging.INFO)
    f(args, optdict)
