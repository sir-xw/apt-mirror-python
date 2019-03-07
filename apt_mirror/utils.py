#!/usr/bin/env python2
# coding:utf-8

import os
import re
import logging

TILDE = False

def sanitise_uri(uri):
    uri = uri.split('://')[-1]
    if uri.find('@') >= 0:
        uri = uri.split('@')[-1]
    # and port information
    uri = re.sub(r'\:\d+','',uri)
    if TILDE:
        uri = uri.replace('~', '%7E')
    return uri

def quoted_path(path):
    path = path.replace("'", "\\'")
    return "'" + path + "'"

def round_number(n):
    return round(n, 1)


def format_bytes(bytes):
    size_name = 'bytes'
    KiB = 1024
    MiB = 1024 * 1024
    GiB = 1024 * 1024 * 1024

    if bytes >= GiB:
        bytes_out = float(bytes) / GiB
        size_name = 'GiB'
    elif bytes >= MiB:
        bytes_out = float(bytes) / MiB
        size_name = 'MiB'
    elif bytes >= KiB:
        bytes_out = float(bytes) / KiB
        size_name = 'KiB'
    else:
        bytes_out = bytes
        size_name = 'bytes'

    bytes_out = round_number(bytes_out)

    return str(bytes_out) + ' ' + size_name

def remove_double_slashes(string):
    while 1:
        string, match = re.subn(r'/\./', '/', string)
        if not match:
            break
    while 1:
        string, match = re.subn(r'(?<!:)//', '/', string)
        if not match:
            break
    while 1:
        string, match = re.subn(r'(?<!:/)/[^/]+/\.\./', '/', string)
        if not match:
            break

    if TILDE:
        string = string.replace('~', '%7E')
    return string

def remove_spaces(hashref):
    for key in hashref:
        hashref[key] = hashref[key].lstrip(' ')


def copy_file(source, target, unlink=0):
    todir = os.path.dirname(target)
    if not os.path.exists(source):
        return
    if not os.path.isdir(todir):
        os.makedirs(todir)
    if unlink == 1 and os.path.exists(target):
        if os.system("diff -q '%s' '%s' > /dev/null" % (source, target)) != 0:
            os.unlink(target)

    try:
        os.system('cp "%s" "%s"' % (source, target))
    except:
        logging.warn("apt-mirror: can't copy %s to %s" % (source, target))
        return
    source_stat = os.stat(source)
    os.utime(target, (source_stat.st_atime, source_stat.st_mtime))