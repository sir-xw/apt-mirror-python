#!/usr/bin/env python
# coding:utf-8

import os
import re

CONFIG_VAR_PATTERN = re.compile(
    r'set[\t ]+(?P<key>[^\s]+)[\t ]+(?P<value>"[^"]+"|\'[^\']+\'|[^\s]+)')
CONFIG_MIRROR_PATTERN = re.compile(r"""
    ^[\t ]*
    (?P<type>deb-src|deb)
    (?:-(?P<arch>[\w\-]+))?
    [\t ]+
    (?:\[(?P<options>[^\]]+)\][\t ]+)?
    (?P<uri>[^\s]+)
    [\t ]+
    (?P<components>.+)$
    """, re.X)
CONFIG_CLEAN_PATTERN = re.compile(
    r'(?P<type>clean|skip-clean)[\t ]+(?P<uri>[^\s]+)')


def parse_config_line(line):
    config = {'type':''}  # for bad config line
    match = CONFIG_MIRROR_PATTERN.match(line)
    if match:
        config = match.groupdict()
        if config['options'] == None:
            config['options'] = ''
        arch_option_match = re.match(
            r'arch=((?P<arch>[\w\-]+)[,]*)', config['options'])
        if arch_option_match:
            config['arch'] = arch_option_match.groupdict()['arch']
        config['components'] = config['components'].split()
    else:
        match = CONFIG_VAR_PATTERN.match(line)
        if match:
            config = match.groupdict()
            config['type'] = 'set'
            config['value'] = re.sub(r"^'(.*)'", r'\g<1>', config['value'])
            config['value'] = re.sub(r'^"(.*)"', r'\g<1>', config['value'])
        else:
            match = CONFIG_CLEAN_PATTERN.match(line)
            if match:
                config = match.groupdict()

    return config


class MirrorConfig(object):
    def __init__(self, config_file=''):
        default_arch = os.popen('dpkg --print-architecture').read().strip()
        self.vars = {"defaultarch": default_arch or 'i386',
                     "nthreads": '20',
                     "use_queue": '0',
                     "base_path": '/var/spool/apt-mirror',
                     "mirror_path": '$base_path/mirror',
                     "skel_path": '$base_path/skel',
                     "var_path": '$base_path/var',
                     "cleanscript": '$var_path/clean.sh',
                     "_contents": '1',
                     "_autoclean": '0',
                     "_tilde": '0',
                     "limit_rate": '100m',
                     "run_postmirror": '1',
                     "auth_no_challenge": '0',
                     "no_check_certificate": '0',
                     "unlink": '0',
                     "postmirror_script": '$var_path/postmirror.sh',
                     "use_proxy": 'off',
                     "http_proxy": '',
                     "https_proxy": '',
                     "proxy_user": '',
                     "proxy_password": ''}
        self.mirrors = {}
        self.skipclean = {}
        self.clean_directory = {}
        if config_file:
            self.read(config_file)
        return

    def get_variable(self, key):
        value = self.vars[key]
        count = 16
        while 1:
            refs = re.findall(r'\$(\w+)', value)
            if refs:
                for ref in refs:
                    value = value.replace('$' + ref, self.vars[ref])
                count -= 1
                if count < 0:
                    raise Exception(
                        'apt-mirror: too many substitution while evaluating variable')
            else:
                break
        # int variables
        if key in ['nthreads', 'use_queue', '_contents', '_autoclean', '_tilde',
                   'run_postmirror', 'auth_no_challenge',
                   'no_check_certificate', 'unlink']:
            try:
                return int(value)
            except:
                pass

        return value

    def __getattribute__(self, attr):
        try:
            return object.__getattribute__(self, attr)
        except:
            return self.get_variable(attr)

    def read(self, config_file):
        cf = open(config_file)
        line_number = 0
        for line in cf.readlines():
            line_number += 1
            if re.match(r'^\s*#', line):
                continue
            if not re.match(r'\S', line):
                continue
            config_line = parse_config_line(line)

            if config_line['type'] == "set":
                self.vars[config_line['key']] = config_line['value']
                continue
            elif config_line['type'] == "deb":
                arch = config_line['arch'] or self.defaultarch
                components = config_line['components']
                base_url = config_line['uri']
                suite = components[0]
                components = components[1:] or ['']
                if base_url not in self.mirrors:
                    self.mirrors[base_url] = {
                        suite: {c: set([arch]) for c in components}}
                else:
                    mirror_data = self.mirrors[base_url]
                    if suite not in mirror_data:
                        mirror_data[suite] = {
                            c: set([arch]) for c in components}
                    else:
                        suite_data = mirror_data[suite]
                        for c in components:
                            if c not in suite_data:
                                suite_data[c] = set([arch])
                            else:
                                suite_data[c].add(arch)
                continue
            elif config_line['type'] == "deb-src":
                components = config_line['components']
                arch = 'src'
                base_url = config_line['uri']
                suite = components[0]
                components = components[1:] or ['']
                if base_url not in self.mirrors:
                    self.mirrors[base_url] = {
                        suite: {c: set([arch]) for c in components}}
                else:
                    mirror_data = self.mirrors[base_url]
                    if suite not in mirror_data:
                        mirror_data[suite] = {
                            c: set([arch]) for c in components}
                    else:
                        suite_data = mirror_data[suite]
                        for c in components:
                            if c not in suite_data:
                                suite_data[c] = set([arch])
                            else:
                                suite_data[c].add(arch)
                continue
            elif config_line['type'] in ['skip-clean', 'clean']:
                link = config_line['uri']
                link = link.split('://', 1)[1].rstrip('/')
                if self._tilde:
                    link = link.replace('~', '%7E')
                if config_line['type'] == "skip-clean":
                    self.skipclean[link] = 1
                elif config_line['type'] == "clean":
                    self.clean_directory[link] = 1
                continue

            raise Exception(
                "apt-mirror: invalid line in config file (%d: %s ...)" % (line_number, line))

        if not self.defaultarch:
            raise Exception(
                "Please explicitly specify 'defaultarch' in mirror.list")
