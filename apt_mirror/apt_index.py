#!/usr/bin/env python2
# coding:utf-8

import os
import re
import logging
from .utils import remove_double_slashes, sanitise_uri


class MirrorSkel(object):
    """
    apt archive mirror skel
    """

    def __init__(self, url, skel_path, data):
        self.url = url
        self.skel_path = skel_path
        self.data = data
        self.suites = []
        for suite in self.data:
            self.suites.append(SuiteSkel(mirror=self, suite=suite))
        return

    def check_md5(self):
        # TODO:
        return

    def fix(self, filename, retry=0):
        # TODO:
        return

    def get_index_urls(self, contents=False):
        index_urls = []
        for suite in self.suites:
            index_urls += suite.get_index_urls(contents)
        return index_urls


class SuiteSkel(object):
    def __init__(self, mirror, suite):
        self.mirror = mirror
        self.suite = suite
        self.components = self.mirror.data[suite]
        if len(self.components) == 1 and '' in self.components:
            # simple archive
            rel_path = suite
            self.simple = True
        else:
            rel_path = 'dists' + '/' + suite
            self.simple = False
        self.url = self.mirror.url + '/' + rel_path
        self.skel_path = self.mirror.skel_path + '/' + rel_path
        self.sources = []
        self.packages = []
        return

    def compressed_index(self, rel_path):
        COMPRESSIONS = ['.gz', '.bz2', '.xz']
        url = self.url + '/' + rel_path
        # check index file name
        fn = os.path.basename(rel_path)
        if fn == 'Sources':
            self.sources.append(self.skel_path + '/' + rel_path)
        elif fn == 'Packages':
            self.packages.append(self.skel_path + '/' + rel_path)
        return [url] + [url + ext for ext in COMPRESSIONS]

    def get_index_urls(self, contents=False):
        index_urls = [
            self.url + '/' + fn for fn in ('InRelease', 'Release', 'Release.gpg')]  # Release
        # other index
        for component, arch_list in self.components.items():
            for arch in arch_list:
                if self.simple:
                    if arch == 'src':
                        index_urls += self.compressed_index('Sources')
                    else:
                        index_urls += self.compressed_index('Packages')
                else:
                    if arch == 'src':
                        rel_dir = component + '/source'
                        index_urls.append(
                            self.url + '/' + rel_dir + '/Release')
                        index_urls += self.compressed_index(
                            rel_dir + '/Sources')
                    else:
                        rel_dir = component + '/binary-' + arch
                        index_urls.append(
                            self.url + '/' + rel_dir + '/Release')
                        index_urls += self.compressed_index(
                            rel_dir + '/Packages')
                        index_urls.append(
                            self.url + '/' + component + "/i18n/Index")
                        if contents:
                            index_urls += self.compressed_index(
                                component + '/Contents-' + arch)
        return index_urls

    def find_translation_files_in_release(self):
        """Look in the dists/DIST/Release file for the translation files that belong
        to the given component.
        """
        if self.simple:
            return {}

        release_url = self.url + '/Release'
        release_path = self.skel_path + '/Release'
        release_file = open(release_path)

        urls = {}
        checksums = 0
        for line in release_file.readlines():
            line = line.rstrip()
            if checksums:
                if re.match(r'^ +(.*)', line):
                    parts = line.split()
                    if len(parts) == 3:
                        _sha1, size, filename = parts
                        if re.match('^(' + '|'.join(self.components) + r')/i18n/Translation-[^./]*\.bz2', filename):
                            urls[os.path.join(self.url, filename)] = int(size)
                    else:
                        logging.warning("Malformed checksum line \"%s\" in %s" %
                                        (line, release_url))
                else:
                    checksums = 0
            if not checksums:
                if line == "SHA256:":
                    checksums = 1
        release_file.close()
        return urls

    def find_translation_files_in_index(self):
        # Extract all translation files from the dists/DIST/COMPONENT/i18n/Index
        # file. Fall back to parsing dists/DIST/Release if i18n/Index is not
        # found.
        if self.simple:
            return {}

        urls = {}
        for component in self.components:
            base_url = os.path.join(self.url, component, 'i18n')
            index_url = os.path.join(base_url, 'Index')
            index_path = os.path.join(self.skel_path, component, 'i18n/Index')
            try:
                index_file = open(index_path)
            except:
                return self.find_translation_files_in_release()

            checksums = 0
            for line in index_file.readlines():
                line = line.rstrip()
                if checksums:
                    if re.match(r'^ +(.*)', line):
                        parts = line.split()
                        if len(parts) == 3:
                            _checksum, size, filename = parts
                            urls[os.path.join(base_url, filename)] = int(size)
                        else:
                            logging.warn("Malformed checksum line \"%s\" in %s" %
                                         (line, index_url))
                    else:
                        checksums = 0
                if not checksums:
                    if line == "SHA256:" or line == "SHA1:" or line == "MD5Sum:":
                        checksums = 1

            index_file.close()

        return urls

    def find_dep11_files_in_release(self):
        # Look in the dists/DIST/Release file for the DEP-11 files
        if self.simple:
            return {}

        release_url = os.path.join(self.url, 'Release')
        release_path = os.path.join(self.skel_path, 'Release')

        release_file = open(release_path)

        urls = {}
        checksums = 0
        for line in release_file.readlines():
            line = line.rstrip()
            if checksums:
                if re.match(r'^ +(.*)', line):
                    parts = line.split()
                    if len(parts) == 3:
                        _sha1, size, filename = parts
                        for component, arch_list in self.components.items():
                            fn_pattern = r'(Components-(' + '|'.join(arch_list) + \
                                r')\.yml|icons-[^./]+\.tar)\.(gz|bz2|xz)'
                            if re.match(component + r'/dep11/' + fn_pattern, filename):
                                urls[os.path.join(self.url, filename)] = int(
                                    size)
                                break
                    else:
                        logging.warn("Malformed checksum line \"%s\" in %s" %
                                     (line, release_url))
                else:
                    checksums = 0
            if not checksums:
                if line == "SHA256:":
                    checksums = 1

        return urls
