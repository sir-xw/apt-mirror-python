#!/usr/bin/env python2
# coding:utf-8

import os
import re
import logging


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

    def get_indexes(self, contents=False):
        index_list = []
        for suite in self.suites:
            index_list += suite.get_indexes(contents)
        return index_list


class SuiteSkel(object):
    def __init__(self, mirror, suite):
        self.mirror = mirror
        self.suite = suite
        self.components = self.mirror.data[suite]
        if len(self.components) == 1 and '' in self.components:
            # simple archive
            self.rel_path = suite
            self.simple = True
        else:
            self.rel_path = 'dists' + '/' + suite
            self.simple = False
        self.url = self.mirror.url + '/' + self.rel_path
        self.skel_path = self.mirror.skel_path + '/' + self.rel_path
        self.sources = []
        self.packages = []
        return

    def compressed_index(self, rel_path):
        COMPRESSIONS = ['.gz', '.bz2', '.xz']
        path = self.rel_path + '/' + rel_path
        # check index file name
        fn = os.path.basename(rel_path)
        if fn == 'Sources':
            self.sources.append(self.skel_path + '/' + rel_path)
        elif fn == 'Packages':
            self.packages.append(self.skel_path + '/' + rel_path)
        return [path] + [path + ext for ext in COMPRESSIONS]

    def get_indexes(self, contents=False):
        index_list = [os.path.join(self.rel_path, fn) for fn in [
            'InRelease', 'Release', 'Release.gpg']]  # Release
        # other index
        for component, arch_list in self.components.items():
            for arch in arch_list:
                if self.simple:
                    if arch == 'src':
                        index_list += self.compressed_index('Sources')
                    else:
                        index_list += self.compressed_index('Packages')
                else:
                    if arch == 'src':
                        rel_dir = component + '/source'
                        index_list.append(
                            self.rel_path + '/' + rel_dir + '/Release')
                        index_list += self.compressed_index(
                            rel_dir + '/Sources')
                    else:
                        rel_dir = component + '/binary-' + arch
                        index_list.append(
                            self.rel_path + '/' + rel_dir + '/Release')
                        index_list += self.compressed_index(
                            rel_dir + '/Packages')
                        index_list.append(
                            self.rel_path + '/' + component + "/i18n/Index")
                        if contents:
                            index_list += self.compressed_index(
                                component + '/Contents-' + arch)
        return index_list

    def find_translation_files_in_release(self, components):
        """Look in the dists/DIST/Release file for the translation files that belong
        to the given component.
        """
        if self.simple:
            return {}

        release_url = self.url + '/Release'
        release_path = self.skel_path + '/Release'
        release_file = open(release_path)

        files = {}
        checksums = 0
        for line in release_file.readlines():
            line = line.rstrip()
            if checksums:
                if re.match(r'^ +(.*)', line):
                    parts = line.split()
                    if len(parts) == 3:
                        _sha1, size, filename = parts
                        if re.match('^(' + '|'.join(components) + r')/i18n/Translation-[^./]*\.bz2', filename):
                            files[os.path.join(
                                self.rel_path, filename)] = int(size)
                    else:
                        logging.warning("Malformed checksum line \"%s\" in %s" %
                                        (line, release_url))
                else:
                    checksums = 0
            if not checksums:
                if line == "SHA256:":
                    checksums = 1
        release_file.close()
        return files

    def find_translation_files_in_index(self):
        # Extract all translation files from the dists/DIST/COMPONENT/i18n/Index
        # file. Fall back to parsing dists/DIST/Release if i18n/Index is not
        # found.
        if self.simple:
            return {}

        files = {}
        not_found = []
        for component in self.components:
            i18n_dir = component + '/i18n'
            base_url = os.path.join(self.url, i18n_dir)
            index_url = os.path.join(base_url, 'Index')
            index_path = os.path.join(self.skel_path, component, 'i18n/Index')
            try:
                index_file = open(index_path)
            except:
                not_found.append(component)
                continue

            checksums = 0
            for line in index_file.readlines():
                line = line.rstrip()
                if checksums:
                    if re.match(r'^ +(.*)', line):
                        parts = line.split()
                        if len(parts) == 3:
                            _checksum, size, filename = parts
                            files[os.path.join(
                                self.rel_path, i18n_dir, filename)] = int(size)
                        else:
                            logging.warn("Malformed checksum line \"%s\" in %s" %
                                         (line, index_url))
                    else:
                        checksums = 0
                if not checksums:
                    if line == "SHA256:" or line == "SHA1:" or line == "MD5Sum:":
                        checksums = 1

            index_file.close()

        if not_found:
            files.update(self.find_translation_files_in_release(
                components=not_found))

        return files

    def find_dep11_files_in_release(self):
        # Look in the dists/DIST/Release file for the DEP-11 files
        if self.simple:
            return {}

        release_url = os.path.join(self.url, 'Release')
        release_path = os.path.join(self.skel_path, 'Release')

        release_file = open(release_path)

        files = {}
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
                                files[os.path.join(self.rel_path, filename)] = int(
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

        return files

    def find_cnf_files_in_release(self):
        # Look in the dists/DIST/Release file for the cnf/Command-* files
        if self.simple:
            return {}

        release_url = os.path.join(self.url, 'Release')
        release_path = os.path.join(self.skel_path, 'Release')

        release_file = open(release_path)

        files = {}
        checksums = 0
        for line in release_file.readlines():
            line = line.rstrip()
            if checksums:
                if re.match(r'^ +(.*)', line):
                    parts = line.split()
                    if len(parts) == 3:
                        _sha1, size, filename = parts
                        for component, arch_list in self.components.items():
                            fn_pattern = r'Commands-(' + \
                                '|'.join(arch_list) + r')\.(gz|bz2|xz)'
                            if re.match(component + r'/cnf/' + fn_pattern, filename):
                                files[os.path.join(self.rel_path, filename)] = int(
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

        return files
