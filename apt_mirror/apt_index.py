#!/usr/bin/env python2
# coding:utf-8

import os
import re
import logging
from .utils import remove_double_slashes, sanitise_uri


class AptIndex(object):
    """
    index files of apt archive
    """

    def __init__(self, uri, config):
        self.uri = remove_double_slashes(uri, config)
        self.config = config

    def find_translation_files_in_release(self, component):
        """Look in the dists/DIST/Release file for the translation files that belong
        to the given component.
        """
        release_uri = os.path.join(self.uri, "Release")
        release_path = os.path.join(self.config.skel_path,
                                    sanitise_uri(release_uri, self.config))

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
                        if re.match('^' + component + r'/i18n/Translation-[^./]*\.bz2', filename):
                            urls[os.path.join(self.uri, filename)] = int(size)
                    else:
                        logging.warning("Malformed checksum line \"%s\" in %s" %
                                        (line, release_uri))
                else:
                    checksums = 0
            if not checksums:
                if line == "SHA256:":
                    checksums = 1
        release_file.close()
        return urls

    def find_translation_files_in_index(self, component):
        # Extract all translation files from the dists/DIST/COMPONENT/i18n/Index
        # file. Fall back to parsing dists/DIST/Release if i18n/Index is not
        # found.

        dist_uri = remove_double_slashes(self.uri, self.config)

        base_uri = os.path.join(dist_uri ,component, 'i18n')
        index_uri = os.path.join(base_uri , "Index")
        index_path = os.path.join(self.config.skel_path,
                                  sanitise_uri(index_uri, self.config))
        try:
            index_file = open(index_path)
        except:
            return self.find_translation_files_in_release(component)

        urls = {}
        checksums = 0
        for line in index_file.readlines():
            line = line.rstrip()
            if checksums:
                if re.match(r'^ +(.*)', line):
                    parts = line.split()
                    if len(parts) == 3:
                        _checksum, size, filename = parts
                        urls[os.path.join(base_uri ,filename)] = int(size)
                    else:
                        logging.warn("Malformed checksum line \"%s\" in %s" %
                                     (line, index_uri))
                else:
                    checksums = 0
            if not checksums:
                if line == "SHA256:" or line == "SHA1:" or line == "MD5Sum:":
                    checksums = 1

        index_file.close()
        return urls

    def find_dep11_files_in_release(self, component, arch):
        # Look in the dists/DIST/Release file for the DEP-11 files that belong
        # to the given component and architecture.
        release_uri = os.path.join(self.uri, "Release")
        release_path = os.path.join(self.config.skel_path,
                                    sanitise_uri(release_uri, self.config))

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
                        if re.match(component + r'/dep11/(Components-'+arch+r'\.yml|icons-[^./]+\.tar)\.(gz|bz2|xz)', filename):
                            urls[os.path.join(self.uri, filename)] = int(size)
                    else:
                        logging.warn("Malformed checksum line \"%s\" in %s" %
                                     (line, release_uri))
                else:
                    checksums = 0
            if not checksums:
                if line == "SHA256:":
                    checksums = 1

        return urls
