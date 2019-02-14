#!/usr/bin/env python2
# coding:utf-8

from __future__ import print_function
import os
import sys
import subprocess
import re
import time
import logging
from .config import MirrorConfig
from .utils import remove_double_slashes, remove_spaces, sanitise_uri, format_bytes, quoted_path, copy_file
from .apt_index import AptIndex

COMPRESSIONS = ['.gz', '.bz2', '.xz']


def output(string):
    sys.stdout.write(string)
    sys.stdout.flush()


def download_urls(stage, urls, context):
    childrens = []
    i = 0
    nthreads = context.nthreads
    args = []

    if len(urls) < nthreads:
        nthreads = len(urls)

    if context.auth_no_challenge == 1:
        args.append("--auth-no-challenge")
    if context.no_check_certificate == 1:
        args.append("--no-check-certificate")
    if context.unlink == 1:
        args.append("--unlink")
    if context.use_proxy and (context.use_proxy == 'yes' or context.use_proxy == 'on'):
        if context.http_proxy or context.https_proxy:
            args.append("-e use_proxy=yes")
        if context.http_proxy:
            args.append("-e http_proxy=" + context.http_proxy)
        if context.https_proxy:
            args.append("-e https_proxy=" + context.https_proxy)
        if context.proxy_user:
            args.append("-e proxy_user=" + context.proxy_user)
        if context.proxy_password:
            args.append("-e proxy_password=" + context.proxy_password)
    print("Downloading", len(urls),  stage,
          "files using", nthreads, "threads...")

    while urls:
        # splice
        amount = len(urls) / nthreads
        part = urls[:amount]
        urls = urls[amount:]
        with open(os.path.join(context.var_path,
                               stage + '-urls.%d' % i),
                  'w') as URLS:
            URLS.write('\n'.join(part))

        child = subprocess.Popen(['wget', '--no-cache',
                                  '--limit-rate=' + context.limit_rate,
                                  '-t', '5', '-r', '-N', '-l', 'inf',
                                  '-o', context.var_path + "/" +
                                  stage + "-log.%d" % i,
                                  '-i', context.var_path + "/" + stage + "-urls.%d" % i] + args)
        childrens.append(child)
        i += 1
        nthreads -= 1

    print("Begin time: ", time.strftime('%c'))
    output("[" + str(len(childrens)) + "]... ")
    while childrens:
        pid,_retcode = os.wait()
        childrens = [c for c in childrens if c.pid != pid]
        output("[" + str(len(childrens)) + "]... ")
    print("\nEnd time: ", time.strftime('%c'), "\n")


class AptMirror(object):
    def __init__(self, config_file):
        self.lock_file = None
        self.urls_to_download = {}
        self.index_urls = []
        self.stat_cache = {}
        self.rm_dirs = []
        self.rm_files = []
        self.unnecessary_bytes = 0
        # config
        self.config = MirrorConfig(config_file)
        return

    def run(self):
        self.init()
        self.lock_aptmirror()

        # Skel download
        self.download_skel()
        self.download_translation()
        self.download_dep11()

        # Main download
        self.download_archive()
        self.copy_skel()
        # Make cleaning script
        self.clean()
        self.post()

        self.unlock_aptmirror()

    def init(self):
        # Create the 3 needed directories if they don't exist yet
        needed_directories = (self.config.mirror_path,
                              self.config.skel_path,
                              self.config.var_path)
        for directory in needed_directories:
            if not os.path.isdir(directory):
                os.makedirs(directory)

    def lock_aptmirror(self):
        import fcntl
        self.lock_file = open(os.path.join(
            self.config.var_path, 'apt-mirror.lock'), 'a')
        try:
            fcntl.lockf(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except:
            print("apt-mirror is already running, exiting")
            sys.exit(1)

    def unlock_aptmirror(self):
        self.lock_file.close()
        os.unlink(os.path.join(self.config.var_path, "apt-mirror.lock"))

    def _stat(self, filename):
        if filename in self.stat_cache:
            return self.stat_cache[filename]
        try:
            size = os.stat(filename).st_size
            self.stat_cache[filename] = size
        except:
            size = 0
        self.stat_cache[filename] = size
        return size

    def clear_stat_cache(self):
        self.stat_cache = {}

    def need_update(self, filename, size_on_server):
        size = self._stat(filename)
        if not size:
            return 1
        elif size_on_server == size:
            return 0
        else:
            return 1

    def add_url_to_download(self, url, size=0, compressed=False):
        download_url = remove_double_slashes(url, self.config)
        if compressed:
            for ext in COMPRESSIONS:
                self.urls_to_download[download_url + ext] = size
        else:
            self.urls_to_download[download_url] = size

    def process_index(self, uri, index):
        path = sanitise_uri(uri, self.config)
        mirror = self.config.mirror_path + "/" + path

        index_path = os.path.join(path, index)

        if os.path.exists(index_path + '.gz'):
            os.system("gunzip < %s.gz > %s" % (index_path, index_path))
        elif os.path.exists(index_path + ".xz"):
            os.system("xz -d < %s.xz > %s" % (index_path, index_path))
        elif os.path.exists(index_path + ".bz2"):
            os.system("bzip2 -d < %s.bz2 > %s" % (index_path, index_path))

        try:
            index_file = open(index_path)
        except:
            logging.warn(
                "apt-mirror: can't open index %s in process_index" % index_path)
            return

        pkg_field_pattern = re.compile(r'^([\w\-]+):(.*)')

        for package in index_file.read().split('\n\n'):
            package = package.strip()
            if not package:
                continue
            lines = {'': ''}

            key = ''
            for line in package.split('\n'):
                match = re.match(pkg_field_pattern, line)
                if match:
                    key, value = match.groups()
                    lines[key] = value
                else:
                    lines[key] += '\n' + line

            if 'Directory' not in lines:
                lines['Directory'] = ''

            remove_spaces(lines)

            if 'Filename' in lines:
                # Packages index
                store_path = remove_double_slashes(path + "/" + lines["Filename"],
                                                   self.config)
                self.config.skipclean[store_path] = 1
                self.file_all.write(store_path + '\n')
                if 'MD5sum' in lines:
                    self.file_md5.write(
                        lines["MD5sum"] + "  " + store_path + "\n")
                if 'SHA1' in lines:
                    self.file_sha1.write(
                        lines['SHA1'] + '  ' + store_path + '\n')
                if 'SHA256' in lines:
                    self.file_sha256.write(
                        lines["SHA256"] + "  " + store_path + "\n")
                if self.need_update(mirror + "/" + lines["Filename"], int(lines["Size"])):
                    download_uri = uri + "/" + lines["Filename"]
                    self.file_new.write(remove_double_slashes(
                        download_uri, self.config) + "\n")
                    self.add_url_to_download(
                        download_uri, int(lines["Size"]))
            else:
                # Sources index
                for line in lines['Files'].split('\n'):
                    line = line.strip()
                    if line == '':
                        continue
                    try:
                        checksum, size, fn = line.split()
                    except:
                        raise Exception('apt-mirror: invalid Sources format')
                    store_path = remove_double_slashes(path + "/" + lines["Directory"] + "/" + fn,
                                                       self.config)
                    self.config.skipclean[store_path] = 1
                    self.file_all.write(store_path + "\n")
                    self.file_md5.write(checksum + "  " + store_path + "\n")
                    if self.need_update(mirror + "/" + lines["Directory"] + "/" + fn, int(size)):
                        download_uri = uri + "/" + \
                            lines["Directory"] + "/" + fn
                        self.file_new.write(remove_double_slashes(
                            download_uri,
                            self.config
                        ) + "\n")
                        self.add_url_to_download(
                            download_uri, int(size))

        index_file.close()

    def download_skel(self):
        self.urls_to_download = {}
        for uri, distribution, components in self.config.sources:
            if components:
                url = uri + "/dists/" + distribution + "/"

                self.add_url_to_download(url + "InRelease")
                self.add_url_to_download(url + "Release")
                self.add_url_to_download(url + "Release.gpg")
                for component in components:
                    self.add_url_to_download(
                        url + component + "/source/Release")
                    self.add_url_to_download(
                        url + component + "/source/Sources", compressed=True)
            else:
                self.add_url_to_download(uri + "/" + distribution + "/Release")
                self.add_url_to_download(
                    uri + "/" + distribution + "/Release.gpg")
                self.add_url_to_download(uri + "/" + distribution +
                                         "/Sources", compressed=True)

        for arch, uri, distribution, components in self.config.binaries:
            if components:
                url = uri + "/dists/" + distribution + "/"

                self.add_url_to_download(url + "InRelease")
                self.add_url_to_download(url + "Release")
                self.add_url_to_download(url + "Release.gpg")
                if self.config._contents:
                    self.add_url_to_download(
                        url + "Contents-" + arch, compressed=True)
                for component in components:
                    if self.config._contents:
                        self.add_url_to_download(
                            url + component + "/Contents-" + arch, compressed=True)
                    self.add_url_to_download(
                        url + component + "/binary-" + arch + "/Release")
                    self.add_url_to_download(
                        url + component + "/binary-" + arch + "/Packages", compressed=True)
                    self.add_url_to_download(url + component + "/i18n/Index")
            else:
                self.add_url_to_download(uri + "/" + distribution + "/Release")
                self.add_url_to_download(
                    uri + "/" + distribution + "/Release.gpg")
                self.add_url_to_download(uri + "/" + distribution +
                                         "/Packages", compressed=True)

        os.chdir(self.config.skel_path)
        self.index_urls = sorted(self.urls_to_download.keys())
        download_urls("index", self.index_urls, self.config)

        for key in self.urls_to_download.keys():
            path = key.split('://')[-1]
            if self.config._tilde:
                path = path.replace('~', '%7E')
            self.config.skipclean[path] = 1
            if path.endswith('.gz') or path.endswith('.bz2'):
                self.config.skipclean[path.rsplit('.', 1)[0]] = 1

    def download_translation(self):
        # Translation index download
        self.urls_to_download = {}
        output("Processing translation indexes: [")
        for _arch, uri, distribution, components in self.config.binaries:
            output("T")
            if components:
                index_uri = os.path.join(uri, 'dists', distribution)
                apt_index = AptIndex(index_uri, self.config)
                for component in components:
                    for url, size in apt_index.find_translation_files_in_index(component).iteritems():
                        self.add_url_to_download(url, size)

        output("]\n\n")

        self.index_urls.extend(sorted(self.urls_to_download.keys()))
        download_urls("translation", sorted(
            self.urls_to_download.keys()), self.config)

        for url in self.urls_to_download.keys():
            url = url.split('://')[-1]
            if self.config._tilde:
                url = url.replace('~', '%7E')
            self.config.skipclean[url] = 1

    def download_dep11(self):
        # DEP-11 index download
        self.urls_to_download = {}
        output("Processing DEP-11 indexes: [")
        for arch, uri, distribution, components in self.config.binaries:
            output("D")
            if components:
                index_uri = os.path.join(uri, 'dists', distribution)
                apt_index = AptIndex(index_uri, self.config)
                for component in components:
                    for url, size in apt_index.find_dep11_files_in_release(component, arch).iteritems():
                        self.add_url_to_download(url, size)

        output("]\n\n")

        self.index_urls.extend(sorted(self.urls_to_download.keys()))
        download_urls("dep11", sorted(
            self.urls_to_download.keys()), self.config)

        for url in self.urls_to_download.keys():
            url = url.split('://')[-1]
            if self.config._tilde:
                url = url.replace('~', '%7E')
            self.config.skipclean[url] = 1

    def download_archive(self):
        self.urls_to_download = {}

        self.file_all = open(os.path.join(
            self.config.var_path, 'ALL'), 'w')
        self.file_new = open(os.path.join(
            self.config.var_path, 'NEW'), 'w')
        self.file_md5 = open(os.path.join(
            self.config.var_path, 'MD5'), 'w')
        self.file_sha1 = open(os.path.join(
            self.config.var_path, 'SHA1'), 'w')
        self.file_sha256 = open(os.path.join(
            self.config.var_path, 'SHA256'), 'w')

        output("Processing indexes: [")
        for uri, distribution, components in self.config.sources:
            output("S")
            if components:
                for component in components:
                    self.process_index(uri, "dists/%s/%s/source/Sources" %
                                       (distribution, component))
            else:
                self.process_index(uri, "%s/Sources" % distribution)

        for arch, uri, distribution, components in self.config.binaries:
            output("P")
            if components:
                for component in components:
                    self.process_index(uri, "dists/%s/%s/binary-%s/Packages" %
                                       (distribution, component, arch))
            else:
                self.process_index(uri, "%s/Packages" % distribution)

        self.clear_stat_cache()

        output("]\n\n")

        self.file_all.close()
        self.file_new.close()
        self.file_md5.close()
        self.file_sha1.close()
        self.file_sha256.close()
        os.chdir(self.config.mirror_path)

        need_bytes = sum(self.urls_to_download.itervalues())

        size_output = format_bytes(need_bytes)

        print(size_output, " will be downloaded into archive.")

        download_urls("archive", sorted(
            self.urls_to_download.keys()), self.config)

    def copy_skel(self):
        # Copy skel to main archive
        for url in self.index_urls:
            if not re.match(r'^(\w+)://', url):
                raise Exception("apt-mirror: invalid url in index_urls")
            rel_url = sanitise_uri(url, self.config)
            copy_file(self.config.skel_path + "/" + rel_url,
                      self.config.mirror_path + "/" + rel_url,
                      unlink=self.config.unlink)
            for ext in COMPRESSIONS:
                if url.endswith(ext):
                    raw_file = url.rsplit('.', 1)[0]
                    rel_url = sanitise_uri(raw_file, self.config)
                    copy_file(self.config.skel_path + "/" + rel_url,
                              self.config.mirror_path + "/" + rel_url,
                              unlink=self.config.unlink)

    def process_file(self, path):
        if self.config._tilde:
            path = path.replace('~', '%7E')
        if self.config.skipclean.get(path):
            return 1
        self.rm_files.append(sanitise_uri(path, self.config))

        block_count, block_size = os.popen(
            'stat -c "%b,%B" ' + path).read().strip().split(',')
        self.unnecessary_bytes += int(block_count) * int(block_size)
        return 0

    def process_directory(self, directory):
        is_needed = 0
        if self.config.skipclean.get(directory):
            return 1
        for sub in os.listdir(directory):
            path = directory + "/" + sub
            if os.path.islink(path):
                # symlinks are always needed
                is_needed |= 1
            elif os.path.isdir(path):
                is_needed |= self.process_directory(path)
            elif os.path.isfile(path):
                is_needed |= self.process_file(path)

        if not is_needed:
            self.rm_dirs.append(directory)
        return is_needed

    def clean(self):
        os.chdir(self.config.mirror_path)

        for path in self.config.clean_directory:
            if os.path.isdir(path) and not os.path.islink(path):
                self.process_directory(path)

        script = open(self.config.cleanscript, 'w')

        i = 0
        total = len(self.rm_files)
        size_output = format_bytes(self.unnecessary_bytes)

        if self.config._autoclean:
            print(size_output, "in", total, "files and",
                  len(self.rm_dirs), "directories will be freed...")

            os.chdir(self.config.mirror_path)

            for path in self.rm_files:
                os.unlink(path)
            for path in self.rm_dirs:
                os.rmdir(path)
        else:
            print(size_output, "in", total, "files and",
                  len(self.rm_dirs), " directories can be freed.")
            print("Run ", self.config.cleanscript, " for this purpose.\n")

            script.write("#!/bin/sh\n")
            script.write("set -e\n\n")
            script.write(
                "cd " + quoted_path(self.config.mirror_path) + "\n\n")
            script.write("echo 'Removing %d unnecessary files [%s]...'\n" % (
                total, size_output))
            for filepath in self.rm_files:
                script.write("rm -f '%s'\n" % filepath)
                if i % 500 == 0:
                    script.write(
                        "echo -n '[" + str(int(100 * i / total)) + "%]'\n")
                if i % 10 == 0:
                    script.write("echo -n .\n")
                i += 1
            script.write("echo 'done.'\n")
            script.write("echo\n\n")

            i = 0
            total = len(self.rm_dirs)
            script.write(
                "echo 'Removing %d unnecessary directories...'\n" % total)
            for dirpath in self.rm_dirs:
                script.write("if test -d '%s'; then rmdir '%s'; fi\n" %
                             (dirpath, dirpath))
                if i % 50 == 0:
                    script.write(
                        "echo -n '[" + str(int(100 * i / total)) + "%]'\n")
                script.write("echo -n .\n")
                i += 1
            script.write("echo 'done.'\n")
            script.write("echo\n")

            script.close()

        # Make clean script executable
        os.system('chmod a+x ' + self.config.cleanscript)

    def post(self):
        if not self.config.run_postmirror:
            return

        post_script = self.config.postmirror_script
        print("Running the Post Mirror script ...")
        print("(" + post_script + ")\n")

        if os.path.isfile(post_script):
            if os.access(post_script, os.X_OK):
                os.system(post_script)
            else:
                os.system('/bin/sh ' + post_script)
        else:
            logging.warn('Postmirror script not found')
        print("\nPost Mirror script has completed. See above output for any possible errors.\n")


def main():
    config_file = "/etc/apt/mirror.list"
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    if not os.path.exists(config_file):
        print('apt-mirror: invalid config file specified')
        sys.exit(1)

    apt_mirror = AptMirror(config_file)
    apt_mirror.run()


if __name__ == '__main__':
    main()
