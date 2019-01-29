# apt-mirror rewritten by Python


Currently this project is basically a line by line translate from apt-mirror 0.5.4, so it is 100% compatible with apt-mirror config file.

## Usage:
Get help from http://apt-mirror.github.com/

## Todo:

* Automatically check the checksum of index files before download archive files.
* If check failed, retry several times.
* threading.Queue for download threads.
* Accept command line arguments to override variables from mirror.list file.

Any improvement is welcome.