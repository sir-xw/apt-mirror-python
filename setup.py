import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

with open('VERSION', 'r') as fh:
    version = fh.read().strip()

setuptools.setup(
    name='apt-mirror-python',
    version=version,
    author='Xie Wei',
    author_email='xw.master@live.cn',
    description='A small and efficient tool that lets you mirror a part of or the whole Debian GNU/Linux distribution or any other apt sources.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/sir-xw/apt-mirror-python',
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python',
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Operating System :: POSIX :: Linux',
    ],
    entry_points={
        'console_scripts': [
            'apt-mirror=apt_mirror:main',
        ],
    }
)
