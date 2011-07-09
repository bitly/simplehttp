#!/usr/bin/env python

from distutils.core import setup

scripts = ['pysimplehttp/scripts/ps_to_sq.py', 
           'pysimplehttp/scripts/file_to_sq.py']

version = "0.1"
setup(name='pysimplehttp',
        version=version,
        description='Python libraries for simplehttp',
        author='Jehiah Czebotar',
        author_email='jehiah@gmail.com',
        url='https://github.com/bitly/simplehttp',
        classifiers=[
              'Intended Audience :: Developers',
              'Programming Language :: Python',
              ],
        download_url="http://github.com/downloads/bitly/simplehttp/pysimplehttp-%s.tar.gz" % version,
        scripts = scripts,
        packages=['pysimplehttp'],
        package_dir = {'pysimplehttp' : 'pysimplehttp/src'},
        install_requires=['tornado', 'ujson'],
        requires=['tornado', 'ujson'],
    )