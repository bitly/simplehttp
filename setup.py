from setuptools import setup

# release steps
# ---------------
# update version variable (below)
# update pysimplehttp/src/__init__.__version__
# run python setup.py sdist
# upload .tar.gz to github
# run python setup.py register to update pypi

__version__ = "0.2.0"
scripts = ['pysimplehttp/scripts/ps_to_sq.py', 
           'pysimplehttp/scripts/file_to_sq.py']

setup(
    name='pysimplehttp',
    version=__version__,
    author='Jehiah Czebotar',
    author_email='jehiah@gmail.com',
    description='Python libraries for simplehttp',
    url='https://github.com/bitly/simplehttp',
    classifiers=[
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          ],
    download_url="http://github.com/downloads/bitly/simplehttp/pysimplehttp-%s.tar.gz" %__version__,

    packages=['pysimplehttp'],
    package_dir = {'pysimplehttp' : 'pysimplehttp/src'},

    scripts = scripts,
    install_requires = [
        'tornado',
    ],
    requires = [
        'ujson',
        'host_pool',
    ],
)
