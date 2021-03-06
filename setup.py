# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='bubble-chamber',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version='1.0.0',

    description='Sysdig event analyser',
    long_description=long_description,

    # The project's main homepage.
    url='https://github.com/osallou/sysdig-analyser',

    # Author details
    author='Olivier Sallou',
    author_email='olivier.sallou@irisa.fr',

    # Choose your license
    license='Apache 2.0',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: Apache Software License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],

    keywords='sysevent event alanyser',

    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),

    install_requires=[
                      'mongo',
                      'progressbar',
                      'flask',
                      'gunicorn',
                      'PyYAML',
                      'PyJWT',
                      'prometheus_client',
                      'python-consul',
                      'pika==0.12.0',
                      'influxdb',
                      'SQLAlchemy',
                      'MySQL-Python'
                     ],
    scripts=[
            'bc_api.py',
            'bc_web_record.py',
            'bc_db.py',
            'bc_record.py'
    ],
)
