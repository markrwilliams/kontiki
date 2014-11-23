"""
    kontiki
    ~~~~~~~

    A Twisted implementation of the Raft consensus algorithm

    :copyright: (c) 2014 by Mark Williams
    :license: BSD, see LICENSE for more details.

"""

from setuptools import setup, find_packages

__author__ = 'Mark Williams'
__version__ = '0.0b'
__contact__ = 'markrwilliams@gmail.com'
__url__ = 'https://github.com/markrwilliams/kontiki'
__license__ = 'BSD'
__description__ = 'A Raft consensus algorithm implementation in Twisted'
__requirements__ = open('requirements.txt').read().splitlines()



if __name__ == '__main__':
    setup(name='kontiki',
          version=__version__,
          description=__description__,
          long_description=__doc__,
          author=__author__,
          author_email=__contact__,
          url=__url__,
          install_requires=__requirements__,
          license=__license__,
          platforms='any',
          packages=find_packages())
