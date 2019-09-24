from setuptools import setup,find_packages
import re
from os import path

def read(*paths):
    filename = path.join(path.abspath(path.dirname(__file__)), *paths)
    with open(filename, 'r') as f:
        return f.read()

def find_version(*paths):
    contents = read(*paths)
    match = re.search(r'^__version__ = [\'"]([^\'"]+)[\'"]', contents, re.M)
    if not match:
        raise RuntimeError('Unable to find version string.')
    return match.group(1)

setup(
    name='ThinkingDataSdk',
    version=find_version('tgasdk', 'sdk.py'),
    description='Official ThinkingData Analytics library for Python',
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    url='https://github.com/ThinkingDataAnalytics/python-sdk',
    license='Apache',
    author='ThinkingData, Inc.',
    author_email='sdk@thinkingdata.cn',
    packages=find_packages(),
    platforms=["all"],
    install_requires=['requests'],

    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries'
    ],
)
