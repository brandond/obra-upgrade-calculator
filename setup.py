# -*- coding: utf-8 -*-
from os import chdir
from os.path import abspath, dirname

from setuptools import find_packages, setup

chdir(dirname(abspath(__file__)))


with open('README.md') as f:
    readme = f.read()

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    author='Brandon Davidson',
    author_email='brad@oatmail.org',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
    ],
    description='OBRA Upgrade Points Calculator',
    entry_points={
        'console_scripts': ['obra-upgrade-calculator=obra_upgrade_calculator.commands:cli']
    },
    include_package_data=True,
    install_requires=requirements,
    long_description=readme,
    name='obra-upgrade-calculator',
    packages=find_packages(exclude=('docs')),
    python_requires='>=2.7',
    url='https://github.com/brandond/obra-upgrade-calculator',
    version='1.0.0',
)
