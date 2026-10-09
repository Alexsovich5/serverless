import os
import re
import sys

import pkg_resources

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PIN = re.compile(r'^\s*([A-Za-z0-9_.\-]+)==([^\s#;]+)')


def read_pins(filename):
    pins = []
    with open(os.path.join(ROOT, filename)) as handle:
        for line in handle:
            match = PIN.match(line)
            if match:
                pins.append((match.group(1), match.group(2)))
    return pins


def test_python_version():
    assert sys.version_info[:3] == (3, 6, 5)


def test_runtime_requirements_are_pinned():
    names = {name.lower() for name, _ in read_pins('requirements.txt')}
    assert {'boto3', 'botocore', 's3transfer', 'jmespath',
            'python-dateutil', 'docutils', 'six'} <= names


def test_dev_requirements_are_pinned():
    names = {name.lower() for name, _ in read_pins('requirements-dev.txt')}
    assert {'pytest', 'flake8', 'moto'} <= names


def test_installed_versions_match_pins():
    mismatches = []
    for filename in ('requirements.txt', 'requirements-dev.txt'):
        for name, version in read_pins(filename):
            installed = pkg_resources.get_distribution(name).version
            if installed != version:
                mismatches.append('{}: pinned {}, installed {}'.format(name, version, installed))
    assert mismatches == []


def test_boto3_and_moto_import():
    import boto3
    import moto

    assert boto3.__version__ == '1.7.28'
    assert moto.__version__ == '1.3.3'
