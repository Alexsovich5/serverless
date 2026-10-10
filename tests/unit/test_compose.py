"""Checks on docker-compose.yml: loopback-only ports and a fixed project name."""

import os

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _compose():
    with open(os.path.join(ROOT, 'docker-compose.yml')) as handle:
        return yaml.safe_load(handle)


def test_published_ports_bind_to_loopback_only():
    published = [(name, port)
                 for name, service in _compose()['services'].items()
                 for port in service.get('ports', [])]
    assert published
    for name, port in published:
        assert str(port).startswith('127.0.0.1:'), '%s exposes %s publicly' % (name, port)


def test_published_host_ports_stay_in_assigned_range():
    for service in _compose()['services'].values():
        for port in service.get('ports', []):
            host_port = int(str(port).split(':')[1])
            assert 20700 <= host_port <= 20799


def test_project_name_is_set_in_the_file():
    assert _compose()['name'] == 'serverless'


def test_obsolete_version_key_is_absent():
    assert 'version' not in _compose()
