"""Run scripts/smoke.sh against the in-process runner and check it reports success."""

import os
import subprocess

SCRIPT = os.path.join(os.path.dirname(__file__), '..', '..', 'scripts', 'smoke.sh')


def test_smoke_script_posts_event_and_sees_it_processed(base_url):
    result = subprocess.run(
        ['sh', SCRIPT, base_url],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
    output = result.stdout.decode('utf-8')

    assert result.returncode == 0, output
    assert 'processed' in output
    assert 'Invoice.Created' in output
