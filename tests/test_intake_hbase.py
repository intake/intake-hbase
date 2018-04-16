
import happybase
import os

import pytest
import pandas as pd
import time

from intake_hbase.source import HBaseSource
from .util import start_hbase, stop_docker, TEST

CONNECT = {'host': 'localhost', 'port': 9090}
TEST_DATA_DIR = 'tests'
TEST_DATA = 'sample1.csv'
df = pd.read_csv(os.path.join(TEST_DATA_DIR, TEST_DATA))


@pytest.fixture(scope='module')
def engine():
    """Start docker container for ES and cleanup connection afterward."""
    stop_docker('intake-hbase', let_fail=True)
    cid = None
    try:
        cid = start_hbase()

        conn = happybase.Connection(**CONNECT)
        conn.create_table(TEST, {'field': dict()})
        table = conn.table(TEST)
        timeout = 20
        while True:
            try:
                for i, row in df.iterrows():
                    table.put(b'%i' % i, {'field:%s' % k: str(v)
                                          for (k, v) in row.to_dict().items()})
                break
            except Exception:
                assert time > 0, 'Timeout while waiting for HBase'
                time.sleep(0.5)
                timeout -= 0.5
        yield
    finally:
        stop_docker(cid=cid)


def test_read(engine):
    source = HBaseSource(TEST, CONNECT)
    out = source.read()
    assert len(out) == 4
    assert out[0][1][b'field:name'] == b'Alice'
    assert out[-1][1][b'field:name'] == b'Eve'


def test_read_part(engine):
    source = HBaseSource(TEST, CONNECT, divisions=list('01234'))
    source.discover()
    assert source.npartitions == 4
    out = source.read_partition(2)
    assert out[0][1][b'field:name'] == b'Charlie'
    out = source.read()
    assert len(out) == 4
