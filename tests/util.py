import shlex
import subprocess
import requests

TEST = 'testtable'
container_name = 'intake_hbase'


def start_hbase():
    """Bring up a container running HBase.

    Waits until web UI is live and responsive.
    """
    print('Starting HBASE server...')

    cmd = shlex.split('docker run -d -p 9095:9095 -p 9090:9090 --name %s ' 
                      ' dajobe/hbase' % container_name)
    cid = subprocess.check_output(cmd).decode()[:-1]
    timeout = 60

    while True:
        try:
            r = requests.get('http://localhost:9095/thrift.jsp')
            if r.ok:
                break
            timeout -= 0.2
            assert timeout > 0, "Timed out waiting for HBase server"
        except requests.ConnectionError:
            pass
    return cid


def stop_docker(name=container_name, cid=None, let_fail=False):
    """Stop docker container with given name tag

    Parameters
    ----------
    name: str
        name field which has been attached to the container we wish to remove
    cid: str
        container ID, if known
    let_fail: bool
        whether to raise an exception if the underlying commands return an
        error.
    """
    try:
        if cid is None:
            print('Finding %s ...' % name)
            cmd = shlex.split('docker ps -q --filter "name=%s"' % name)
            cid = subprocess.check_output(cmd).strip().decode()
        if cid:
            print('Stopping %s ...' % cid)
            subprocess.call(['docker', 'kill', cid])
            subprocess.call(['docker', 'rm', cid])
    except subprocess.CalledProcessError as e:
        print(e)
        if not let_fail:
            raise
