import subprocess
import json
import os

KILL_SCRIPT = f"{os.path.realpath(__file__).rstrip('teardown.py')}killnode.sh"

def teardown():
    hosts = list()
    with open( "hosts.json", "rb" ) as f:
        hosts = json.loads(f.read())["hosts"]
    
    for host in hosts:
        retcode = subprocess.call([KILL_SCRIPT, host])
        print(f"Issued kill command to {host} with retcode {retcode}")


if __name__ == "__main__":
    teardown()