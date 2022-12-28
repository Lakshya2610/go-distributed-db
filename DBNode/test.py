import random, string, subprocess
from typing import List, Tuple

DEBUGGING_LOCALLY = True
TYPE_POST = "POST"
TYPE_GET = "GET"

TOTAL_REQUESTS = 1000

hosts = [
        "dh2020pc08.utm.utoronto.ca",
        "dh2026pc01.utm.utoronto.ca",
        "dh2026pc02.utm.utoronto.ca",
        "dh2020pc15.utm.utoronto.ca",
        "dh2020pc13.utm.utoronto.ca",
        "dh2020pc14.utm.utoronto.ca"
    ]

generatedURLs: List[Tuple[str, str]] = []

def Debug_GetRequest(shortResource, longResource, type):
	localhostPorts = range(5000, 5001)
	port = random.choice( localhostPorts )
	if type == "POST":
		return f"http://localhost:{port}/set?short={shortResource}&long={longResource}"
	else:
		return f"http://localhost:{port}/get?short={shortResource}"

def GetRequest(shortResource, longResource, type):
	port = 5000
	host = random.choice(hosts)
	if type == "POST":
		return f"http://{host}:{port}/set?short={shortResource}&long={longResource}"
	else:
		return f"http://{host}:{port}/get?short={shortResource}"

def post():
	longResource = "http://"+''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
	shortResource = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6))

	request = Debug_GetRequest(shortResource, longResource, TYPE_POST) if DEBUGGING_LOCALLY else GetRequest(shortResource, longResource, TYPE_POST)
	if subprocess.call(["curl", "-X", "POST", request], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0:
		generatedURLs.append((shortResource, longResource))
		return True
	
	return False


def get():
	short, long = random.choice(generatedURLs)
	request = Debug_GetRequest(short, long, TYPE_GET) if DEBUGGING_LOCALLY else GetRequest(short, long, TYPE_GET)
	if subprocess.call(["curl", request], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0:
		return True
	
	return False


successfulGets = 0
successfulPosts = 0
for i in range(TOTAL_REQUESTS):
	shouldGet = len(generatedURLs) > 0 and random.uniform(0, 1) > 0.8
	if shouldGet:
		successfulGets += int( get() )
	else:
		successfulPosts += int( post() )

print(f"{TOTAL_REQUESTS} requests sent. Num successful requests: GET -> {successfulGets} | POST -> {successfulPosts}")
