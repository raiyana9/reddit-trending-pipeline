"""Block until an HTTP endpoint returns 200 (stdlib only, so `make demo` needs no Python deps)."""

import sys
import time
import urllib.request

url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000/health"
timeout = float(sys.argv[2]) if len(sys.argv) > 2 else 240
deadline = time.time() + timeout
while time.time() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=3) as r:
            if r.status == 200:
                print(f"ready: {url}")
                sys.exit(0)
    except Exception:
        pass
    time.sleep(2)
print(f"timed out waiting for {url}", file=sys.stderr)
sys.exit(1)
