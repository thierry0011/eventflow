"""
WSGI entry point, used by gunicorn.

WSGI vs ASGI, in one paragraph: WSGI is synchronous — one worker handles one
request at a time, and you scale by running more workers/threads. ASGI is
asynchronous — one worker juggles thousands of concurrent connections, which is
what you need for websockets, SSE, or fan-out to slow upstreams. Django supports
both; this service is transactional and database-bound, so WSGI with a handful
of gunicorn workers is simpler and performs the same. The FastAPI service in
this repo is ASGI — compare the two.

Worker count rule of thumb for CPU-light, IO-bound WSGI apps:
    workers = (2 x CPU cores) + 1
and remember each worker holds its own DB connections.
"""

import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

application = get_wsgi_application()
