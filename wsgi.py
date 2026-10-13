"""Native Flask WSGI entrypoint for all five servers in serve.py."""

import app as app_module

app_module.started(app_module.SERVER)
app = app_module.app
