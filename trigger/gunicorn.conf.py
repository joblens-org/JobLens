#   Copyright 2026 - 2026 wzycc
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
"""
Gunicorn configuration for JobLens Trigger

This file is loaded by gunicorn when starting the application.
Since preload_app=True, the app is initialized in the master process.
"""

# Network binding
bind = "0.0.0.0:7592"

# Worker configuration
workers = 1
worker_class = "sync"
timeout = 120
keepalive = 5

# Application preloading
# When True, the app is loaded in the master process before forking workers
preload_app = True

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"


def post_fork(server, worker):
    """
    Called after a worker process is forked.
    
    Args:
        server: The gunicorn server instance
        worker: The worker instance
    """
    server.log.info(f"Worker spawned (pid: {worker.pid})")


def worker_exit(server, worker):
    """
    Called when a worker process is exiting.
    
    This hook is used to cleanup resources in worker processes.
    
    Args:
        server: The gunicorn server instance
        worker: The worker instance
    """
    server.log.info(f"Worker exiting (pid: {worker.pid})")
    
    # try:
    #     # Get the Flask app instance from the server
    #     app = server.app
        
    #     # Cleanup resources via app_context
    #     if hasattr(app, '_app_context'):
    #         server.log.info("Cleaning up worker resources...")
    #         app._app_context.shutdown()
    #         server.log.info("Worker resources cleaned up")
    # except Exception as e:
    #     server.log.error(f"Error during worker cleanup: {e}")


def on_exit(server):
    """
    Called when the master process is exiting.
    
    This hook is used to cleanup resources in the master process.
    
    Args:
        server: The gunicorn server instance
    """
    server.log.info("Gunicorn master process is exiting...")
    
    try:
        # Get the Flask app instance from the server
        # server.app is the gunicorn WSGIApplication, wsgi() returns the actual Flask app
        app = server.app.wsgi()
        
        # Cleanup resources via app_context
        if hasattr(app, '_app_context'):
            server.log.info("Cleaning up master process resources...")
            app._app_context.shutdown()
            server.log.info("Master process resources cleaned up")
    except Exception as e:
        server.log.error(f"Error during master cleanup: {e}")
    
    server.log.info("Gunicorn master process has exited")