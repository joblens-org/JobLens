#!/usr/bin/env python3
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
#!/usr/bin/env python3
"""
Entry point for joblens-trigger.pyz
When run without arguments, starts gunicorn with default configuration.
"""
import sys
import os

def find_config_file():
    """Find gunicorn.conf.py relative to the .pyz file or current directory."""
    # First, check if --config is already provided in arguments
    if "--config" in sys.argv:
        config_index = sys.argv.index("--config") + 1
        if config_index < len(sys.argv):
            config_path = sys.argv[config_index]
            if os.path.exists(config_path):
                return config_path
            else:
                print(f"Warning: Config file '{config_path}' not found", file=sys.stderr)
    
    # Try to find config file relative to the .pyz file location
    pyz_path = os.path.abspath(sys.argv[0])
    pyz_dir = os.path.dirname(pyz_path)
    
    # Check in the same directory as the .pyz file
    config_candidates = [
        os.path.join(pyz_dir, "gunicorn.conf.py"),
        os.path.join(os.getcwd(), "gunicorn.conf.py"),
        "/opt/JobLens/trigger/gunicorn.conf.py",
    ]
    
    for config_path in config_candidates:
        if os.path.exists(config_path):
            return config_path
    
    # If no config file found, return default name (will cause gunicorn to fail with proper error)
    return "gunicorn.conf.py"

def main():
    # If no arguments provided, add default arguments
    if len(sys.argv) == 1:
        # Find config file
        config_file = find_config_file()
        # Default: use found config file and app:app
        sys.argv.extend(["--config", config_file, "app:app"])
    else:
        # Ensure config file exists if --config is provided
        find_config_file()  # This will print warning if config not found
    
    # Import and run gunicorn
    from gunicorn.app.wsgiapp import run
    sys.exit(run())

if __name__ == "__main__":
    main()