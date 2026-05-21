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
Minimal setup.py for JobLens Trigger service to allow shiv packaging.
"""

from setuptools import setup, find_packages
from version import __version__
import os

# Read requirements from requirements.txt
def read_requirements():
    with open('requirements.txt', 'r') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name="joblens-trigger",
    version=__version__,
    description="JobLens Trigger Service - RESTful API for JobLens",
    author="JobLens Team",
    license="MIT",
    packages=find_packages(),
    py_modules=['app', 'app_factory', 'entrypoint', 'version'],
    install_requires=read_requirements(),
    python_requires='>=3.8',
    include_package_data=True,
)