import os
from setuptools import setup, find_packages


with open(os.path.join(os.path.dirname(__file__), "README.rst")) as fh:
    readme = fh.read()

setup(
    name="task-logs",
    version=__import__("task_logs").__version__,
    description="Task manager logging middleware, used by task-dashboard",
    long_description=readme,
    author="Flare Systems",
    author_email="opensource@flare.systems",
    url="http://github.com/Flared/task-logs/",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Lesser GNU General Public License v3 (LGPLv3)",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
    ],
)
