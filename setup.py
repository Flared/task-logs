import os

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))


def rel(*xs: str) -> str:
    return os.path.join(here, *xs)


with open(rel("README.md")) as f:
    long_description = f.read()


with open(rel("src", "task_logs", "__init__.py"), "r") as f:
    version_marker = "__version__ = "
    for line in f:
        if line.startswith(version_marker):
            _, version = line.split(version_marker)
            version = version.strip().strip('"')
            break
    else:
        raise RuntimeError("Version marker not found.")


dependencies = [
    "typing_extensions",
]

extra_dependencies = {
    "redis": ["redis>=2.0,<4.0"],
    "elasticsearch": ["elasticsearch>=6.0.0"],
    "dramatiq": ["dramatiq"],
}

extra_dependencies["all"] = list(set(sum(extra_dependencies.values(), [])))
extra_dependencies["test"] = extra_dependencies["all"] + [
    "pytest",
    "pytest-cov",
    "freezegun",
]
extra_dependencies["dev"] = extra_dependencies["test"] + [
    # Linting
    "flake8",
    "flake8-bugbear",
    "flake8-quotes",
    "flake8-mypy",
    "isort",
    "mypy",
    "black",
    # Docs
    # "sphinx",
    # "sphinx-autodoc-typehints",
]

setup(
    name="task-logs",
    version=version,
    author="Flare Systems Inc.",
    author_email="oss@flare.systems",
    description="Task manager logging middleware",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/flared/task-logs",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    package_data={"task_logs": ["py.typed"]},
    include_package_data=True,
    install_requires=dependencies,
    python_requires=">=3.5",
    extras_require=extra_dependencies,
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3 :: Only",
        "Development Status :: 4 - Beta",
        "Topic :: System :: Distributed Computing",
        (
            "License :: OSI Approved :: "
            "GNU Lesser General Public License v3 or later (LGPLv3+)"
        ),
    ],
)
