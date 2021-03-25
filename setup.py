import os
from setuptools import setup, find_packages
import subprocess
import shutil

cwd = os.path.dirname(os.path.abspath(__file__))

version_txt = os.path.join(cwd, "version.txt")
with open(version_txt, "r") as f:
	version = f.readline().strip()
sha = "Unknown"
package_name = "obselt"

try:
	sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=cwd).decode("ascii").strip()
except Exception:
	pass


def write_version_file():
	version_path = os.path.join(cwd, "obselt", "version.py")
	with open(version_path, "w") as f:
		f.write("__version__ = '{}'\n".format(version))
		f.write("git_version = {}\n".format(repr(sha)))


if __name__ == "__main__":
	print("Building wheel {}-{}".format(package_name, version))

	write_version_file()

	with open("README.rst") as f:
		readme = f.read()

	setup(
		# Metadata
		name=package_name,
		version=version,
		author="Virevol",
		author_email="saurabh@virevol.com",
		url="https://github.com/virevolai/obselt",
		description="Tools for observable ELT",
		long_description=readme,
		license="MIT",
		# Package info
		packages=find_packages(exclude=("test",)),
		zip_safe=False,
		install_requires=["google-cloud-bigquery"],
	)
