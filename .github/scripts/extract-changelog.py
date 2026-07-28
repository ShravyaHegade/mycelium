"""Extract the changelog section for a given version.
Called from release.yml; no external dependencies beyond stdlib.
"""
import re
import sys

version = sys.argv[1]
with open("CHANGELOG.md") as f:
    content = f.read()

parts = re.split(r"^##\s+", content, flags=re.MULTILINE)
for part in parts:
    if part.split()[0] == version:
        sys.stdout.write("## " + part.strip())
        break
