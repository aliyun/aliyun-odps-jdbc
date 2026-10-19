"""Print the current Maven version's section from the canonical changelog."""

import pathlib
import re
import xml.etree.ElementTree as ET


root = pathlib.Path(__file__).resolve().parent.parent
version = ET.parse(root / "pom.xml").getroot().find(
    "{http://maven.apache.org/POM/4.0.0}version"
).text
lines = (root / "CHANGELOG.md").read_text(encoding="utf-8").splitlines()
heading = re.compile(r"^## \[?" + re.escape(version) + r"\]?(?=\s|$)")
starts = [index for index, line in enumerate(lines) if heading.match(line)]
if len(starts) != 1:
    raise SystemExit("Expected exactly one CHANGELOG.md section for " + version)
start = starts[0]
end = next(
    (index for index in range(start + 1, len(lines)) if lines[index].startswith("## ")),
    len(lines),
)
notes = "\n".join(lines[start + 1:end]).strip()
if not notes:
    raise SystemExit("Empty CHANGELOG.md section for " + version)
print(notes)
