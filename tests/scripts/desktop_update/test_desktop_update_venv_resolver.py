"""Isolated resolver matrix for the POSIX Desktop updater runtime."""
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SOURCE = REPO_ROOT / "scripts" / "desktop-update" / "posix.sh"


class ResolverTest(unittest.TestCase):
    def test_installed_layout_matrix(self):
        with tempfile.TemporaryDirectory(prefix="desktop-update-resolver-") as scratch:
            root = Path(scratch)
            script = root / "posix.sh"
            shutil.copy2(SOURCE, script)
            for name, paths, expected, code in (
                ("venv-only", ("venv",), "venv/bin", 0),
                ("dotvenv-only", (".venv",), ".venv/bin", 0),
                ("both", ("venv", ".venv"), "venv/bin", 0),
                ("neither", (), "", 3),
            ):
                with self.subTest(layout=name):
                    install = root / name
                    for envname in paths:
                        bindir = install / envname / "bin"
                        bindir.mkdir(parents=True)
                        hermes = bindir / "hermes"
                        hermes.write_text("#!/bin/sh\nexit 0\n")
                        hermes.chmod(0o755)
                    result = subprocess.run(
                        ["/bin/bash", str(script), "--install-root", str(install), "--self-test-resolver"],
                        env={**os.environ, "TMPDIR": scratch}, capture_output=True, text=True, timeout=10,
                    )
                    self.assertEqual(result.returncode, code, result.stderr)
                    if expected:
                        self.assertEqual(result.stdout.strip(), str(install / expected))
                    else:
                        self.assertEqual(result.stdout.strip(), "")


if __name__ == "__main__":
    unittest.main()
