"""The worker over its real pipes: UTF-8 in, UTF-8 out, whatever the locale."""

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class WorkerProtocol(unittest.TestCase):
    def run_worker(self, record, env_extra=None):
        env = {**os.environ, **(env_extra or {})}
        env.pop("PYTHONIOENCODING", None)
        env.pop("PYTHONUTF8", None)
        request = json.dumps({"id": 7, "record": record, "context": {"feed": "t", "file": "t", "line": 1}}, ensure_ascii=False)
        proc = subprocess.run(
            [sys.executable, "-u", str(ROOT / "hie_runtime" / "worker.py"), str(ROOT / "transforms" / "fhir_passthrough.py")],
            input=(request + "\n").encode("utf-8"),
            capture_output=True,
            env=env,
            timeout=30,
        )
        lines = proc.stdout.decode("utf-8").splitlines()
        self.assertEqual(json.loads(lines[0]), {"ready": True}, proc.stderr.decode("utf-8", "replace"))
        return json.loads(lines[1])

    def test_accented_names_survive_the_round_trip(self):
        record = {"resourceType": "Practitioner", "id": "p1", "name": [{"family": "Beltrán", "given": ["Ángela"]}], "note": "日本語 — ñ — 😀"}
        reply = self.run_worker(record)
        self.assertEqual(reply["id"], 7)
        self.assertEqual(reply["resources"][0]["name"][0]["family"], "Beltrán")
        self.assertEqual(reply["resources"][0]["name"][0]["given"], ["Ángela"])
        self.assertEqual(reply["resources"][0]["note"], "日本語 — ñ — 😀")

    def test_a_data_error_comes_back_as_data(self):
        reply = self.run_worker({"resourceType": "Observation"})
        self.assertEqual(reply["error"]["kind"], "data")
        self.assertIn("neither id nor identifier", reply["error"]["message"])



class TryStreamCapture(unittest.TestCase):
    """With HIE_WORKER_CAPTURE=1 each record's output comes back, bounded."""

    def test_a_stream_keeps_only_its_first_bytes_on_a_character_boundary(self):
        sys.path.insert(0, str(ROOT / "hie_runtime"))
        import worker

        stream = worker.BoundedStream(limit=5)
        self.assertEqual(stream.write("ab"), 2)
        stream.write("\u20ac\u20ac")  # three bytes each: only the first fits
        self.assertEqual(stream.getvalue(), "ab\u20ac")
        self.assertTrue(stream.truncated)
        for _ in range(10_000):
            stream.write("more")
        self.assertEqual(stream.getvalue(), "ab\u20ac")

        cut = worker.BoundedStream(limit=4)
        cut.write("ab\u20ac")  # the euro would end past the limit
        self.assertEqual((cut.getvalue(), cut.truncated), ("ab", True))
        exact = worker.BoundedStream(limit=4)
        exact.write("abcd")
        self.assertEqual((exact.getvalue(), exact.truncated), ("abcd", False))

    def test_a_record_that_prints_a_lot_returns_at_most_4_kib_per_stream(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            script = Path(tmp) / "loud.py"
            script.write_text(
                "import sys\n\n"
                "def transform(record, context):\n"
                "    print('\u00e9' * 5000)\n"
                "    print('err', file=sys.stderr)\n"
                "    return None\n",
                encoding="utf-8",
            )
            env = {**os.environ, "HIE_WORKER_CAPTURE": "1"}
            request = json.dumps({"id": 1, "record": {}, "context": {"feed": "t", "file": "t", "line": 1}})
            proc = subprocess.run(
                [sys.executable, "-u", str(ROOT / "hie_runtime" / "worker.py"), str(script)],
                input=(request + "\n").encode("utf-8"),
                capture_output=True,
                env=env,
                timeout=30,
            )
        lines = proc.stdout.decode("utf-8").splitlines()
        self.assertEqual(json.loads(lines[0]), {"ready": True})
        diagnostics = json.loads(lines[1])["diagnostics"]
        self.assertEqual(diagnostics["stdout"], "\u00e9" * 2048)
        self.assertEqual(len(diagnostics["stdout"].encode("utf-8")), 4096)
        self.assertEqual(diagnostics["stderr"], "err\n")
        self.assertIs(diagnostics["truncated"], True)


if __name__ == "__main__":
    unittest.main()
