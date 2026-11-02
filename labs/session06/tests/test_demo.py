"""실제 demo.sh에 가짜 runner를 연결해 충돌·단계·로그 동작을 확인해요."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[3]


class DemoTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        (self.root / "labs/session06").mkdir(parents=True)
        (self.root / "scripts").mkdir()
        shutil.copy(ROOT / "labs/session06/demo.sh", self.root / "labs/session06/demo.sh")
        runner = self.root / "scripts/lab.sh"
        runner.write_text("#!/usr/bin/env python3\n" + textwrap.dedent(f"""
            import importlib.util
            import os
            import sys
            import types
            sys.path.insert(0, {str(ROOT)!r})
            if importlib.util.find_spec("pyhive") is None:
                sys.modules["pyhive"] = types.SimpleNamespace(hive=None)
            from labs.session06 import lab

            class Cursor:
                def execute(self, sql):
                    self.rows = []
                    if sql == "SHOW NAMESPACES IN lake":
                        self.rows = [("lesson06",)]
                    elif sql == "SHOW TABLES IN lake.lesson06" and os.environ.get("DEMO_EXISTING"):
                        self.rows = [("lesson06", "s06_demo_naive_orders", False)]
                def fetchall(self):
                    return self.rows

            class Connection:
                def __enter__(self):
                    return self
                def __exit__(self, *args):
                    pass
                def cursor(self):
                    return Cursor()

            class Lesson(lab.Lesson):
                def prepare(self):
                    self.record("prepare", run_id=self.run_id)
                def scenario(self, name, naive_dates=False):
                    self.record("scenario", run_id=self.run_id, name=name, naive_dates=naive_dates)

            lab.Lesson = Lesson
            lab.connect = Connection
            sys.argv = ["-", sys.argv[-1]]
            exec(compile(sys.stdin.read(), "demo-stdin", "exec"))
        """))
        runner.chmod(0o755)

    def run_demo(self, *args, existing=False):
        env = {**os.environ, "DEMO_EXISTING": "1" if existing else ""}
        return subprocess.run(["bash", str(self.root / "labs/session06/demo.sh"), *args],
                              cwd=self.root, env=env, capture_output=True, text=True)

    def test_all_six_runs_stop_at_requested_stage(self):
        result = self.run_demo()
        self.assertEqual(result.returncode, 0, result.stderr)
        records = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(sum(r["step"] == "prepare" for r in records), 6)
        for run_id, stop, naive in [(f"s06-demo-s{i}", i, False) for i in range(2, 7)] + [("s06-demo-naive", 6, True)]:
            scenarios = [r for r in records if r["step"] == "scenario" and r["run_id"] == run_id]
            self.assertEqual([r["name"] for r in scenarios], [f"s{i}" for i in range(1, stop + 1)])
            self.assertTrue(all(r["naive_dates"] == naive for r in scenarios))

    def test_collision_in_last_run_stops_before_any_changes(self):
        result = self.run_demo(existing=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("s06-demo-naive", result.stderr)
        self.assertIn("--fresh", result.stderr)

    def test_fresh_cleans_only_demo_lists_and_log_appends_verbatim_json(self):
        log = self.root / "output with spaces/demo.jsonl"
        first = self.run_demo("--fresh", "--log", str(log), existing=True)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(log.read_text(), first.stdout)
        cleans = [r for r in map(json.loads, first.stdout.splitlines()) if r["step"] == "clean"]
        self.assertEqual(len(cleans), 6)
        for record in cleans:
            prefix = "lake.lesson06." + record["run_id"].replace("-", "_") + "_"
            self.assertEqual(record["tables"], [prefix + suffix for suffix in
                             ("orders", "payments", "refunds", "gold_order", "gold_daily", "progress", "plan")])
        second = self.run_demo("--fresh", "--log", str(log), existing=True)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(log.read_text(), first.stdout + second.stdout)

    def test_help_and_bad_arguments_do_not_run_demo(self):
        result = self.run_demo("--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn("사용법", result.stdout)
        for args in (("--log",), ("--unknown",)):
            result = self.run_demo(*args)
            self.assertEqual(result.returncode, 2)
            self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
