"""Tests for gobs CLI."""

import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from gobs import cli


def _make_git_repo(path: str, name: str = "test-repo") -> str:
	repo = os.path.join(path, name)
	os.makedirs(repo)
	subprocess.run(["git", "init", repo], capture_output=True, check=True)
	subprocess.run(
		["git", "-C", repo, "commit", "--allow-empty", "-m", "init"],
		capture_output=True, check=True,
		env={**os.environ, "GIT_AUTHOR_NAME": "Test", "GIT_AUTHOR_EMAIL": "t@t",
			"GIT_COMMITTER_NAME": "Test", "GIT_COMMITTER_EMAIL": "t@t"},
	)
	return repo


class GobsTestCase(unittest.TestCase):
	def setUp(self):
		self.tmpdir = tempfile.mkdtemp()
		self.db_dir = os.path.join(self.tmpdir, "db")
		os.makedirs(self.db_dir)
		self.db_path = os.path.join(self.db_dir, "repos.db")
		self._patches = [
			patch.object(cli, "DB_DIR", Path(self.db_dir)),
			patch.object(cli, "DB_PATH", Path(self.db_path)),
			patch.object(cli, "OLD_DB_DIR", Path(os.path.join(self.tmpdir, "old-db"))),
		]
		for p in self._patches:
			p.start()

	def tearDown(self):
		for p in self._patches:
			p.stop()
		shutil.rmtree(self.tmpdir)

	def _get_db(self):
		return cli.get_db()


class TestExportImportRoundtrip(GobsTestCase):
	def test_roundtrip(self):
		repo_path = _make_git_repo(self.tmpdir, "my-project")
		db = self._get_db()
		cli.update_repo(db, repo_path)
		db.execute("UPDATE repositories SET description = ? WHERE path = ?", ("A test project", repo_path))
		db.execute("INSERT OR IGNORE INTO tags (repo_id, tag) VALUES (1, 'active')")
		db.execute("INSERT OR IGNORE INTO tags (repo_id, tag) VALUES (1, 'work')")
		db.commit()
		db.close()

		export_file = os.path.join(self.tmpdir, "export.json")
		args = type("Args", (), {"file": export_file})()
		cli.cmd_export(args)

		data = json.loads(Path(export_file).read_text())
		self.assertEqual(data["version"], 1)
		self.assertEqual(len(data["repositories"]), 1)
		self.assertEqual(data["repositories"][0]["description"], "A test project")
		self.assertIn("active", data["repositories"][0]["tags"])
		self.assertIn("work", data["repositories"][0]["tags"])

		# Clear descriptions and tags
		db = self._get_db()
		db.execute("UPDATE repositories SET description = ''")
		db.execute("DELETE FROM tags")
		db.commit()
		db.close()

		args = type("Args", (), {"file": export_file})()
		cli.cmd_import(args)

		db = self._get_db()
		row = db.execute("SELECT description FROM repositories WHERE path = ?", (repo_path,)).fetchone()
		self.assertEqual(row["description"], "A test project")
		tags = [t["tag"] for t in db.execute("SELECT tag FROM tags ORDER BY tag").fetchall()]
		self.assertEqual(tags, ["active", "work"])
		db.close()


class TestImportSkipsUnmatched(GobsTestCase):
	def test_skip_count(self):
		export_data = {
			"version": 1,
			"exported_at": "2026-01-01T00:00:00+00:00",
			"repositories": [
				{"path": "/nonexistent/repo", "name": "ghost", "description": "gone", "tags": ["old"]},
			],
		}
		export_file = os.path.join(self.tmpdir, "export.json")
		Path(export_file).write_text(json.dumps(export_data))

		# Ensure the DB exists (empty)
		db = self._get_db()
		db.close()

		import io
		from contextlib import redirect_stdout
		f = io.StringIO()
		args = type("Args", (), {"file": export_file})()
		with redirect_stdout(f):
			cli.cmd_import(args)
		self.assertIn("skipped 1", f.getvalue())


class TestImportMergesTags(GobsTestCase):
	def test_union_tags(self):
		repo_path = _make_git_repo(self.tmpdir, "tagged-repo")
		db = self._get_db()
		cli.update_repo(db, repo_path)
		db.execute("INSERT OR IGNORE INTO tags (repo_id, tag) VALUES (1, 'existing')")
		db.commit()
		db.close()

		export_data = {
			"version": 1,
			"exported_at": "2026-01-01T00:00:00+00:00",
			"repositories": [
				{"path": repo_path, "name": "tagged-repo", "description": "", "tags": ["existing", "new"]},
			],
		}
		export_file = os.path.join(self.tmpdir, "export.json")
		Path(export_file).write_text(json.dumps(export_data))

		args = type("Args", (), {"file": export_file})()
		cli.cmd_import(args)

		db = self._get_db()
		tags = sorted(t["tag"] for t in db.execute("SELECT tag FROM tags").fetchall())
		self.assertEqual(tags, ["existing", "new"])
		db.close()


class TestGcRemovesStaleRepos(GobsTestCase):
	def test_gc_removes_stale(self):
		repo_path = _make_git_repo(self.tmpdir, "doomed")
		db = self._get_db()
		cli.update_repo(db, repo_path)
		db.close()

		shutil.rmtree(repo_path)

		args = type("Args", (), {"yes": True})()
		cli.cmd_gc(args)

		db = self._get_db()
		count = db.execute("SELECT COUNT(*) as c FROM repositories").fetchone()["c"]
		self.assertEqual(count, 0)
		db.close()


class TestGcVacuums(GobsTestCase):
	def test_db_size_stable(self):
		repo_path = _make_git_repo(self.tmpdir, "bulky")
		db = self._get_db()
		cli.update_repo(db, repo_path)
		# Insert junk to inflate the DB
		for i in range(200):
			db.execute(
				"INSERT INTO status_snapshots (repo_id, branch) VALUES (1, ?)",
				(f"branch-{i}",),
			)
		db.commit()
		db.close()

		size_before = os.path.getsize(self.db_path)

		args = type("Args", (), {"yes": True})()
		cli.cmd_gc(args)

		size_after = os.path.getsize(self.db_path)
		self.assertLessEqual(size_after, size_before)


class TestSnapshotCap(GobsTestCase):
	def test_prune_caps_at_limit(self):
		repo_path = _make_git_repo(self.tmpdir, "snappy")
		db = self._get_db()
		cli.update_repo(db, repo_path)
		for i in range(150):
			db.execute(
				"INSERT INTO status_snapshots (repo_id, branch, captured_at) VALUES (1, 'main', ?)",
				(f"2026-01-01T{i:05d}",),
			)
		db.commit()

		cli.prune_snapshots(db)
		db.commit()

		count = db.execute("SELECT COUNT(*) as c FROM status_snapshots WHERE repo_id = 1").fetchone()["c"]
		self.assertLessEqual(count, cli.SNAPSHOTS_PER_REPO)
		db.close()


class TestDbMigration(GobsTestCase):
	def test_migration_moves_old_dir(self):
		# Remove the new db dir that setUp created
		shutil.rmtree(self.db_dir)

		old_dir = os.path.join(self.tmpdir, "old-db")
		os.makedirs(old_dir)
		# Create a dummy DB at the old path
		old_db_path = os.path.join(old_dir, "repos.db")
		conn = sqlite3.connect(old_db_path)
		conn.execute("CREATE TABLE test_marker (id INTEGER)")
		conn.close()

		db = self._get_db()
		# Verify the old dir was moved to the new location
		self.assertTrue(os.path.exists(self.db_path))
		self.assertFalse(os.path.exists(old_dir))
		# Verify the marker table survived the move
		tables = [r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
		self.assertIn("test_marker", tables)
		db.close()


if __name__ == "__main__":
	unittest.main()
