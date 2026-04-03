#!/usr/bin/env python3
"""gobs — track all git repos in a single SQLite database."""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

OLD_DB_DIR = Path.home() / ".local" / "share" / "project-obs"
DB_DIR = Path.home() / ".local" / "share" / "gobs"
DB_PATH = DB_DIR / "repos.db"
DEFAULT_WORKSPACE = os.environ.get("GOBS_WORKSPACE", str(Path.home()))
MAX_SCAN_DEPTH = 4
COMMITS_TO_KEEP = 50
SNAPSHOT_RETENTION_DAYS = 30
SNAPSHOTS_PER_REPO = 100
MAX_FILE_LIST = 100

SCHEMA = """
CREATE TABLE IF NOT EXISTS repositories (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	path TEXT UNIQUE NOT NULL,
	name TEXT NOT NULL,
	description TEXT DEFAULT '',
	remote_url TEXT DEFAULT '',
	default_branch TEXT DEFAULT '',
	current_branch TEXT DEFAULT '',
	primary_language TEXT DEFAULT '',
	total_commits INTEGER DEFAULT 0,
	is_shallow INTEGER DEFAULT 0,
	last_edited TEXT DEFAULT '',
	last_updated TEXT,
	created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS commits (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	repo_id INTEGER NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
	hash TEXT NOT NULL,
	author TEXT NOT NULL,
	author_email TEXT DEFAULT '',
	date TEXT NOT NULL,
	message TEXT NOT NULL,
	UNIQUE(repo_id, hash)
);

CREATE TABLE IF NOT EXISTS status_snapshots (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	repo_id INTEGER NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
	captured_at TEXT DEFAULT (datetime('now')),
	branch TEXT DEFAULT '',
	ahead INTEGER DEFAULT 0,
	behind INTEGER DEFAULT 0,
	staged_count INTEGER DEFAULT 0,
	modified_count INTEGER DEFAULT 0,
	untracked_count INTEGER DEFAULT 0,
	staged_files TEXT DEFAULT '[]',
	modified_files TEXT DEFAULT '[]',
	untracked_files TEXT DEFAULT '[]',
	has_conflicts INTEGER DEFAULT 0,
	stash_count INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS tags (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	repo_id INTEGER NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
	tag TEXT NOT NULL,
	UNIQUE(repo_id, tag)
);
"""


# ── Database ──────────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
	if OLD_DB_DIR.exists() and not DB_DIR.exists():
		os.rename(str(OLD_DB_DIR), str(DB_DIR))
		print(f"migrated database: {OLD_DB_DIR} → {DB_DIR}", file=sys.stderr)
	DB_DIR.mkdir(parents=True, exist_ok=True)
	db = sqlite3.connect(str(DB_PATH), timeout=5)
	db.execute("PRAGMA journal_mode=WAL")
	db.execute("PRAGMA foreign_keys=ON")
	db.row_factory = sqlite3.Row
	db.executescript(SCHEMA)
	return db


# ── Git helpers ───────────────────────────────────────────────────────

def git_run(repo_path: str, *args: str, timeout: int = 10) -> str | None:
	try:
		r = subprocess.run(
			["git", "-C", repo_path, *args],
			capture_output=True, text=True, timeout=timeout,
		)
		return r.stdout.strip() if r.returncode == 0 else None
	except (subprocess.TimeoutExpired, FileNotFoundError):
		return None


def is_git_repo(path: str) -> bool:
	return git_run(path, "rev-parse", "--git-dir") is not None


def find_git_root(path: str) -> str | None:
	result = git_run(path, "rev-parse", "--show-toplevel")
	return result


def get_repo_info(repo_path: str) -> dict:
	name = os.path.basename(repo_path)
	remote_url = git_run(repo_path, "remote", "get-url", "origin") or ""
	default_branch = _detect_default_branch(repo_path)
	current_branch = git_run(repo_path, "rev-parse", "--abbrev-ref", "HEAD") or ""
	total_commits_str = git_run(repo_path, "rev-list", "--count", "HEAD")
	total_commits = int(total_commits_str) if total_commits_str else 0
	is_shallow = os.path.exists(os.path.join(repo_path, ".git", "shallow"))
	language = detect_language(repo_path)
	last_edited = get_last_edited(repo_path)
	return {
		"name": name,
		"remote_url": remote_url,
		"default_branch": default_branch,
		"current_branch": current_branch,
		"total_commits": total_commits,
		"is_shallow": int(is_shallow),
		"primary_language": language,
		"last_edited": last_edited,
	}


def _detect_default_branch(repo_path: str) -> str:
	ref = git_run(repo_path, "symbolic-ref", "refs/remotes/origin/HEAD")
	if ref:
		return ref.split("/")[-1]
	for candidate in ("main", "master"):
		if git_run(repo_path, "rev-parse", "--verify", f"refs/heads/{candidate}") is not None:
			return candidate
	return git_run(repo_path, "rev-parse", "--abbrev-ref", "HEAD") or "main"


def get_recent_commits(repo_path: str, n: int = COMMITS_TO_KEEP) -> list[dict]:
	fmt = "%H%x00%an%x00%ae%x00%aI%x00%s"
	raw = git_run(repo_path, "log", f"--max-count={n}", f"--format={fmt}")
	if not raw:
		return []
	commits = []
	for line in raw.splitlines():
		parts = line.split("\x00")
		if len(parts) < 5:
			continue
		commits.append({
			"hash": parts[0],
			"author": parts[1],
			"author_email": parts[2],
			"date": parts[3],
			"message": parts[4],
		})
	return commits


def get_status(repo_path: str) -> dict:
	branch = git_run(repo_path, "rev-parse", "--abbrev-ref", "HEAD") or ""
	ahead, behind = 0, 0
	staged, modified, untracked = [], [], []
	has_conflicts = False

	raw = git_run(repo_path, "status", "--porcelain=v2", "--branch")
	if raw:
		for line in raw.splitlines():
			if line.startswith("# branch.ab"):
				parts = line.split()
				for p in parts:
					if p.startswith("+"):
						try: ahead = int(p)
						except ValueError: pass
					elif p.startswith("-"):
						try: behind = abs(int(p))
						except ValueError: pass
			elif line.startswith("1 ") or line.startswith("2 "):
				xy = line.split()[1]
				fname = line.split()[-1]
				if xy[0] != ".":
					staged.append(fname)
				if xy[1] != ".":
					modified.append(fname)
			elif line.startswith("u "):
				has_conflicts = True
				modified.append(line.split()[-1])
			elif line.startswith("? "):
				untracked.append(line[2:])

	stash_raw = git_run(repo_path, "stash", "list")
	stash_count = len(stash_raw.splitlines()) if stash_raw else 0

	return {
		"branch": branch,
		"ahead": ahead,
		"behind": behind,
		"staged_count": len(staged),
		"modified_count": len(modified),
		"untracked_count": len(untracked),
		"staged_files": json.dumps(staged[:MAX_FILE_LIST]),
		"modified_files": json.dumps(modified[:MAX_FILE_LIST]),
		"untracked_files": json.dumps(untracked[:MAX_FILE_LIST]),
		"has_conflicts": int(has_conflicts),
		"stash_count": stash_count,
	}


IGNORE_PATTERNS = {".DS_Store", "Thumbs.db", ".env"}
IGNORE_EXTENSIONS = {".log", ".pyc", ".pyo", ".class", ".o", ".so", ".dylib"}


IGNORE_DIRS = {".git", "__pycache__", "node_modules", ".venv", "venv", "target", "build", "dist"}


def get_last_edited(repo_path: str) -> str:
	raw = git_run(repo_path, "ls-files")
	if raw:
		files = raw.splitlines()
	else:
		files = _walk_files(repo_path)
	latest = 0.0
	for f in files:
		basename = os.path.basename(f)
		if basename in IGNORE_PATTERNS:
			continue
		ext = os.path.splitext(f)[1].lower()
		if ext in IGNORE_EXTENSIONS:
			continue
		full = f if os.path.isabs(f) else os.path.join(repo_path, f)
		try:
			mtime = os.path.getmtime(full)
			if mtime > latest:
				latest = mtime
		except OSError:
			continue
	if latest == 0.0:
		return ""
	return datetime.fromtimestamp(latest, tz=timezone.utc).isoformat()


def _walk_files(repo_path: str) -> list[str]:
	files = []
	for dirpath, dirnames, filenames in os.walk(repo_path):
		dirnames[:] = [d for d in dirnames if d not in IGNORE_DIRS]
		for f in filenames:
			files.append(os.path.join(dirpath, f))
	return files


LANG_EXTENSIONS = {
	".rs": "Rust", ".py": "Python", ".ts": "TypeScript", ".tsx": "TypeScript",
	".js": "JavaScript", ".jsx": "JavaScript", ".go": "Go", ".java": "Java",
	".c": "C", ".cpp": "C++", ".cs": "C#", ".rb": "Ruby", ".swift": "Swift",
	".kt": "Kotlin", ".zig": "Zig", ".lua": "Lua", ".ex": "Elixir",
	".hs": "Haskell", ".ml": "OCaml", ".scala": "Scala", ".dart": "Dart",
}

LANG_FILES = {
	"Cargo.toml": "Rust", "pyproject.toml": "Python", "setup.py": "Python",
	"package.json": "TypeScript", "tsconfig.json": "TypeScript",
	"go.mod": "Go", "pom.xml": "Java", "build.gradle": "Java",
	"Gemfile": "Ruby", "Package.swift": "Swift",
}


def detect_language(repo_path: str) -> str:
	for fname, lang in LANG_FILES.items():
		if os.path.exists(os.path.join(repo_path, fname)):
			return lang

	raw = git_run(repo_path, "ls-files")
	if not raw:
		return ""
	counts: dict[str, int] = {}
	for f in raw.splitlines():
		ext = os.path.splitext(f)[1].lower()
		if ext in LANG_EXTENSIONS:
			lang = LANG_EXTENSIONS[ext]
			counts[lang] = counts.get(lang, 0) + 1
	if not counts:
		return ""
	return max(counts, key=counts.get)


# ── DB operations ────────────────────────────────────────────────────

def upsert_repo(db: sqlite3.Connection, repo_path: str, info: dict) -> int:
	now = datetime.now(timezone.utc).isoformat()
	db.execute("""
		INSERT INTO repositories (path, name, remote_url, default_branch, current_branch,
			primary_language, total_commits, is_shallow, last_edited, last_updated)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET
			name=excluded.name, remote_url=excluded.remote_url,
			default_branch=excluded.default_branch, current_branch=excluded.current_branch,
			primary_language=excluded.primary_language, total_commits=excluded.total_commits,
			is_shallow=excluded.is_shallow, last_edited=excluded.last_edited,
			last_updated=excluded.last_updated
	""", (repo_path, info["name"], info["remote_url"], info["default_branch"],
		info["current_branch"], info["primary_language"], info["total_commits"],
		info["is_shallow"], info["last_edited"], now))
	row = db.execute("SELECT id FROM repositories WHERE path = ?", (repo_path,)).fetchone()
	return row["id"]


def insert_commits(db: sqlite3.Connection, repo_id: int, commits: list[dict]):
	for c in commits:
		db.execute("""
			INSERT OR IGNORE INTO commits (repo_id, hash, author, author_email, date, message)
			VALUES (?, ?, ?, ?, ?, ?)
		""", (repo_id, c["hash"], c["author"], c["author_email"], c["date"], c["message"]))
	db.execute("""
		DELETE FROM commits WHERE id NOT IN (
			SELECT id FROM commits WHERE repo_id = ?
			ORDER BY date DESC LIMIT ?
		) AND repo_id = ?
	""", (repo_id, COMMITS_TO_KEEP, repo_id))


def insert_status(db: sqlite3.Connection, repo_id: int, status: dict):
	db.execute("""
		INSERT INTO status_snapshots (repo_id, branch, ahead, behind, staged_count,
			modified_count, untracked_count, staged_files, modified_files, untracked_files,
			has_conflicts, stash_count)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	""", (repo_id, status["branch"], status["ahead"], status["behind"],
		status["staged_count"], status["modified_count"], status["untracked_count"],
		status["staged_files"], status["modified_files"], status["untracked_files"],
		status["has_conflicts"], status["stash_count"]))


def prune_snapshots(db: sqlite3.Connection):
	cutoff = (datetime.now(timezone.utc) - timedelta(days=SNAPSHOT_RETENTION_DAYS)).isoformat()
	db.execute("DELETE FROM status_snapshots WHERE captured_at < ?", (cutoff,))
	db.execute("""
		DELETE FROM status_snapshots WHERE id NOT IN (
			SELECT id FROM (
				SELECT id, ROW_NUMBER() OVER (
					PARTITION BY repo_id ORDER BY captured_at DESC
				) AS rn
				FROM status_snapshots
			) WHERE rn <= ?
		)
	""", (SNAPSHOTS_PER_REPO,))


def update_repo(db: sqlite3.Connection, repo_path: str) -> int | None:
	if not is_git_repo(repo_path):
		return None
	info = get_repo_info(repo_path)
	repo_id = upsert_repo(db, repo_path, info)
	commits = get_recent_commits(repo_path)
	insert_commits(db, repo_id, commits)
	status = get_status(repo_path)
	insert_status(db, repo_id, status)
	prune_snapshots(db)
	db.commit()
	return repo_id


# ── CLI commands ─────────────────────────────────────────────────────

def cmd_scan(args):
	paths = args.paths or [DEFAULT_WORKSPACE]
	db = get_db()
	found = 0
	for base in paths:
		base = os.path.expanduser(base)
		if not os.path.isdir(base):
			print(f"skip: {base} (not a directory)")
			continue
		for repo_path in _find_repos(base, MAX_SCAN_DEPTH):
			repo_path = os.path.realpath(repo_path)
			if not args.quiet:
				print(f"  {repo_path}")
			update_repo(db, repo_path)
			found += 1
	stale = [r for r in db.execute("SELECT id, path, name FROM repositories").fetchall()
		if not os.path.isdir(r["path"])]
	for r in stale:
		db.execute("DELETE FROM repositories WHERE id = ?", (r["id"],))
		if not args.quiet:
			print(f"  removed: {r['name']} ({r['path']})")
	if stale:
		prune_snapshots(db)
		db.commit()
		db.execute("VACUUM")

	db.close()
	if not args.quiet:
		if stale:
			print(f"removed {len(stale)} stale repos")
		print(f"scanned {found} repositories")


def _find_repos(base: str, max_depth: int) -> list[str]:
	repos = []
	base = os.path.realpath(base)
	base_depth = base.count(os.sep)
	for dirpath, dirnames, _ in os.walk(base):
		depth = dirpath.count(os.sep) - base_depth
		if depth >= max_depth:
			dirnames.clear()
			continue
		if ".git" in dirnames:
			repos.append(dirpath)
			dirnames.clear()
	return sorted(repos)


def cmd_update(args):
	repo_path = os.path.realpath(args.path or os.getcwd())
	if not args.path and not sys.stdin.isatty():
		try:
			hook_data = json.load(sys.stdin)
			if "cwd" in hook_data:
				repo_path = os.path.realpath(hook_data["cwd"])
		except (json.JSONDecodeError, ValueError):
			pass

	root = find_git_root(repo_path)
	if not root:
		if not args.quiet:
			print(f"not a git repo: {repo_path}")
		return
	repo_path = os.path.realpath(root)

	db = get_db()
	repo_id = update_repo(db, repo_path)
	db.close()
	if not args.quiet and repo_id:
		print(f"updated: {repo_path}")


def cmd_status(args):
	db = get_db()
	query = """
		SELECT r.name, r.path, r.current_branch, r.primary_language,
			s.modified_count, s.untracked_count, s.ahead, s.behind, s.stash_count,
			(SELECT max(date) FROM commits WHERE repo_id = r.id) AS last_commit,
			r.last_edited
		FROM repositories r
		LEFT JOIN status_snapshots s ON s.id = (
			SELECT id FROM status_snapshots WHERE repo_id = r.id
			ORDER BY captured_at DESC LIMIT 1
		)
	"""
	params = []
	if args.tag:
		placeholders = ", ".join("?" for _ in args.tag)
		query += f"""
		WHERE r.id IN (
			SELECT repo_id FROM tags WHERE tag IN ({placeholders})
			GROUP BY repo_id HAVING COUNT(DISTINCT tag) = ?
		)"""
		params.extend(args.tag)
		params.append(len(args.tag))
	rows = db.execute(query, params).fetchall()
	db.close()

	if not rows:
		print("no repositories tracked yet — run: gobs scan")
		return

	sort_keys = {
		"edit": lambda r: r["last_edited"] or "",
		"commit": lambda r: r["last_commit"] or "",
		"name": lambda r: (r["name"] or "").lower(),
		"lang": lambda r: (r["primary_language"] or "").lower(),
		"mod": lambda r: r["modified_count"] or 0,
	}
	key_fn = sort_keys[args.sort]
	reverse = args.sort != "name" and args.sort != "lang"
	sorted_rows = sorted(rows, key=key_fn, reverse=reverse)

	headers = ["REPO", "DIR", "BRANCH", "LANG", "MOD", "UNT", "+", "-", "STASH", "LAST COMMIT", "LAST EDIT"]
	table = []
	for r in sorted_rows:
		last_commit = _relative_time(r["last_commit"]) if r["last_commit"] else "never"
		last_edited = _relative_time(r["last_edited"]) if r["last_edited"] else "never"
		table.append([
			r["name"] or "",
			os.path.basename(os.path.dirname(r["path"])) if r["path"] else "",
			r["current_branch"] or "",
			r["primary_language"] or "",
			str(r["modified_count"] or 0),
			str(r["untracked_count"] or 0),
			str(r["ahead"] or 0),
			str(r["behind"] or 0),
			str(r["stash_count"] or 0),
			last_commit,
			last_edited,
		])
	_print_table(headers, table)


def cmd_show(args):
	db = get_db()
	repo = _resolve_repo(db, args.repo)
	if not repo:
		print(f"repo not found: {args.repo}")
		db.close()
		return

	tags = db.execute("SELECT tag FROM tags WHERE repo_id = ? ORDER BY tag", (repo["id"],)).fetchall()
	snap = db.execute("""
		SELECT * FROM status_snapshots WHERE repo_id = ?
		ORDER BY captured_at DESC LIMIT 1
	""", (repo["id"],)).fetchone()
	commits = db.execute("""
		SELECT hash, author, date, message FROM commits WHERE repo_id = ?
		ORDER BY date DESC LIMIT 10
	""", (repo["id"],)).fetchall()
	db.close()

	print(f"  {repo['name']}")
	print(f"  path: {repo['path']}")
	if repo["description"]:
		print(f"  desc: {repo['description']}")
	if repo["remote_url"]:
		print(f"  remote: {repo['remote_url']}")
	print(f"  branch: {repo['current_branch']} (default: {repo['default_branch']})")
	print(f"  language: {repo['primary_language'] or 'unknown'}")
	print(f"  commits: {repo['total_commits']}")
	if tags:
		print(f"  tags: {', '.join(t['tag'] for t in tags)}")
	if repo["last_updated"]:
		print(f"  updated: {_relative_time(repo['last_updated'])}")

	if snap:
		print(f"\n  status:")
		parts = []
		if snap["staged_count"]:
			parts.append(f"{snap['staged_count']} staged")
		if snap["modified_count"]:
			parts.append(f"{snap['modified_count']} modified")
		if snap["untracked_count"]:
			parts.append(f"{snap['untracked_count']} untracked")
		if snap["ahead"]:
			parts.append(f"+{snap['ahead']} ahead")
		if snap["behind"]:
			parts.append(f"-{snap['behind']} behind")
		if snap["stash_count"]:
			parts.append(f"{snap['stash_count']} stashes")
		if snap["has_conflicts"]:
			parts.append("CONFLICTS")
		print(f"    {', '.join(parts) if parts else 'clean'}")

	if commits:
		print(f"\n  recent commits:")
		for c in commits:
			short = c["hash"][:7]
			date = c["date"][:10]
			msg = c["message"][:60]
			print(f"    {short} {date} {msg}")


def cmd_describe(args):
	db = get_db()
	repo = _resolve_repo(db, args.repo)
	if not repo:
		print(f"repo not found: {args.repo}")
		db.close()
		return
	db.execute("UPDATE repositories SET description = ? WHERE id = ?", (args.text, repo["id"]))
	db.commit()
	db.close()
	print(f"description set for {repo['name']}")


def cmd_tag(args):
	db = get_db()
	repo = _resolve_repo(db, args.repo)
	if not repo:
		print(f"repo not found: {args.repo}")
		db.close()
		return
	if args.remove:
		db.execute("DELETE FROM tags WHERE repo_id = ? AND tag = ?", (repo["id"], args.tag))
		print(f"removed tag '{args.tag}' from {repo['name']}")
	else:
		db.execute("INSERT OR IGNORE INTO tags (repo_id, tag) VALUES (?, ?)", (repo["id"], args.tag))
		print(f"tagged {repo['name']} with '{args.tag}'")
	db.commit()
	db.close()


def cmd_export(args):
	db = get_db()
	rows = db.execute("SELECT id, path, name, description FROM repositories").fetchall()

	repos = []
	for r in rows:
		tags = [t["tag"] for t in db.execute(
			"SELECT tag FROM tags WHERE repo_id = ? ORDER BY tag", (r["id"],)
		).fetchall()]
		if not r["description"] and not tags:
			continue
		repos.append({
			"path": r["path"],
			"name": r["name"],
			"description": r["description"] or "",
			"tags": tags,
		})
	db.close()

	data = {
		"version": 1,
		"exported_at": datetime.now(timezone.utc).isoformat(),
		"repositories": repos,
	}
	output = json.dumps(data, indent=2) + "\n"

	if args.file:
		Path(args.file).write_text(output)
		print(f"exported {len(repos)} repositories to {args.file}")
	else:
		sys.stdout.write(output)


def cmd_import(args):
	data = json.loads(Path(args.file).read_text())
	if data.get("version") != 1:
		print(f"error: unsupported export version: {data.get('version')}")
		return

	db = get_db()
	imported = 0
	skipped = 0

	for entry in data["repositories"]:
		repo = db.execute(
			"SELECT id FROM repositories WHERE path = ?", (entry["path"],)
		).fetchone()
		if not repo:
			repo = db.execute(
				"SELECT id FROM repositories WHERE name = ?", (entry["name"],)
			).fetchone()
		if not repo:
			skipped += 1
			continue

		repo_id = repo["id"]
		if entry.get("description"):
			db.execute(
				"UPDATE repositories SET description = ? WHERE id = ?",
				(entry["description"], repo_id),
			)
		for tag in entry.get("tags", []):
			db.execute(
				"INSERT OR IGNORE INTO tags (repo_id, tag) VALUES (?, ?)",
				(repo_id, tag),
			)
		imported += 1

	db.commit()
	db.close()
	print(f"imported {imported} repositories, skipped {skipped} unmatched")


def cmd_gc(args):
	db = get_db()
	repos = db.execute("SELECT id, path, name FROM repositories").fetchall()

	stale = [(r["id"], r["path"], r["name"]) for r in repos if not os.path.isdir(r["path"])]

	if stale and not args.yes:
		print("stale repositories (path no longer exists):")
		for _, path, name in stale:
			print(f"  {name} ({path})")
		answer = input(f"\nremove {len(stale)} stale repos? [y/N] ")
		if answer.lower() != "y":
			print("aborted")
			db.close()
			return

	db_size_before = os.path.getsize(str(DB_PATH))

	for repo_id, path, name in stale:
		db.execute("DELETE FROM repositories WHERE id = ?", (repo_id,))
		print(f"  removed: {name} ({path})")

	prune_snapshots(db)
	db.commit()
	db.execute("VACUUM")
	db.close()

	db_size_after = os.path.getsize(str(DB_PATH))
	print(f"removed {len(stale)} stale repos")
	print(f"db size: {_fmt_size(db_size_before)} → {_fmt_size(db_size_after)}")


def _fmt_size(n: int) -> str:
	for unit in ("B", "KB", "MB", "GB"):
		if n < 1024:
			return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
		n /= 1024
	return f"{n:.1f} TB"


def cmd_query(args):
	sql = args.sql.strip()
	if not sql.upper().startswith("SELECT"):
		print("error: only SELECT queries are allowed")
		return
	db = get_db()
	try:
		rows = db.execute(sql).fetchall()
	except sqlite3.Error as e:
		print(f"error: {e}")
		db.close()
		return
	db.close()
	if not rows:
		print("(no results)")
		return
	headers = list(rows[0].keys())
	table = [[str(r[h]) if r[h] is not None else "" for h in headers] for r in rows]
	_print_table(headers, table)


# ── Helpers ───────────────────────────────────────────────────────────

def _resolve_repo(db: sqlite3.Connection, name: str) -> sqlite3.Row | None:
	row = db.execute("SELECT * FROM repositories WHERE name = ?", (name,)).fetchone()
	if row:
		return row
	row = db.execute("SELECT * FROM repositories WHERE path = ?", (os.path.realpath(name),)).fetchone()
	if row:
		return row
	row = db.execute("SELECT * FROM repositories WHERE name LIKE ?", (name + "%",)).fetchone()
	return row


def _print_table(headers: list[str], rows: list[list[str]]):
	widths = [len(h) for h in headers]
	for row in rows:
		for i, val in enumerate(row):
			widths[i] = max(widths[i], len(val))
	fmt = "  ".join(f"{{:<{w}}}" for w in widths)
	print(fmt.format(*headers))
	print(fmt.format(*["─" * w for w in widths]))
	for row in rows:
		print(fmt.format(*row))


def _relative_time(iso_str: str) -> str:
	try:
		dt = datetime.fromisoformat(iso_str)
		if dt.tzinfo is None:
			dt = dt.replace(tzinfo=timezone.utc)
		now = datetime.now(timezone.utc)
		delta = now - dt
		secs = int(delta.total_seconds())
		if secs < 60:
			return "just now"
		if secs < 3600:
			m = secs // 60
			return f"{m}m ago"
		if secs < 86400:
			h = secs // 3600
			return f"{h}h ago"
		d = secs // 86400
		return f"{d}d ago"
	except (ValueError, TypeError):
		return iso_str


# ── Main ──────────────────────────────────────────────────────────────

def main():
	parser = argparse.ArgumentParser(prog="gobs", description="gobs — git observatory")
	sub = parser.add_subparsers(dest="command")

	p_scan = sub.add_parser("scan", help="discover and register git repos")
	p_scan.add_argument("paths", nargs="*", help="directories to scan (default: ~ or $GOBS_WORKSPACE)")
	p_scan.add_argument("--quiet", "-q", action="store_true")

	p_update = sub.add_parser("update", help="refresh status for one repo")
	p_update.add_argument("path", nargs="?", help="repo path (default: cwd)")
	p_update.add_argument("--quiet", "-q", action="store_true")

	p_status = sub.add_parser("status", help="table of all tracked repos")
	p_status.add_argument("--sort", "-s", default="edit",
		choices=["edit", "commit", "name", "lang", "mod"],
		help="sort by: edit (default), commit, name, lang, mod")
	p_status.add_argument("--tag", "-t", action="append", metavar="TAG",
		help="filter by tag (can be repeated)")

	p_show = sub.add_parser("show", help="detailed view of a repo")
	p_show.add_argument("repo", help="repo name or path")

	p_desc = sub.add_parser("describe", help="set repo description")
	p_desc.add_argument("repo", help="repo name or path")
	p_desc.add_argument("text", help="description text")

	p_tag = sub.add_parser("tag", help="add/remove a tag")
	p_tag.add_argument("repo", help="repo name or path")
	p_tag.add_argument("tag", help="tag name")
	p_tag.add_argument("--remove", "-r", action="store_true")

	p_export = sub.add_parser("export", help="export descriptions and tags")
	p_export.add_argument("file", nargs="?", help="output file (default: stdout)")

	p_import = sub.add_parser("import", help="import descriptions and tags")
	p_import.add_argument("file", help="JSON file to import")

	p_gc = sub.add_parser("gc", help="remove stale repos and vacuum")
	p_gc.add_argument("--yes", "-y", action="store_true", help="skip confirmation")

	p_query = sub.add_parser("query", help="run read-only SQL")
	p_query.add_argument("sql", help="SQL query")

	args = parser.parse_args()
	if not args.command:
		parser.print_help()
		return

	{
		"scan": cmd_scan,
		"update": cmd_update,
		"status": cmd_status,
		"show": cmd_show,
		"describe": cmd_describe,
		"tag": cmd_tag,
		"export": cmd_export,
		"import": cmd_import,
		"gc": cmd_gc,
		"query": cmd_query,
	}[args.command](args)


if __name__ == "__main__":
	main()
