# gobs

Track all your git repos in a single SQLite database. Get a bird's-eye view of what's where, what state things are in, and when you last touched each project.

```
$ gobs status
REPO                     DIR          BRANCH   LANG        MOD  UNT  +  -    STASH  LAST COMMIT  LAST EDIT
───────────────────────  ───────────  ───────  ──────────  ───  ───  ─  ───  ─────  ───────────  ─────────
svelte_app               ~/projects   main     TypeScript  0    0    0  0    0      28m ago      36m ago
rust-engine-rts          ~/projects   3d-test  Rust        14   5    0  0    1      7d ago       7d ago
tenx_mode                ~/workspace  main     Python      6    7    0  0    0      88d ago      74d ago
firecracker              ~/workspace  main     Rust        0    0    0  297  0      121d ago     120d ago
```

## Requirements

- macOS
- Python 3.13+
- [uv](https://docs.astral.sh/uv/)
- git

## Install

```sh
uv tool install git+https://github.com/irelandmi/project-gobs
```

Or from a local clone:

```sh
git clone https://github.com/irelandmi/project-gobs
uv tool install ./project-gobs
```

This installs `gobs` as a global command.

## Configuration

Set `GOBS_WORKSPACE` to change the default scan directory (defaults to `~`):

```sh
export GOBS_WORKSPACE=~/workspace
```

## Quick start

```sh
# Scan for repos (defaults to ~ or GOBS_WORKSPACE)
gobs scan

# Scan specific directories
gobs scan ~/projects ~/work

# See everything at a glance
gobs status

# Detailed view of a repo
gobs show my-project

# Add a description
gobs describe my-project "REST API for the dashboard"

# Tag repos
gobs tag my-project active
gobs tag my-project --remove active
```

## Commands

| Command | Purpose |
|---|---|
| `gobs scan [paths...]` | Recursively find git repos, register and update each |
| `gobs update [path]` | Refresh status/commits for one repo (default: cwd) |
| `gobs status` | Table of all repos with branch, status, and activity |
| `gobs show <repo>` | Detailed view: description, tags, status, recent commits |
| `gobs describe <repo> <text>` | Set project description |
| `gobs tag <repo> <tag>` | Add a tag (`--remove` to delete) |
| `gobs export [file]` | Export descriptions and tags to JSON (stdout if no file) |
| `gobs import <file>` | Import descriptions and tags from JSON |
| `gobs gc` | Remove stale repos, prune snapshots, vacuum (`--yes` to skip prompt) |
| `gobs query <sql>` | Run read-only SQL against the database |

### Sorting

`gobs status` sorts by last file edit (most recent first) by default. Use `--sort` to change:

```sh
gobs status --sort edit     # last file modification (default)
gobs status --sort commit   # last commit date
gobs status --sort name     # alphabetical
gobs status --sort lang     # by language
gobs status --sort mod      # by modified file count
```

Filter by tag:

```sh
gobs status --tag active    # only repos tagged "active"
```

### Ad-hoc queries

```sh
gobs query "SELECT name, description FROM repositories WHERE primary_language = 'Rust'"
gobs query "SELECT r.name, t.tag FROM repositories r JOIN tags t ON t.repo_id = r.id"
```

## Auto-update with Claude Code

Add a `Stop` hook to `~/.claude/settings.json` to refresh the current repo after every Claude turn:

```json
{
  "hooks": {
    "Stop": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "gobs update --quiet 2>/dev/null",
            "timeout": 10
          }
        ]
      }
    ]
  }
}
```

## Shell alias (optional)

Add to your `~/.zshrc` to scan on shell startup:

```sh
gobs scan --quiet &!
```

## Data

The SQLite database is stored at `~/.local/share/gobs/repos.db`. WAL mode is enabled for safe concurrent access. If upgrading from `project-obs`, the database directory is automatically migrated on first run.

Tables:
- **repositories** — one row per repo (path, branch, language, description, etc.)
- **commits** — last 50 commits per repo
- **status_snapshots** — append-only status history, pruned after 30 days
- **tags** — free-form labels per repo

Access it directly:

```sh
sqlite3 ~/.local/share/gobs/repos.db "SELECT name, current_branch FROM repositories"
```
