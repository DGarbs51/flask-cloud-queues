"""Ship main to every environment: merge it into each environment's branch, push, and wait for the deploys.

Run: uv run python scripts/ship.py [env ...]
Environments named k6-* (load tests) are skipped unless named on the command line.
Merges origin/main into each branch in a scratch worktree, so your checkout is untouched. On a conflict main
wins, except for the branch's own .python-version or .web-server; anything else (e.g. a deleted file) stops
the run. Then waits until every environment's latest deployment is at its branch head and finished, and exits 1
if any failed or is still running after 30 minutes.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time

APP = "app-a2f7dab8-e81a-4ae7-870c-84493946fe9c"
FINISHED = ("deployment.succeeded", "deployment.failed", "build.failed", "deployment.cancelled")


def cloud(*args: str):
    for attempt in range(6):
        out = subprocess.run(["cpx", "laravel/cloud-cli", *args, "--json", "-n"], capture_output=True, text=True)
        if not out.returncode:
            return json.loads(out.stdout)
        time.sleep(15 * (attempt + 1))  # mostly "Too Many Attempts": the CLI is rate limited per account
    raise SystemExit(f"{args[0]} failed: {out.stdout[-300:]} {out.stderr[-300:]}")


def git(*args: str, cwd: str | None = None, check: bool = True) -> subprocess.CompletedProcess:
    out = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)
    if check and out.returncode:
        raise SystemExit(f"git {' '.join(args)} failed: {out.stdout}{out.stderr}")
    return out


def merge(branch: str, tree: str) -> str:
    """Merge origin/main into origin/<branch> and push it. Returns the branch's new head."""
    own = ".python-version" if branch.startswith("python-") else ".web-server"
    git("checkout", "-q", "--detach", f"origin/{branch}", cwd=tree)
    if git("merge", "--no-edit", "-m", f"Merge branch 'main' into {branch}", "origin/main", cwd=tree, check=False).returncode:
        conflicted = git("diff", "--name-only", "--diff-filter=U", cwd=tree).stdout.split()
        for path in conflicted:
            side = "--ours" if path == own else "--theirs"  # ours = the branch, theirs = main
            if git("checkout", side, "--", path, cwd=tree, check=False).returncode:
                git("merge", "--abort", cwd=tree)
                raise SystemExit(f"{branch}: can't resolve the conflict in {path}; merge main into it by hand")
            git("add", "--", path, cwd=tree)
        if not conflicted:
            raise SystemExit(f"{branch}: merge failed without a conflict: {git('status', cwd=tree).stdout}")
        git("commit", "-q", "--no-edit", cwd=tree)
        print(f"{branch}: resolved {', '.join(conflicted)} (kept the branch's {own}, main's for the rest)", flush=True)
    git("push", "-q", "origin", f"HEAD:refs/heads/{branch}", cwd=tree)
    return git("rev-parse", "HEAD", cwd=tree).stdout.strip()


def selected(name: str, named: list[str]) -> bool:
    return name in named if named else not name.startswith("k6-")  # k6-* only when named: a run may be in progress


def main() -> None:
    envs = {e["name"]: (e["id"], e["branch"]) for e in cloud("env:list", APP) if selected(e["name"], sys.argv[1:])}
    git("fetch", "-q", "origin")
    tree = tempfile.mkdtemp(prefix="ship-")
    git("worktree", "add", "-q", "--detach", tree, "origin/main")
    try:
        heads = {branch: merge(branch, tree) for _, branch in envs.values()}
    finally:
        git("worktree", "remove", "--force", tree)
    for branch, head in heads.items():
        print(f"{branch}: pushed {head[:7]}", flush=True)

    pending, failed = dict(envs), []
    deadline = time.time() + 1800
    while pending and time.time() < deadline:
        for name, (env_id, branch) in list(pending.items()):
            deployments = cloud("deployment:list", env_id)
            if not deployments:
                continue  # a new environment: its first deployment hasn't been created yet
            latest = max(deployments, key=lambda d: d.get("createdAt") or d.get("startedAt") or "")
            if not heads[branch].startswith(latest.get("commitHash") or "-"):
                continue  # the push hasn't started a deployment yet
            if latest["status"] in FINISHED:
                print(f"{name}: {latest['status']} {heads[branch][:7]}", flush=True)
                if latest["status"] != "deployment.succeeded":
                    failed.append(name)
                del pending[name]
        if pending:
            time.sleep(30)
    for name in pending:
        print(f"{name}: still running after 30 minutes", flush=True)
    sys.exit(1 if failed or pending else 0)


if __name__ == "__main__":
    main()
