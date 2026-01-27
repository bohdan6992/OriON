import os, shutil, time, tempfile, subprocess, json
from pathlib import Path
from typing import Optional, Dict, Any


def run_cmd(cmd, cwd: Path, env=None):
    p = subprocess.run(cmd, cwd=str(cwd), env=env, capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr


def read_github_token(orion_home: Path) -> str:
    """
    HARD RULE:
      GitHub token is ONLY read from:  OriON/ops/access_token.json
    This avoids конфликт with OriON/access_token.json that belongs to Datum API.

    Expected JSON:
      {"token":"..."}  OR  {"github_pat":"..."} OR {"pat":"..."}
    """
    access_json = (Path(orion_home) / "ops" / "access_token.json").resolve()
    if not access_json.exists():
        raise FileNotFoundError(f"Missing GitHub token file: {access_json}")

    data = json.loads(access_json.read_text(encoding="utf-8"))
    token = data.get("token") or data.get("github_pat") or data.get("pat")
    if not token or not isinstance(token, str):
        raise RuntimeError(
            f"GitHub token not found in {access_json}. "
            f"Expected keys: token/github_pat/pat"
        )
    return token.strip()


def with_token_https(url: str, token: Optional[str]) -> str:
    if not token:
        return url
    if url.startswith("https://"):
        return "https://" + token + "@" + url[len("https://"):]
    return url


def ls_remote_head_sha(repo_url: str, branch: str, token: Optional[str]) -> str:
    url = with_token_https(repo_url, token)
    code, so, se = run_cmd(["git", "ls-remote", url, f"refs/heads/{branch}"], cwd=Path(tempfile.gettempdir()))
    if code != 0:
        raise RuntimeError(f"git ls-remote failed: {se.strip() or so.strip()}")
    if not so.strip():
        raise RuntimeError("git ls-remote returned empty output")
    return so.strip().split()[0][:12]


def clone_depth1(repo_url: str, branch: str, dst: Path, token: Optional[str]):
    url = with_token_https(repo_url, token)
    if dst.exists():
        shutil.rmtree(dst, ignore_errors=True)
    code, so, se = run_cmd(
        ["git", "clone", "--depth", "1", "--branch", branch, url, str(dst)],
        cwd=Path(tempfile.gettempdir())
    )
    if code != 0:
        raise RuntimeError(f"git clone failed: {se.strip() or so.strip()}")


def update_dir_clone_swap(repo_url: str, branch: str, dst_dir: Path, token: Optional[str]) -> Dict[str, Any]:
    remote_sha = ls_remote_head_sha(repo_url, branch, token)

    local_sha = None
    if dst_dir.exists() and (dst_dir / ".git").exists():
        c, so, _ = run_cmd(["git", "rev-parse", "--short=12", "HEAD"], cwd=dst_dir)
        if c == 0:
            local_sha = so.strip()

    if local_sha == remote_sha:
        return {"updated": False, "sha": remote_sha, "error": None, "backup": None}

    tmp_dir = Path(tempfile.gettempdir()) / f"orion_swap_{dst_dir.name}_{remote_sha}_{int(time.time())}"
    clone_depth1(repo_url, branch, tmp_dir, token)

    backup_dir = None
    if dst_dir.exists():
        backup_dir = dst_dir.parent / f"{dst_dir.name}_backup_{local_sha or int(time.time())}"
        i = 0
        while backup_dir.exists():
            i += 1
            backup_dir = dst_dir.parent / f"{dst_dir.name}_backup_{local_sha or int(time.time())}_{i}"
        shutil.move(str(dst_dir), str(backup_dir))

    shutil.move(str(tmp_dir), str(dst_dir))
    return {"updated": True, "sha": remote_sha, "error": None, "backup": str(backup_dir) if backup_dir else None}


def ensure_repo_checkout(repo_url: str, branch: str, dst_dir: Path, token: Optional[str]) -> Dict[str, Any]:
    url = with_token_https(repo_url, token)
    if not dst_dir.exists():
        clone_depth1(repo_url, branch, dst_dir, token)
        return {"updated": True, "error": None}

    if not (dst_dir / ".git").exists():
        raise RuntimeError(f"Destination exists but not a git repo: {dst_dir}")

    code, so, se = run_cmd(["git", "fetch", "origin", branch], cwd=dst_dir)
    if code != 0:
        return {"updated": False, "error": se.strip() or so.strip()}

    code, so, se = run_cmd(["git", "checkout", branch], cwd=dst_dir)
    if code != 0:
        return {"updated": False, "error": se.strip() or so.strip()}

    code, so, se = run_cmd(["git", "pull", "origin", branch], cwd=dst_dir)
    if code != 0:
        return {"updated": False, "error": se.strip() or so.strip()}

    return {"updated": True, "error": None}


def copy_tree(src: Path, dst: Path):
    dst.mkdir(parents=True, exist_ok=True)
    for item in src.rglob("*"):
        rel = item.relative_to(src)
        out = dst / rel
        if item.is_dir():
            out.mkdir(parents=True, exist_ok=True)
        else:
            out.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, out)


def git_add_commit_push(repo_dir: Path, message: str, add_paths: list[str], branch: str = "main") -> Dict[str, Any]:
    out = {"pushed": False, "commit": None, "error": None}
    for p in add_paths:
        run_cmd(["git", "add", p], cwd=repo_dir)

    code, _, _ = run_cmd(["git", "diff", "--cached", "--quiet"], cwd=repo_dir)
    if code == 0:
        return out

    run_cmd(["git", "config", "user.name", "orion-bot"], cwd=repo_dir)
    run_cmd(["git", "config", "user.email", "orion-bot@local"], cwd=repo_dir)

    c, so, se = run_cmd(["git", "commit", "-m", message], cwd=repo_dir)
    if c != 0:
        out["error"] = (se.strip() or so.strip() or "commit failed")
        return out

    c2, so2, _ = run_cmd(["git", "rev-parse", "HEAD"], cwd=repo_dir)
    if c2 == 0:
        out["commit"] = so2.strip() or None

    c3, so3, se3 = run_cmd(["git", "push", "origin", branch], cwd=repo_dir)
    if c3 != 0:
        out["error"] = (se3.strip() or so3.strip() or "push failed")
        return out

    out["pushed"] = True
    return out
