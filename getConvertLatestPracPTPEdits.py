import io
import os
import re
import zipfile
from pathlib import Path
from typing import Optional, Tuple, List

import pandas as pd
import requests


# --- Patterns ---
_VERSION_RE = re.compile(r"(?:^|[-_])v(?P<v>\d{3})r(?P<r>\d+)(?:[-_]|$)", re.IGNORECASE)
_QTR_RE     = re.compile(r"(?P<y>20\d{2})q(?P<q>[1-4])", re.IGNORECASE)
_F_RE       = re.compile(r"-f(?P<f>[12])(?=\.zip|[-_])", re.IGNORECASE)


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (compatible; NCCI-Puller/3.0)"})
    return s

def _bytes_is_zip(b: bytes) -> bool:
    import zipfile, io
    if not b:
        return False
    try:
        # Authoritative test: can ZipFile open it?
        return zipfile.is_zipfile(io.BytesIO(b))
    except Exception:
        return False

def _dump_debug_blob(b: bytes, out_path: Path, label: str):
    """
    Optional: Write unexpected responses to disk for inspection.
    """
    try:
        out_path.mkdir(parents=True, exist_ok=True)
        p = out_path / f"debug-{label}.bin"
        p.write_bytes(b or b"")
    except Exception:
        pass


def _looks_like_zip(content: bytes, headers: dict) -> bool:
    if not content:
        return False
    # ZIP local-file header magic: PK\x03\x04 (also accept empty archives PK\x05\x06)
    if content[:2] == b"PK":
        return True
    ctype = (headers or {}).get("Content-Type", "").lower()
    if "zip" in ctype:
        return True
    cd = (headers or {}).get("Content-Disposition", "").lower()
    if ".zip" in cd or "application/zip" in cd:
        return True
    return False

def _fetch_zip_via_license(license_url: str, timeout=60) -> Optional[bytes]:
    """
    Follow the CMS license gate.
    Returns ZIP bytes iff the body is truly a ZIP (validated with zipfile.is_zipfile).
    Otherwise tries multiple patterns to discover the real .zip URL from HTML.
    """
    try:
        s = _session()
        r = s.get(license_url, timeout=timeout, allow_redirects=True)

        if r.status_code >= 400:
            return None

        # If it already is a ZIP, use it.
        if _bytes_is_zip(r.content):
            return r.content

        ctype = (r.headers.get("Content-Type") or "").lower()
        html = r.text if "html" in ctype and hasattr(r, "text") else ""

        # If HTML, mine it for a .zip URL using several patterns.
        if html:
            # 1) Normal links: <a href="/files/zip/...zip">  or absolute version
            pats = [
                r'href=[\'"](/files/zip/[^\'"]+\.zip)[\'"]',
                r'href=[\'"](https?://[^\'"]+\.zip)[\'"]',
                # 2) Meta refresh: <meta http-equiv="refresh" content="0;url=/files/zip/...zip">
                r'http-equiv=["\']refresh["\'][^>]*content=["\'][^"\']*url=([^"\']+\.zip)[^"\']*["\']',
                # 3) JS redirects:
                r'location\.href\s*=\s*[\'"](.*?\.zip)[\'"]',
                r'window\.location\s*=\s*[\'"](.*?\.zip)[\'"]',
                r'window\.location\.href\s*=\s*[\'"](.*?\.zip)[\'"]',
                # 4) Any stray .zip in the HTML as last resort
                r'((?:https?://|/files/zip/)[^\'"\s<>]+\.zip)'
            ]
            zip_url = None
            for p in pats:
                m = re.search(p, html, flags=re.IGNORECASE | re.DOTALL)
                if m:
                    zip_url = m.group(1)
                    break

            if zip_url:
                # Normalize relative path
                if zip_url.startswith("/"):
                    zip_url = "https://www.cms.gov" + zip_url

                # Re-fetch with proper Referer
                s.headers.update({"Referer": license_url})
                r2 = s.get(zip_url, timeout=timeout, allow_redirects=True)
                if r2.status_code < 400 and _bytes_is_zip(r2.content):
                    return r2.content

        # If we’re here, we didn’t get a ZIP—save HTML for debugging if present
        if html:
            debug_dir = Path("data/ncci/_debug")
            debug_dir.mkdir(parents=True, exist_ok=True)
            (debug_dir / "last-license-page.html").write_text(html, encoding="utf-8")

        return None

    except requests.RequestException:
        return None


# --- Parsing helpers ---
def _parse_vr(url: str) -> Tuple[int, int]:
    m = _VERSION_RE.search(url)
    if not m:
        raise ValueError("Missing v###r# in URL.")
    return int(m.group("v")), int(m.group("r"))

def _parse_quarter(url: str) -> Tuple[int, int]:
    m = _QTR_RE.search(url)
    if not m:
        raise ValueError("Missing YYYYq# in URL.")
    return int(m.group("y")), int(m.group("q"))

def _next_quarter(y: int, q: int) -> Tuple[int, int]:
    return (y, q + 1) if q < 4 else (y + 1, 1)


def _set_vr(url: str, v: int, r: int) -> str:
    def repl(m: re.Match) -> str:
        prefix = "-" if m.group(0).startswith("-") else ""
        suffix = "-" if m.group(0).endswith("-") else ""
        return f"{prefix}v{v:03d}r{r}{suffix}"
    return _VERSION_RE.sub(repl, url, count=1)


def _set_quarter(url: str, y: int, q: int) -> str:
    return _QTR_RE.sub(f"{y}q{q}", url, count=1)


def _with_f(url: str, fnum: int) -> str:
    if _F_RE.search(url):
        return _F_RE.sub(f"-f{fnum}", url, count=1)
    return re.sub(r"\.zip$", f"-f{fnum}.zip", url, flags=re.IGNORECASE)


def _pair_f1_f2(base_f1_url: str) -> Tuple[str, str]:
    return (_with_f(base_f1_url, 1), _with_f(base_f1_url, 2))


# --- File helpers ---
def _extract_member(zbytes: bytes) -> Tuple[str, bytes]:
    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
        for name in zf.namelist():
            if name.lower().endswith((".xlsx", ".txt")):
                return name, zf.read(name)
    raise FileNotFoundError("No .xlsx or .txt found inside zip.")


def _read_zip_to_df(zbytes: Optional[bytes]) -> pd.DataFrame:
    """
    Read CMS NCCI PTP file (xlsx or txt) and skip the first 6 non-data lines.
    Ensures correct header:
      Column 1 | Column 2 | *=in existence | Effective | Deletion | Modifier | PTP Edit Rationale
    """
    import io

    if not zbytes or not _bytes_is_zip(zbytes):
        raise RuntimeError(
            "CMS response was not a valid ZIP (likely the HTML license page or an error body)."
        )

    name, blob = _extract_member(zbytes)

    if name.lower().endswith(".xlsx"):
        # Skip first 6 rows (metadata) and read header row at line 7 (header=0 after skip)
        df = pd.read_excel(
            io.BytesIO(blob),
            dtype=str,
            engine="openpyxl",
            skiprows=6
        )
    else:
        # TXT fallback
        buf = io.StringIO(blob.decode("utf-8", errors="replace"))
        try:
            df = pd.read_csv(
                buf,
                delim_whitespace=True,
                dtype=str,
                engine="python",
                skiprows=6,
                header=0
            )
        except Exception:
            buf.seek(0)
            df = pd.read_fwf(buf, dtype=str, skiprows=6, header=0)

    # Normalize column names
    df.columns = [str(c).strip() for c in df.columns]
    df = df.astype(str)

    # Make sure expected columns exist (just for consistency)
    expected = [
        "Column 1",
        "Column 2",
        "*=in existence",
        "Effective",
        "Deletion",
        "Modifier",
        "PTP Edit Rationale",
    ]
    # If mismatch, just keep current names—CMS sometimes changes spacing slightly.
    if len(df.columns) == len(expected):
        df.columns = expected

    return df



def _concat_dfs(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    all_cols = sorted(set().union(*(df.columns for df in dfs)))
    return pd.concat([df.reindex(columns=all_cols) for df in dfs], ignore_index=True)


def _combined_csv_path(base_f1_url: str, out_dir: Path) -> Path:
    base = os.path.basename(re.search(r"file=([^&]+)", base_f1_url).group(1))
    stem = os.path.splitext(base)[0]
    stem = re.sub(r"-f[12]$", "", stem)
    tail = re.search(r"(ccipra-[^/\\]+)$", stem, re.IGNORECASE)
    final = tail.group(1) if tail else stem
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{final}-f1f2.csv"


def _version_file(out_dir: Path) -> Path:
    return out_dir / "version.txt"


def _read_local_version(out_dir: Path) -> Optional[Tuple[int, int, int, int]]:
    vf = _version_file(out_dir)
    if not vf.exists():
        return None
    txt = vf.read_text().strip()
    m = re.match(r"(\d{4})q(\d)\s+v(\d{3})r(\d+)", txt)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))


def _write_local_version(out_dir: Path, y: int, q: int, v: int, r: int):
    vf = _version_file(out_dir)
    vf.write_text(f"{y}q{q} v{v:03d}r{r}\n")


# --- Core workflow ---
def download_or_skip_both_with_version(
    base_f1_url: str,
    out_dir: str | Path = "data"
) -> Tuple[Path, str]:
    out_dir = Path(out_dir)

    # Load current version from version.txt (if any)
    local = _read_local_version(out_dir)
    if local:
        y, q, v, r = local
        print(f"Using stored version: {y}q{q} v{v:03d}r{r}")
    else:
        v, r = _parse_vr(base_f1_url)
        y, q = _parse_quarter(base_f1_url)

    # Candidate search order
    candidates = []
    # 1. newer revisions same version/quarter
    for trial_r in range(r + 1, r + 6):
        candidates.append((y, q, v, trial_r))
    # 2. next version same quarter
    candidates.append((y, q, v + 1, 0))
    # 3. next quarter (roll over)
    ny, nq = _next_quarter(y, q)
    candidates.append((ny, nq, v + 1, 0))
    candidates.append((ny, nq, v, 0))

    # Try each candidate pair
    target = None
    for (cy, cq, cv, cr) in candidates:
        url = _set_vr(_set_quarter(base_f1_url, cy, cq), cv, cr)
        f1, f2 = _pair_f1_f2(url)
        if _fetch_zip_via_license(f1) and _fetch_zip_via_license(f2):
            target = (cy, cq, cv, cr, f1, f2)
            break

    # If none found and we already have CSV → skip
    csv_path = _combined_csv_path(base_f1_url, out_dir)
    if not target:
        if csv_path.exists():
            return csv_path, "Up to date — no newer release found."
        # otherwise, download current
        f1, f2 = _pair_f1_f2(base_f1_url)
        df = _concat_dfs([_read_zip_to_df(_fetch_zip_via_license(f1)),
                          _read_zip_to_df(_fetch_zip_via_license(f2))])
        df.to_csv(csv_path, index=False)
        _write_local_version(out_dir, y, q, v, r)
        return csv_path, f"Downloaded initial version {y}q{q} v{v:03d}r{r}."

    # Found an update → download and combine
    cy, cq, cv, cr, f1, f2 = target
    df = _concat_dfs([_read_zip_to_df(_fetch_zip_via_license(f1)),
                      _read_zip_to_df(_fetch_zip_via_license(f2))])
    csv_path = _combined_csv_path(f1, out_dir)
    df.to_csv(csv_path, index=False)
    _write_local_version(out_dir, cy, cq, cv, cr)
    return csv_path, f"Updated to {cy}q{cq} v{cv:03d}r{cr}."


if __name__ == "__main__":
    base = "https://www.cms.gov/license/ama?file=/files/zip/medicare-ncci-2025q4-practitioner-ptp-edits-ccipra-v313r0-f1.zip"
    csv, msg = download_or_skip_both_with_version(base)
    print(msg)
    print(f"CSV: {csv.resolve()}")
