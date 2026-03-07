#!/usr/bin/env python3
"""
download_bdc_api.py — Automated FCC BDC data download via Public Data API.

Downloads Fixed Broadband Location Coverage CSVs for:
  Group A: Verizon (131425) + Frontier (130258)
  Group B: Charter/Spectrum (130235) + Cox (130360)

Uses curl for HTTP (Python urllib times out on FCC's API).

Usage:
  python3 download_bdc_api.py                    # uses .env file
  python3 download_bdc_api.py --list-only         # just list available files
  python3 download_bdc_api.py --force             # re-download everything
"""

import argparse
import json
import os
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from urllib.parse import urlencode

# ============================================
# CONFIGURATION
# ============================================

BASE_URL = 'https://bdc.fcc.gov'
AS_OF_DATE = '2025-06-30'  # Latest availability filing (J25)

TARGET_PROVIDERS = {
    '131425': 'Verizon',
    '130258': 'Frontier',
    '130235': 'Charter/Spectrum',
    '130360': 'Cox',
}

RATE_LIMIT_DELAY = 6.5  # seconds between API calls

SCRIPT_DIR = Path(__file__).parent
FCC_DATA_DIR = SCRIPT_DIR / 'fcc_data'
ENV_FILE = SCRIPT_DIR / '.env'


# ============================================
# CREDENTIALS
# ============================================

def load_credentials(args):
    """Load FCC credentials from args or .env file."""
    username = getattr(args, 'username', None)
    token = getattr(args, 'token', None)

    if not username or not token:
        if ENV_FILE.exists():
            with open(ENV_FILE) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('#') or '=' not in line:
                        continue
                    key, val = line.split('=', 1)
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    if key == 'FCC_USERNAME':
                        username = val
                    elif key == 'FCC_TOKEN':
                        token = val

    if not username or not token:
        print("[ERROR] FCC credentials not found.")
        print("  Either pass --username and --token arguments,")
        print(f"  or create {ENV_FILE} with:")
        print("    FCC_USERNAME=your_username")
        print("    FCC_TOKEN=your_api_token")
        sys.exit(1)

    return username, token


# ============================================
# API CALLS (using curl)
# ============================================

def api_get(url, username, token, timeout=120):
    """GET JSON from FCC API using curl."""
    result = subprocess.run(
        ['curl', '-s', '--max-time', str(timeout),
         '-H', f'username: {username}',
         '-H', f'hash_value: {token}',
         url],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl failed (exit {result.returncode}): {result.stderr}")
    return json.loads(result.stdout)


def api_download(url, username, token, output_path, timeout=600):
    """Download file from FCC API using curl, returns (path, size)."""
    # Use -D to capture headers, -o to save body
    header_file = str(output_path) + '.headers'
    result = subprocess.run(
        ['curl', '-s', '--max-time', str(timeout),
         '-H', f'username: {username}',
         '-H', f'hash_value: {token}',
         '-D', header_file,
         '-o', str(output_path),
         url],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl failed (exit {result.returncode}): {result.stderr}")

    # Check for filename in Content-Disposition header
    actual_path = output_path
    if os.path.exists(header_file):
        with open(header_file) as f:
            for line in f:
                if 'filename=' in line.lower():
                    fname = line.split('filename=')[1].strip().strip('"').strip("'").strip()
                    if fname:
                        actual_path = output_path.parent / fname
                        if actual_path != output_path:
                            os.rename(output_path, actual_path)
                    break
        os.remove(header_file)

    size = actual_path.stat().st_size if actual_path.exists() else 0
    return actual_path, size


# ============================================
# MAIN LOGIC
# ============================================

def list_available_files(username, token):
    """List all Fixed Broadband Location Coverage files for our target providers."""
    print(f"[1/3] Querying available files for as_of_date={AS_OF_DATE}...")
    print(f"  Filtering: category=Provider, subcategory=Location Coverage, technology=Fixed Broadband")

    params = urlencode({
        'category': 'Provider',
        'subcategory': 'Location Coverage',
        'technology_type': 'Fixed Broadband',
    })
    url = f'{BASE_URL}/api/public/map/downloads/listAvailabilityData/{AS_OF_DATE}?{params}'

    data = api_get(url, username, token, timeout=180)

    if data.get('status') != 'successful':
        print(f"[ERROR] API returned: {data.get('message', 'Unknown error')}")
        sys.exit(1)

    total = data.get('result_count', 0)
    print(f"  Total Fixed Broadband Location Coverage files: {total}")

    target_files = [
        item for item in data.get('data', [])
        if item.get('provider_id', '') in TARGET_PROVIDERS
    ]

    print(f"  Files matching our 4 providers: {len(target_files)}")
    return target_files


def check_already_downloaded(target_files):
    """Check which files we already have in fcc_data/."""
    needed = []
    already_have = []

    for item in target_files:
        fname = item.get('file_name', '')
        state = item.get('state_fips', '')
        pid = item.get('provider_id', '')

        # Check for CSV by exact filename
        csv_path = FCC_DATA_DIR / f"{fname}.csv"
        if csv_path.exists():
            already_have.append(item)
            continue

        # Check for any existing CSV matching state+provider pattern
        existing = list(FCC_DATA_DIR.glob(f"bdc_{state}_{pid}_*.csv"))
        if existing:
            already_have.append(item)
        else:
            needed.append(item)

    return needed, already_have


def download_files(target_files, username, token, force=False):
    """Download all target files via API."""
    FCC_DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not force:
        needed, already_have = check_already_downloaded(target_files)
        if already_have:
            print(f"\n  Already have {len(already_have)} files, skipping.")
    else:
        needed = target_files

    if not needed:
        print("\n  All files already downloaded!")
        return

    est_min = len(needed) * RATE_LIMIT_DELAY / 60
    print(f"\n[2/3] Downloading {len(needed)} files...")
    print(f"  Rate limit: 1 request every {RATE_LIMIT_DELAY}s (~{est_min:.0f} min total)")

    downloaded = 0
    failed = []

    for i, item in enumerate(needed):
        file_id = item.get('file_id')
        state = item.get('state_name', '?')
        state_fips = item.get('state_fips', '??')
        pid = item.get('provider_id', '')
        pname = TARGET_PROVIDERS.get(pid, pid)
        records = item.get('record_count', '?')

        print(f"  [{i+1}/{len(needed)}] {state} ({state_fips}) / {pname} ({records} records)...", end=' ', flush=True)

        try:
            output_name = f"bdc_{state_fips}_{pid}_download.zip"
            url = f'{BASE_URL}/api/public/map/downloads/downloadFile/availability/{file_id}'
            target_path, size_bytes = api_download(
                url, username, token,
                FCC_DATA_DIR / output_name,
            )
            size_mb = size_bytes / 1e6
            print(f"OK ({size_mb:.1f} MB) -> {target_path.name}")
            downloaded += 1
        except Exception as e:
            print(f"FAILED: {e}")
            failed.append((state, pname, str(e)))

        # Rate limiting
        if i < len(needed) - 1:
            time.sleep(RATE_LIMIT_DELAY)

    print(f"\n[3/3] Done. Downloaded {downloaded}/{len(needed)} files.")
    if failed:
        print(f"  Failed ({len(failed)}):")
        for state, pname, err in failed:
            print(f"    - {state} / {pname}: {err}")

    # Unzip all downloaded ZIPs
    unzip_all()


def unzip_all():
    """Unzip all ZIP files in fcc_data/."""
    zips = list(FCC_DATA_DIR.glob('*.zip'))
    if not zips:
        return
    print(f"\n  Unzipping {len(zips)} files...")
    for zf in zips:
        try:
            with zipfile.ZipFile(zf, 'r') as z:
                z.extractall(FCC_DATA_DIR)
            print(f"    OK: {zf.name}")
            zf.unlink()  # Remove ZIP after extraction
        except Exception as e:
            print(f"    FAILED: {zf.name}: {e}")


def print_file_summary(target_files):
    """Print summary of available files by provider and state."""
    by_provider = {}
    for item in target_files:
        pid = item.get('provider_id', '')
        pname = TARGET_PROVIDERS.get(pid, pid)
        state = item.get('state_name', '?')
        state_fips = item.get('state_fips', '??')
        records = item.get('record_count', '0')

        if pname not in by_provider:
            by_provider[pname] = []
        by_provider[pname].append({
            'state': state,
            'fips': state_fips,
            'records': int(records) if records else 0,
            'file_id': item.get('file_id'),
        })

    print(f"\n{'='*70}")
    print(f"AVAILABLE FILES BY PROVIDER")
    print(f"{'='*70}")

    total_files = 0
    total_records = 0

    for pname in sorted(by_provider.keys()):
        states = sorted(by_provider[pname], key=lambda x: x['fips'])
        provider_records = sum(s['records'] for s in states)
        print(f"\n  {pname} ({len(states)} states, {provider_records:,} total records):")
        for s in states:
            print(f"    {s['fips']} {s['state']:25s} {s['records']:>10,} records")
        total_files += len(states)
        total_records += provider_records

    print(f"\n  TOTAL: {total_files} files, {total_records:,} records")
    return by_provider


# ============================================
# CLI
# ============================================

def main():
    global AS_OF_DATE

    parser = argparse.ArgumentParser(description='Download FCC BDC data via API')
    parser.add_argument('--username', help='FCC username')
    parser.add_argument('--token', help='FCC API token (hash_value)')
    parser.add_argument('--list-only', action='store_true', help='List available files without downloading')
    parser.add_argument('--force', action='store_true', help='Re-download even if files exist')
    parser.add_argument('--as-of-date', default=AS_OF_DATE, help=f'As-of date (default: {AS_OF_DATE})')
    args = parser.parse_args()

    AS_OF_DATE = args.as_of_date

    print("=" * 70)
    print("FCC BDC Automated Downloader")
    print(f"Target providers: {', '.join(TARGET_PROVIDERS.values())}")
    print(f"As-of date: {AS_OF_DATE}")
    print("=" * 70)

    username, token = load_credentials(args)
    print(f"  Authenticated as: {username}")

    target_files = list_available_files(username, token)

    if not target_files:
        print("[ERROR] No files found for target providers.")
        sys.exit(1)

    by_provider = print_file_summary(target_files)

    if args.list_only:
        print("\n  --list-only mode, skipping downloads.")
        return

    download_files(target_files, username, token, force=args.force)

    print("\n" + "=" * 70)
    print("ALL DONE. Run overlap_analysis.py to compute overlap.")
    print("=" * 70)


if __name__ == '__main__':
    main()
