#!/usr/bin/env python3
"""
overlap_analysis.py — Estimate FTTH BSL overlap between
  Group A: Verizon (131425) + Frontier (130258)
  Group B: Charter/Spectrum (130235) + Cox (130360)

Reads FCC BDC CSVs from fcc_data/, filters to FTTP (tech=50),
matches on location_id to find direct overlap.

Usage: python3 overlap_analysis.py
"""

import csv
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

# ============================================
# CONFIGURATION
# ============================================

PROVIDER_GROUPS = {
    'A': {
        'name': 'Verizon PF Frontier',
        'providers': {
            '131425': 'Verizon',
            '130258': 'Frontier',
        }
    },
    'B': {
        'name': 'Charter PF Cox',
        'providers': {
            '130235': 'Charter/Spectrum',
            '130360': 'Cox',
        }
    }
}

# Technology codes
TECH_FTTP = 50        # Fiber to the Premises
TECH_HFC = {40, 41, 42, 43}  # Cable Modem (DOCSIS 3.0, 3.1, other)
TECH_FIBER_AND_HFC = {50} | TECH_HFC

STATE_NAMES = {
    '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA',
    '08': 'CO', '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL',
    '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN',
    '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME',
    '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS',
    '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH',
    '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND',
    '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI',
    '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT',
    '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI',
    '56': 'WY',
}

SCRIPT_DIR = Path(__file__).parent
FCC_DATA_DIR = SCRIPT_DIR / 'fcc_data'


# ============================================
# STEP 1: READ ALL BDC CSVs
# ============================================

def detect_provider_id(filename):
    """Extract provider ID from BDC filename pattern: bdc_{state}_{providerID}_..."""
    parts = filename.split('_')
    if len(parts) >= 3 and parts[0] == 'bdc':
        candidate = parts[2]
        if candidate.isdigit() and len(candidate) == 6:
            return candidate
    return None


def get_group_for_provider(provider_id):
    """Return group key ('A' or 'B') and provider name, or None."""
    for group_key, group in PROVIDER_GROUPS.items():
        if provider_id in group['providers']:
            return group_key, group['providers'][provider_id]
    return None, None


def read_all_csvs():
    """Read all BDC CSVs, return per-provider location data.

    Group A (Verizon/Frontier): fiber only (tech 50)
    Group B (Charter/Cox): fiber + HFC (tech 40-43, 50)

    Returns:
        provider_locations: { provider_id: { location_id: (state_fips, county_fips, block_geoid, biz_res, tech_type) } }
        file_stats: list of dicts with per-file processing stats
    """
    csv_files = sorted(FCC_DATA_DIR.glob('*.csv'))

    # Filter to only files matching our providers
    relevant_files = []
    for f in csv_files:
        pid = detect_provider_id(f.name)
        group, name = get_group_for_provider(pid) if pid else (None, None)
        if group:
            relevant_files.append((f, pid, group, name))

    if not relevant_files:
        print("[ERROR] No relevant CSV files found in fcc_data/")
        sys.exit(1)

    print(f"Found {len(relevant_files)} relevant CSV files\n")

    # Per-provider: location_id -> (state_fips, county_fips, block_geoid, biz_res, tech_type)
    # tech_type: 'fiber' or 'hfc'
    provider_locations = defaultdict(dict)
    file_stats = []

    for csv_path, provider_id, group, provider_name in relevant_files:
        state_fips = csv_path.name.split('_')[1]
        state_abbr = STATE_NAMES.get(state_fips, state_fips)

        # Group A: fiber only | Group B: fiber + HFC
        if group == 'A':
            allowed_techs = {TECH_FTTP}
        else:
            allowed_techs = TECH_FIBER_AND_HFC

        t0 = time.time()
        total_rows = 0
        matched_rows = 0
        fiber_rows = 0
        hfc_rows = 0

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_rows += 1
                tech = int(row.get('technology', 0))
                if tech not in allowed_techs:
                    continue

                matched_rows += 1
                loc_id = row.get('location_id', '')
                block_geoid = row.get('block_geoid', '')
                biz_res = row.get('business_residential_code', 'R')

                if loc_id and block_geoid:
                    county_fips = block_geoid[:5]
                    tech_type = 'fiber' if tech == TECH_FTTP else 'hfc'

                    # If location already recorded, prefer fiber over HFC
                    existing = provider_locations[provider_id].get(loc_id)
                    if existing and existing[4] == 'fiber':
                        continue  # already have fiber record, skip HFC duplicate

                    provider_locations[provider_id][loc_id] = (
                        state_fips, county_fips, block_geoid, biz_res, tech_type
                    )

                    if tech_type == 'fiber':
                        fiber_rows += 1
                    else:
                        hfc_rows += 1

        elapsed = time.time() - t0
        if group == 'A':
            print(f"  {state_abbr} {provider_name:20s} | {fiber_rows:>10,} fiber BSLs / {total_rows:>10,} total | {elapsed:.1f}s")
        else:
            print(f"  {state_abbr} {provider_name:20s} | {fiber_rows:>10,} fiber + {hfc_rows:>10,} HFC = {fiber_rows+hfc_rows:>10,} BSLs / {total_rows:>10,} total | {elapsed:.1f}s")

        file_stats.append({
            'file': csv_path.name,
            'provider_id': provider_id,
            'provider_name': provider_name,
            'group': group,
            'state': state_abbr,
            'total_rows': total_rows,
            'fiber_rows': fiber_rows,
            'hfc_rows': hfc_rows,
            'matched_rows': matched_rows,
        })

    return dict(provider_locations), file_stats


# ============================================
# STEP 2: COMPUTE OVERLAP
# ============================================

def compute_overlap(provider_locations):
    """Compute BSL-level overlap between Group A and Group B.

    Group A = Verizon/Frontier fiber
    Group B = Charter/Cox fiber + HFC

    Breaks down overlap into fiber-vs-fiber and fiber-vs-HFC.

    Returns:
        results dict with all overlap statistics
    """
    # Build group sets with tech type tracking
    # location_id -> (state, county, block, biz_res, provider_id, tech_type)
    group_a_locs = {}
    group_b_locs = {}

    for group_key, group in PROVIDER_GROUPS.items():
        target = group_a_locs if group_key == 'A' else group_b_locs
        for pid in group['providers']:
            if pid in provider_locations:
                for loc_id, (state, county, block, biz_res, tech_type) in provider_locations[pid].items():
                    target[loc_id] = (state, county, block, biz_res, pid, tech_type)

    group_a_set = set(group_a_locs.keys())
    group_b_set = set(group_b_locs.keys())
    overlap_set = group_a_set & group_b_set

    # Split Group B into fiber-only and HFC-only for counting
    b_fiber_set = {lid for lid in group_b_set if group_b_locs[lid][5] == 'fiber'}
    b_hfc_set = {lid for lid in group_b_set if group_b_locs[lid][5] == 'hfc'}

    # Split overlap by Group B tech type
    overlap_vs_fiber = {lid for lid in overlap_set if group_b_locs[lid][5] == 'fiber'}
    overlap_vs_hfc = {lid for lid in overlap_set if group_b_locs[lid][5] == 'hfc'}

    print(f"\n{'='*70}")
    print(f"OVERLAP RESULTS")
    print(f"{'='*70}")
    print(f"Group A ({PROVIDER_GROUPS['A']['name']}):     {len(group_a_set):>12,} fiber BSLs")
    print(f"Group B ({PROVIDER_GROUPS['B']['name']}):     {len(group_b_set):>12,} broadband BSLs")
    print(f"  of which Fiber:                       {len(b_fiber_set):>12,}")
    print(f"  of which HFC/Cable:                   {len(b_hfc_set):>12,}")
    print(f"")
    print(f"TOTAL OVERLAP:                          {len(overlap_set):>12,} BSLs")
    print(f"  Fiber vs Fiber:                       {len(overlap_vs_fiber):>12,}")
    print(f"  Fiber vs HFC:                         {len(overlap_vs_hfc):>12,}")
    if group_a_set:
        print(f"  = {len(overlap_set)/len(group_a_set)*100:.1f}% of Group A fiber footprint overbuilt by Charter/Cox")
    if group_b_set:
        print(f"  = {len(overlap_set)/len(group_b_set)*100:.1f}% of Group B broadband footprint faces VZ/Frontier fiber")

    # --- Per-provider totals ---
    print(f"\n{'='*70}")
    print(f"PER-PROVIDER BSL COUNTS")
    print(f"{'='*70}")
    for group_key, group in PROVIDER_GROUPS.items():
        for pid, pname in group['providers'].items():
            locs = provider_locations.get(pid, {})
            fiber_ct = sum(1 for v in locs.values() if v[4] == 'fiber')
            hfc_ct = sum(1 for v in locs.values() if v[4] == 'hfc')
            total = len(locs)
            if group_key == 'A':
                print(f"  {pname:25s} (Group {group_key}): {total:>10,} fiber")
            else:
                print(f"  {pname:25s} (Group {group_key}): {fiber_ct:>10,} fiber + {hfc_ct:>10,} HFC = {total:>10,} total")

    # --- By state ---
    state_stats = defaultdict(lambda: {
        'group_a': 0, 'group_b': 0, 'b_fiber': 0, 'b_hfc': 0,
        'overlap': 0, 'overlap_fiber': 0, 'overlap_hfc': 0,
        'a_providers': defaultdict(int),
        'b_providers': defaultdict(int),
    })

    for loc_id in group_a_set:
        state = group_a_locs[loc_id][0]
        pid = group_a_locs[loc_id][4]
        pname = None
        for g in PROVIDER_GROUPS.values():
            if pid in g['providers']:
                pname = g['providers'][pid]
        state_stats[state]['group_a'] += 1
        state_stats[state]['a_providers'][pname] += 1

    for loc_id in group_b_set:
        state = group_b_locs[loc_id][0]
        pid = group_b_locs[loc_id][4]
        tech = group_b_locs[loc_id][5]
        pname = None
        for g in PROVIDER_GROUPS.values():
            if pid in g['providers']:
                pname = g['providers'][pid]
        state_stats[state]['group_b'] += 1
        state_stats[state]['b_providers'][pname] += 1
        if tech == 'fiber':
            state_stats[state]['b_fiber'] += 1
        else:
            state_stats[state]['b_hfc'] += 1

    for loc_id in overlap_set:
        state = group_a_locs[loc_id][0]
        b_tech = group_b_locs[loc_id][5]
        state_stats[state]['overlap'] += 1
        if b_tech == 'fiber':
            state_stats[state]['overlap_fiber'] += 1
        else:
            state_stats[state]['overlap_hfc'] += 1

    print(f"\n{'='*70}")
    print(f"BY STATE")
    print(f"{'='*70}")
    print(f"{'State':<6} {'Grp A Fiber':>12} {'Grp B Total':>12} {'(B Fiber)':>10} {'(B HFC)':>10} {'Overlap':>10} {'(vs Fib)':>9} {'(vs HFC)':>9} {'% of A':>7}")
    print(f"{'-'*6} {'-'*12} {'-'*12} {'-'*10} {'-'*10} {'-'*10} {'-'*9} {'-'*9} {'-'*7}")

    sorted_states = sorted(state_stats.keys(), key=lambda s: state_stats[s]['overlap'], reverse=True)
    for state in sorted_states:
        s = state_stats[state]
        if s['overlap'] == 0 and s['group_a'] == 0 and s['group_b'] == 0:
            continue
        abbr = STATE_NAMES.get(state, state)
        pct_a = f"{s['overlap']/s['group_a']*100:.1f}%" if s['group_a'] else '-'
        print(f"{abbr:<6} {s['group_a']:>12,} {s['group_b']:>12,} {s['b_fiber']:>10,} {s['b_hfc']:>10,} {s['overlap']:>10,} {s['overlap_fiber']:>9,} {s['overlap_hfc']:>9,} {pct_a:>7}")

    # --- Show per-state provider breakdown for states with overlap ---
    print(f"\n{'='*70}")
    print(f"STATE DETAIL (provider breakdown where overlap exists)")
    print(f"{'='*70}")
    for state in sorted_states:
        s = state_stats[state]
        if s['overlap'] == 0:
            continue
        abbr = STATE_NAMES.get(state, state)
        print(f"\n  {abbr}:")
        print(f"    Group A (fiber): {', '.join(f'{k}: {v:,}' for k, v in sorted(s['a_providers'].items(), key=lambda x: -x[1]))}")
        print(f"    Group B (broadband): {', '.join(f'{k}: {v:,}' for k, v in sorted(s['b_providers'].items(), key=lambda x: -x[1]))}")
        print(f"    Overlap: {s['overlap']:,} (fiber-vs-fiber: {s['overlap_fiber']:,} | fiber-vs-HFC: {s['overlap_hfc']:,})")

    # --- By provider pair, split by tech ---
    pair_stats = defaultdict(lambda: {'fiber': 0, 'hfc': 0, 'total': 0})
    for loc_id in overlap_set:
        a_pid = group_a_locs[loc_id][4]
        b_pid = group_b_locs[loc_id][4]
        b_tech = group_b_locs[loc_id][5]
        a_name = PROVIDER_GROUPS['A']['providers'].get(a_pid, a_pid)
        b_name = PROVIDER_GROUPS['B']['providers'].get(b_pid, b_pid)
        key = f"{a_name} vs {b_name}"
        pair_stats[key]['total'] += 1
        pair_stats[key][b_tech] += 1

    print(f"\n{'='*70}")
    print(f"BY PROVIDER PAIR")
    print(f"{'='*70}")
    print(f"  {'Pair':40s} {'Total':>10} {'vs Fiber':>10} {'vs HFC':>10}")
    print(f"  {'-'*40} {'-'*10} {'-'*10} {'-'*10}")
    for pair, counts in sorted(pair_stats.items(), key=lambda x: -x[1]['total']):
        print(f"  {pair:40s} {counts['total']:>10,} {counts['fiber']:>10,} {counts['hfc']:>10,}")

    # --- By county (top 30) ---
    county_stats = defaultdict(lambda: {'overlap': 0, 'overlap_fiber': 0, 'overlap_hfc': 0, 'group_a': 0, 'group_b': 0})

    for loc_id in group_a_set:
        county = group_a_locs[loc_id][1]
        state = group_a_locs[loc_id][0]
        key = f"{county}|{state}"
        county_stats[key]['group_a'] += 1

    for loc_id in group_b_set:
        county = group_b_locs[loc_id][1]
        state = group_b_locs[loc_id][0]
        key = f"{county}|{state}"
        county_stats[key]['group_b'] += 1

    for loc_id in overlap_set:
        county = group_a_locs[loc_id][1]
        state = group_a_locs[loc_id][0]
        b_tech = group_b_locs[loc_id][5]
        key = f"{county}|{state}"
        county_stats[key]['overlap'] += 1
        if b_tech == 'fiber':
            county_stats[key]['overlap_fiber'] += 1
        else:
            county_stats[key]['overlap_hfc'] += 1

    print(f"\n{'='*70}")
    print(f"TOP 30 OVERLAP COUNTIES")
    print(f"{'='*70}")
    print(f"{'County FIPS':<14} {'State':<6} {'Overlap':>10} {'vs Fiber':>10} {'vs HFC':>10} {'Grp A':>10} {'Grp B':>10}")
    print(f"{'-'*14} {'-'*6} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")

    sorted_counties = sorted(county_stats.keys(), key=lambda k: county_stats[k]['overlap'], reverse=True)
    for key in sorted_counties[:30]:
        county_fips, state_fips = key.split('|')
        c = county_stats[key]
        if c['overlap'] == 0:
            break
        abbr = STATE_NAMES.get(state_fips, state_fips)
        print(f"{county_fips:<14} {abbr:<6} {c['overlap']:>10,} {c['overlap_fiber']:>10,} {c['overlap_hfc']:>10,} {c['group_a']:>10,} {c['group_b']:>10,}")

    # --- By block group (12-digit GEOID) ---
    bg_stats = defaultdict(lambda: {'overlap': 0, 'overlap_fiber': 0, 'overlap_hfc': 0, 'group_a': 0, 'group_b': 0})

    print(f"\n[BG] Aggregating by block group (12-digit GEOID)...")
    for loc_id in group_a_set:
        bg_id = group_a_locs[loc_id][2][:12]  # block_geoid[:12]
        bg_stats[bg_id]['group_a'] += 1

    for loc_id in group_b_set:
        bg_id = group_b_locs[loc_id][2][:12]
        bg_stats[bg_id]['group_b'] += 1

    for loc_id in overlap_set:
        bg_id = group_a_locs[loc_id][2][:12]
        b_tech = group_b_locs[loc_id][5]
        bg_stats[bg_id]['overlap'] += 1
        if b_tech == 'fiber':
            bg_stats[bg_id]['overlap_fiber'] += 1
        else:
            bg_stats[bg_id]['overlap_hfc'] += 1

    bg_with_overlap = {k: v for k, v in bg_stats.items() if v['overlap'] > 0}
    print(f"[BG] {len(bg_with_overlap):,} block groups with overlap (of {len(bg_stats):,} total)")

    # --- Build results dict for JSON export ---
    results = {
        'summary': {
            'group_a_name': PROVIDER_GROUPS['A']['name'],
            'group_a_tech': 'Fiber (FTTP)',
            'group_b_name': PROVIDER_GROUPS['B']['name'],
            'group_b_tech': 'Fiber + HFC (Cable)',
            'group_a_bsls': len(group_a_set),
            'group_b_bsls': len(group_b_set),
            'group_b_fiber': len(b_fiber_set),
            'group_b_hfc': len(b_hfc_set),
            'overlap_bsls': len(overlap_set),
            'overlap_fiber_vs_fiber': len(overlap_vs_fiber),
            'overlap_fiber_vs_hfc': len(overlap_vs_hfc),
            'overlap_pct_of_a': round(len(overlap_set) / len(group_a_set) * 100, 2) if group_a_set else 0,
            'overlap_pct_of_b': round(len(overlap_set) / len(group_b_set) * 100, 2) if group_b_set else 0,
        },
        'by_state': {},
        'by_provider_pair': {k: dict(v) for k, v in pair_stats.items()},
        'by_county': {},
        'states_covered': sorted(set(
            STATE_NAMES.get(s, s) for s in state_stats.keys()
        )),
    }

    for state, s in state_stats.items():
        abbr = STATE_NAMES.get(state, state)
        results['by_state'][abbr] = {
            'group_a': s['group_a'],
            'group_b': s['group_b'],
            'b_fiber': s['b_fiber'],
            'b_hfc': s['b_hfc'],
            'overlap': s['overlap'],
            'overlap_fiber': s['overlap_fiber'],
            'overlap_hfc': s['overlap_hfc'],
            'a_providers': dict(s['a_providers']),
            'b_providers': dict(s['b_providers']),
        }

    for key in sorted_counties[:100]:
        county_fips, state_fips = key.split('|')
        c = county_stats[key]
        if c['overlap'] > 0:
            abbr = STATE_NAMES.get(state_fips, state_fips)
            results['by_county'][f"{county_fips} ({abbr})"] = {
                'overlap': c['overlap'],
                'overlap_fiber': c['overlap_fiber'],
                'overlap_hfc': c['overlap_hfc'],
                'group_a': c['group_a'],
                'group_b': c['group_b'],
            }

    # Block group data (compact keys to keep file size reasonable)
    results['by_block_group'] = {}
    for bg_id, s in sorted(bg_with_overlap.items(), key=lambda x: -x[1]['overlap']):
        results['by_block_group'][bg_id] = {
            'o': s['overlap'],
            'of': s['overlap_fiber'],
            'oh': s['overlap_hfc'],
            'a': s['group_a'],
            'b': s['group_b'],
        }

    return results


# ============================================
# STEP 3: EXPORT
# ============================================

def export_results(results):
    """Write results to JSON and CSV."""
    # JSON
    json_path = SCRIPT_DIR / 'overlap_results.json'
    with open(json_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nWritten: {json_path}")

    # CSV summary by state
    csv_path = SCRIPT_DIR / 'overlap_by_state.csv'
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['State', 'Group_A_Fiber', 'Group_B_Total', 'B_Fiber', 'B_HFC',
                         'Overlap_Total', 'Overlap_vs_Fiber', 'Overlap_vs_HFC', 'Pct_of_A'])
        for state, data in sorted(results['by_state'].items(), key=lambda x: -x[1]['overlap']):
            pct_a = round(data['overlap'] / data['group_a'] * 100, 1) if data['group_a'] else 0
            writer.writerow([state, data['group_a'], data['group_b'], data['b_fiber'], data['b_hfc'],
                             data['overlap'], data['overlap_fiber'], data['overlap_hfc'], pct_a])
        # Totals row
        s = results['summary']
        writer.writerow([
            'TOTAL', s['group_a_bsls'], s['group_b_bsls'], s['group_b_fiber'], s['group_b_hfc'],
            s['overlap_bsls'], s['overlap_fiber_vs_fiber'], s['overlap_fiber_vs_hfc'],
            s['overlap_pct_of_a'],
        ])
    print(f"Written: {csv_path}")

    # CSV by county
    csv_county_path = SCRIPT_DIR / 'overlap_by_county.csv'
    with open(csv_county_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['County_FIPS', 'State', 'Overlap_Total', 'Overlap_vs_Fiber', 'Overlap_vs_HFC',
                         'Group_A_Fiber', 'Group_B_Total'])
        for county_key, data in sorted(results['by_county'].items(), key=lambda x: -x[1]['overlap']):
            parts = county_key.split(' ')
            county_fips = parts[0]
            state = parts[1].strip('()')
            writer.writerow([county_fips, state, data['overlap'], data['overlap_fiber'],
                             data['overlap_hfc'], data['group_a'], data['group_b']])
    print(f"Written: {csv_county_path}")


# ============================================
# MAIN
# ============================================

def main():
    print("=" * 70)
    print("FTTH Overlap Analysis: Verizon/Frontier vs Charter/Cox")
    print("Source: FCC BDC Jun 2025 filing, FTTP (tech=50) only")
    print("=" * 70)
    print()

    t0 = time.time()

    print("[1/3] Reading BDC CSV files...")
    provider_locations, file_stats = read_all_csvs()

    print(f"\n[2/3] Computing overlap...")
    results = compute_overlap(provider_locations)

    print(f"\n[3/3] Exporting results...")
    export_results(results)

    elapsed = time.time() - t0
    print(f"\nTotal time: {elapsed:.1f}s")
    print("=" * 70)


if __name__ == '__main__':
    main()
