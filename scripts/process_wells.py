#!/usr/bin/env python3
"""
Process California DWR well completion reports CSV into optimized JSON for web lookup.
Creates county-based JSON files for efficient loading.
"""

import csv
import json
import os
from collections import defaultdict
from pathlib import Path

# Input/output paths
CSV_PATH = Path(__file__).parent.parent / "wellcompletionreports_full.csv"
OUTPUT_DIR = Path(__file__).parent.parent / "well-lookup" / "data"

def parse_float(val):
    """Safely parse a float value."""
    try:
        return float(val) if val and val.strip() else None
    except (ValueError, TypeError):
        return None

def parse_int(val):
    """Safely parse an int value."""
    try:
        return int(float(val)) if val and val.strip() else None
    except (ValueError, TypeError):
        return None

def process_wells():
    """Process CSV and create county-based JSON files."""
    
    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Group wells by county
    wells_by_county = defaultdict(list)
    
    # Also track summary stats
    stats = {
        'total_records': 0,
        'records_with_coords': 0,
        'records_with_depth': 0,
        'counties': {}
    }
    
    print(f"Reading {CSV_PATH}...")
    
    with open(CSV_PATH, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            stats['total_records'] += 1
            
            if stats['total_records'] % 100000 == 0:
                print(f"  Processed {stats['total_records']:,} records...")
            
            # Parse coordinates
            lat = parse_float(row.get('DECIMALLATITUDE'))
            lon = parse_float(row.get('DECIMALLONGITUDE'))
            
            # Skip records without valid coordinates
            if lat is None or lon is None:
                continue
            
            # Skip obviously invalid coordinates
            if not (32 <= lat <= 42) or not (-125 <= lon <= -114):
                continue
                
            stats['records_with_coords'] += 1
            
            # Parse depth info
            total_depth = parse_int(row.get('TOTALDRILLDEPTH'))
            completed_depth = parse_int(row.get('TOTALCOMPLETEDDEPTH'))
            depth = completed_depth or total_depth
            
            if depth is not None:
                stats['records_with_depth'] += 1
            
            # Get county
            county = row.get('COUNTYNAME', 'Unknown').strip()
            if not county:
                county = 'Unknown'
            
            # Track county stats
            if county not in stats['counties']:
                stats['counties'][county] = {'count': 0, 'depths': []}
            stats['counties'][county]['count'] += 1
            if depth:
                stats['counties'][county]['depths'].append(depth)
            
            # Create compact well record
            well = {
                'lat': round(lat, 5),  # ~1m precision
                'lon': round(lon, 5),
                'depth': depth,
                'static': parse_int(row.get('STATICWATERLEVEL')),
                'yield': parse_float(row.get('WELLYIELD')),
                'year': None
            }
            
            # Extract year from date
            date_str = row.get('DATEWORKENDED', '')
            if date_str and len(date_str) >= 4:
                try:
                    if '/' in date_str:
                        # MM/DD/YYYY format
                        parts = date_str.split('/')
                        if len(parts) == 3:
                            well['year'] = int(parts[2])
                    elif '-' in date_str:
                        # YYYY-MM-DD format
                        well['year'] = int(date_str[:4])
                except (ValueError, IndexError):
                    pass
            
            # Remove None values to save space
            well = {k: v for k, v in well.items() if v is not None}
            
            wells_by_county[county].append(well)
    
    print(f"\nTotal records processed: {stats['total_records']:,}")
    print(f"Records with valid coordinates: {stats['records_with_coords']:,}")
    print(f"Records with depth info: {stats['records_with_depth']:,}")
    print(f"Counties found: {len(wells_by_county)}")
    
    # Calculate county statistics
    county_stats = []
    for county, wells in sorted(wells_by_county.items()):
        depths = [w.get('depth') for w in wells if w.get('depth')]
        avg_depth = round(sum(depths) / len(depths)) if depths else None
        min_depth = min(depths) if depths else None
        max_depth = max(depths) if depths else None
        
        county_stats.append({
            'name': county,
            'count': len(wells),
            'avgDepth': avg_depth,
            'minDepth': min_depth,
            'maxDepth': max_depth
        })
    
    # Write county summary
    summary_path = OUTPUT_DIR / 'counties.json'
    with open(summary_path, 'w') as f:
        json.dump(county_stats, f, separators=(',', ':'))
    print(f"\nWrote county summary to {summary_path}")
    
    # Write individual county files
    print(f"\nWriting county files to {OUTPUT_DIR}...")
    for county, wells in sorted(wells_by_county.items()):
        # Sanitize county name for filename
        safe_name = county.lower().replace(' ', '-').replace('.', '')
        filename = f"{safe_name}.json"
        
        filepath = OUTPUT_DIR / filename
        with open(filepath, 'w') as f:
            json.dump(wells, f, separators=(',', ':'))
        
        print(f"  {county}: {len(wells):,} wells -> {filename}")
    
    # Create a master index with bounding boxes for each county
    index = []
    for county, wells in sorted(wells_by_county.items()):
        lats = [w['lat'] for w in wells]
        lons = [w['lon'] for w in wells]
        
        safe_name = county.lower().replace(' ', '-').replace('.', '')
        
        index.append({
            'name': county,
            'file': f"{safe_name}.json",
            'count': len(wells),
            'bounds': {
                'minLat': min(lats),
                'maxLat': max(lats),
                'minLon': min(lons),
                'maxLon': max(lons)
            }
        })
    
    index_path = OUTPUT_DIR / 'index.json'
    with open(index_path, 'w') as f:
        json.dump(index, f, separators=(',', ':'))
    print(f"\nWrote spatial index to {index_path}")
    
    print("\nDone!")

if __name__ == '__main__':
    process_wells()
