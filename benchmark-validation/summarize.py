# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Summarize the existing runner's JSON without treating warmup as a sample."""
import csv
import json
from pathlib import Path
from statistics import median

import sys
root = Path(sys.argv[1]).resolve()
rows = []
for variant in ['base', 'v1', 'v2']:
    timings = {}
    layouts = {}
    for round_path in sorted((root / variant).glob('round-*')):
        for suite in ['scan', 'write']:
            result = round_path / f'{suite}.json'
            if not result.exists():
                continue
            data = json.loads(result.read_text())
            for query in data['queries']:
                assert query['success'], (variant, query['query'])
                sample = timings.setdefault((suite, query['query']), {'times': [], 'peaks': []})
                sample['times'].extend(it['elapsed'] for it in query['iterations'][1:])
                if query.get('pool_peak_bytes') is not None:
                    sample['peaks'].append(query['pool_peak_bytes'])
        layout = round_path / 'layout.json'
        if layout.exists():
            for entry in json.loads(layout.read_text()):
                layouts.setdefault(entry['case'], []).append(entry)
    for (suite, query), samples in timings.items():
        case = query.split('/')[1]
        files = layouts.get(case, [])
        groups = [g for file in files for g in file['groups']]
        row = {
            'variant': variant, 'suite': suite, 'case': case,
            'samples': len(samples['times']), 'median_ms': median(samples['times']),
            'min_ms': min(samples['times']), 'max_ms': max(samples['times']),
            'peak_tracked_bytes': max(samples['peaks'], default=None),
            'row_group_counts': '/'.join(str(len(file['groups'])) for file in files),
            'max_rows_per_group': max((g['rows'] for g in groups), default=None),
            'max_uncompressed_group_bytes': max((g['uncompressed_bytes'] for g in groups), default=None),
            'max_compressed_group_bytes': max((g['compressed_bytes'] for g in groups), default=None),
            'max_file_bytes': max((file['file_bytes'] for file in files), default=None),
        }
        rows.append(row)
assert rows, 'No completed benchmark results'
with (root / 'comparison.csv').open('w', newline='') as output:
    writer = csv.DictWriter(output, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)

lookup = {(row['variant'], row['case']): row for row in rows}
lines = ['# Linux benchmark observations', '',
         'Existing DataFusion SQL runner, release-nonlto, four Tokio threads, one input partition,',
         '2 GiB greedy memory pool. Each process runs seven iterations; the first is excluded.',
         'Two rounds use opposite revision order. Times below are medians of the remaining samples.',
         'These are observations from one GitHub-hosted Linux runner, without statistical significance claims.', '',
         '| Input | V1 bytes (ms) | V2 bytes (ms) | V2 elapsed / V1 | V1 groups | V2 groups |',
         '| --- | ---: | ---: | ---: | ---: | ---: |']
for case in ['narrow', 'wide', 'rotating', 'booleans']:
    a = lookup.get(('v1', f'{case}_parallel_bytes'))
    b = lookup.get(('v2', f'{case}_parallel_bytes'))
    if a and b:
        lines.append(f"| {case} | {a['median_ms']:.2f} | {b['median_ms']:.2f} | {b['median_ms']/a['median_ms']:.3f} | {a['row_group_counts']} | {b['row_group_counts']} |")
lines += ['', 'See comparison.csv for disabled-limit controls, serial controls, ranges, and memory/layout measurements.',
          'The base parallel writer ignores the byte option. Its byte-enabled result is not a size-equivalent baseline.',
          'Footer sizes differ from encoder estimates. Tracked memory is not process RSS.', '']
(root / 'comparison.md').write_text('\n'.join(lines))
print(root / 'comparison.md')
