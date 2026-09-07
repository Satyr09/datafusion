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

import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time
import pyarrow as pa
import pyarrow.parquet as pq

engine, output = [Path(p).resolve() for p in sys.argv[1:]]
reports = output / 'reports'
reports.mkdir(parents=True, exist_ok=True)
variants = {
    'base': '5bf6aef8e37c91d882c0f75b772201d62733c88f',
    'v1': '738cf94800da4a1abc15bbddd45c50ffcd304169',
    'v2': '03eee15739034699924a17b48d108df62c94483e',
}

def run(command, log, cwd=engine):
    print('Running:', ' '.join(map(str, command)), flush=True)
    started = time.monotonic()
    with log.open('w') as stream:
        child = subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in child.stdout:
            print(line, end='', flush=True)
            stream.write(line)
        if child.wait():
            raise RuntimeError(f'Command failed; see {log}')
    print(f'Completed in {time.monotonic() - started:.1f}s: {log.name}', flush=True)

common = ['--partitions', '1', '--batch-size', '2048', '--memory-limit', '2G', '--mem-pool-type', 'greedy']
reference_hashes = {}

def measure(variant, label, iterations):
    report = reports / variant / label
    report.mkdir(parents=True, exist_ok=True)
    files = output / 'parquet' / variant / label
    files.mkdir(parents=True, exist_ok=True)
    cases = [('write', ['local_parquet_write', '--output-dir', str(files)]),
             ('scan', ['parquet_row_filter_skip', '--query', '1', '--subgroup', 'skip', '--rows', '262144', '--rg-size', '8192'])]
    for name, args in cases:
        run([str(binaries / variant), *args, *common, '--iterations', str(iterations), '--output', str(report / f'{name}.json')], report / f'{name}.log', cwd=engine / 'benchmarks')
        result = json.loads((report / f'{name}.json').read_text())
        assert len(result['queries']) == (12 if name == 'write' else 1)
        assert all(q['success'] and len(q['iterations']) == iterations for q in result['queries'])
    layouts = []
    for file in sorted(files.glob('*.parquet')):
        parquet = pq.ParquetFile(file)
        meta = parquet.metadata
        table = parquet.read().combine_chunks().replace_schema_metadata(None)
        assert meta.num_rows == 131072 and table.num_rows == 131072
        buffer = pa.BufferOutputStream()
        with pa.ipc.new_stream(buffer, table.schema) as writer:
            writer.write_table(table)
        digest = hashlib.sha256(buffer.getvalue()).hexdigest()
        shape = file.stem.split('_')[0]
        expected = reference_hashes.setdefault(shape, digest)
        assert digest == expected, f'Content mismatch: {variant}/{label}/{file.stem}'
        groups = []
        for i in range(meta.num_row_groups):
            group = meta.row_group(i)
            assert 0 < group.num_rows <= 1000000
            groups.append({'rows': group.num_rows, 'uncompressed_bytes': group.total_byte_size,
                           'compressed_bytes': sum(group.column(j).total_compressed_size for j in range(group.num_columns))})
        layouts.append({'case': file.stem, 'rows': meta.num_rows, 'content_sha256': digest,
                        'file_bytes': file.stat().st_size, 'groups': groups})
    assert len(layouts) == 12, f'Expected twelve single-file write outputs, got {len(layouts)}'
    (report / 'layout.json').write_text(json.dumps(layouts, indent=2))
    print(f'Validated all write outputs: {variant}/{label}', flush=True)

(reports / 'environment.json').write_text(json.dumps({
    'platform': platform.platform(), 'cpu_count': os.cpu_count(),
    'rustc': subprocess.check_output(['rustc', '-Vv'], text=True),
    'lscpu': subprocess.check_output(['lscpu'], text=True),
    'pyarrow': pa.__version__, 'tokio_worker_threads': os.environ['TOKIO_WORKER_THREADS'],
    'run_id': os.environ.get('GITHUB_RUN_ID'), 'control_commit': os.environ.get('GITHUB_SHA'),
    'variants': variants,
}, indent=2))

subprocess.run(['git', 'fetch', '--no-tags', '--depth=1', 'origin', *variants.values()], cwd=engine, check=True)
shutil.copytree(Path(__file__).parent / 'local_parquet_write', engine / 'benchmarks/sql_benchmarks/local_parquet_write', dirs_exist_ok=True)
sink = engine / 'datafusion/datasource-parquet/src/sink.rs'
original = sink.read_bytes()
config_path = 'datafusion/common/src/config.rs'
config = (engine / config_path).read_text()
without_docs = lambda text: '\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('///'))
binaries = output / 'binaries'
binaries.mkdir(exist_ok=True)
try:
    for variant, commit in variants.items():
        variant_config = subprocess.check_output(['git', 'show', f'{commit}:{config_path}'], cwd=engine, text=True)
        assert without_docs(config) == without_docs(variant_config)
        source = subprocess.check_output(['git', 'show', f'{commit}:datafusion/datasource-parquet/src/sink.rs'], cwd=engine)
        sink.write_bytes(source)
        run(['cargo', 'build', '--locked', '--profile', 'release-nonlto', '-p', 'datafusion-benchmarks', '--bin', 'benchmark_runner'], reports / f'build-{variant}.log')
        executable = binaries / variant
        shutil.copyfile(Path(os.environ['CARGO_TARGET_DIR']) / 'release-nonlto/benchmark_runner', executable)
        executable.chmod(0o755)
        (reports / f'build-{variant}.json').write_text(json.dumps({
            'writer_commit': commit, 'writer_sha256': hashlib.sha256(source).hexdigest(),
            'normalized_config_sha256': hashlib.sha256(without_docs(config).encode()).hexdigest(),
            'binary_sha256': hashlib.sha256(executable.read_bytes()).hexdigest(),
            'note': 'Only sink.rs changes between builds; config code is identical and descriptions stay at base.'
        }, indent=2))
        # Smoke-test repeated COPY and readback before building the next writer.
        measure(variant, 'smoke', 2)
finally:
    sink.write_bytes(original)

for round_number, order in enumerate([list(variants), list(reversed(variants))], 1):
    for variant in order:
        measure(variant, f'round-{round_number}', 7)
subprocess.run([sys.executable, str(Path(__file__).with_name('summarize.py')), str(reports)], check=True)
