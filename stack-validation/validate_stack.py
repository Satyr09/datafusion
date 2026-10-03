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

import json
from pathlib import Path
import subprocess
import sys

engine, control, output = [Path(p).resolve() for p in sys.argv[1:4]]
output.mkdir(parents=True, exist_ok=True)
base = "2566bc1052b32cce327b11d196a4dade52dd57d8"
results = []

def run(label, args, expected=None):
    print(f"START {label}: {' '.join(args)}", flush=True)
    with (output / f'{label}.log').open('w') as log:
        result = subprocess.run(args, cwd=engine, stdout=log, stderr=subprocess.STDOUT)
    contents = (output / f'{label}.log').read_text(errors='replace')
    if expected:
        ok = result.returncode != 0 and expected in contents and 'test result: FAILED' in contents
    else:
        ok = result.returncode == 0
    results.append({'label': label, 'returncode': result.returncode, 'passed': ok})
    (output / 'results.json').write_text(json.dumps(results, indent=2))
    if not ok:
        print(contents[-24000:], flush=True)
        raise RuntimeError(f'{label} failed')
    print(f'PASS {label}', flush=True)

extended = ['cargo', 'test', '--locked', '--profile', 'ci', '--exclude', 'datafusion-examples', '--exclude', 'datafusion-benchmarks', '--exclude', 'datafusion-cli', '--workspace', '--lib', '--tests', '--bins', '--features', 'avro,json,backtrace,extended_tests,recursive_protection,parquet_encryption']
focused = ['cargo', 'test', '--locked', '--profile', 'ci', '-p', 'datafusion', '--test', 'parquet_integration', '--features', 'parquet_encryption']
sink = engine / 'datafusion/datasource-parquet/src/sink.rs'
for stage in ['03-feature', '01-error-fix', '02-refactor']:
    run(f'{stage}-reset', ['git', 'reset', '--hard', base])
    run(f'{stage}-apply', ['git', 'apply', '--index', str(control / 'stack-validation' / f'{stage}.patch')])
    tree = subprocess.check_output(['git', 'write-tree'], cwd=engine, text=True).strip()
    (output / f'{stage}-tree.txt').write_text(tree + '\n')
    run(f'{stage}-fmt', ['cargo', 'fmt', '--all'])
    with (output / f'{stage}-formatting.patch').open('wb') as patch:
        subprocess.run(['git', 'diff', '--binary'], cwd=engine, stdout=patch, check=True)
    run(f'{stage}-fmt-clean', ['git', 'diff', '--exit-code'])
    run(f'{stage}-focused', [*focused, 'parquet::write_'])
    if stage == '01-error-fix':
        fixed = sink.read_bytes()
        sink.write_bytes(subprocess.check_output(['git', 'show', f'{base}:datafusion/datasource-parquet/src/sink.rs'], cwd=engine))
        try:
            run('base-error-ablation', [*focused, 'parallel_write_returns_column_error_before_input_ends', '--', '--nocapture'], 'the writer kept consuming input after a column failed')
        finally:
            sink.write_bytes(fixed)
    if stage == '03-feature':
        updated = sink.read_bytes()
        sink.write_bytes((control / 'stack-validation/arrow59-boundary-sink.rs').read_bytes())
        try:
            run('arrow59-boundary-ablation', [*focused, 'growing_strings', '--', '--nocapture'], 'serial and parallel layouts differ')
        finally:
            sink.write_bytes(updated)
        run('feature-docs-format', ['bash', './ci/scripts/doc_prettier_check.sh', '--write', '--allow-dirty'])
        with (output / 'feature-docs-formatting.patch').open('wb') as patch:
            subprocess.run(['git', 'diff', '--binary'], cwd=engine, stdout=patch, check=True)
        run('feature-docs-clean', ['git', 'diff', '--exit-code'])
        run('feature-generated-docs', ['bash', './dev/update_config_docs.sh', '--output-dir', str(output / 'generated')])
        run('feature-generated-docs-match', ['diff', '-u', 'docs/source/user-guide/configs.md', str(output / 'generated/configs.md')])
        run('feature-sql', ['cargo', 'test', '--locked', '--profile', 'ci', '-p', 'datafusion-sqllogictest', '--test', 'sqllogictests', '--', 'parquet_max_row_group_bytes'])
    run(f'{stage}-clippy', ['cargo', 'clippy', '--locked', '--all-targets', '--all-features', '--', '-D', 'warnings'])
    run(f'{stage}-extended', extended)
