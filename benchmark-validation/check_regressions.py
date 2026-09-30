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

engine, output = [Path(arg).resolve() for arg in sys.argv[1:3]]
output.mkdir(parents=True, exist_ok=True)
sink = engine / 'datafusion/datasource-parquet/src/sink.rs'
original = sink.read_bytes()
stages = {
    'base': '5bf6aef8e37c91d882c0f75b772201d62733c88f',
    'error_fix': '450c5d779f3961f4823f616312d5f5e312c6110a',
    'refactor': '70904d01faa76d8f193ac79ea037fc0c41124420',
}
command = ['cargo', 'test', '--locked', '--profile', 'ci', '-p', 'datafusion',
           '--test', 'parquet_integration', '--features', 'parquet_encryption']
results = []


def test(label, pattern, expected_failure=None):
    with (output / f'{label}.log').open('w') as log:
        result = subprocess.run([*command, pattern, '--', '--nocapture'], cwd=engine,
                                stdout=log, stderr=subprocess.STDOUT, timeout=3600)
    text = (output / f'{label}.log').read_text()
    if expected_failure is None:
        assert result.returncode == 0, text[-16000:]
        assert '0 passed; 0 failed' not in text, 'No tests selected'
    else:
        assert result.returncode != 0, 'Regression unexpectedly passed without the fix'
        assert expected_failure in text, text[-16000:]
        assert 'test result: FAILED' in text, 'Expected a test assertion, not a build failure'
    results.append({'label': label, 'returncode': result.returncode,
                    'expected_failure': expected_failure})
    (output / 'results.json').write_text(json.dumps(results, indent=2))
    print(f'{label}: expected outcome confirmed', flush=True)


subprocess.run(['git', 'fetch', '--no-recurse-submodules', '--no-tags', '--depth=1',
                'origin', *stages.values()], cwd=engine, check=True)
try:
    test('revised', 'parquet::write_')
    sink.write_bytes(subprocess.check_output(['git', 'show', f'{stages["base"]}:datafusion/datasource-parquet/src/sink.rs'], cwd=engine))
    test('base_error_regression', 'parallel_write_returns_column_error_before_input_ends',
         'the writer kept consuming input after a column failed')
    test('base_byte_regression', 'parquet::write_row_groups',
         'the byte target must split each input batch')
    for name in ['error_fix', 'refactor']:
        sink.write_bytes(subprocess.check_output(['git', 'show', f'{stages[name]}:datafusion/datasource-parquet/src/sink.rs'], cwd=engine))
        test(name, 'parallel_write_returns_column_error_before_input_ends')
finally:
    sink.write_bytes(original)

test('parquet_integration', 'parquet::')
