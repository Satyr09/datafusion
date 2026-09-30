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
base = '5bf6aef8e37c91d882c0f75b772201d62733c88f'
head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=engine, text=True).strip()
command = ['cargo', 'clippy', '--locked', '--all-targets', '--all-features']
results = []


def run(label, packages, expect_existing_warning=False):
    args = [*command]
    for package in packages:
        args.extend(['-p', package])
    args.extend(['--', '-D', 'warnings'])
    with (output / f'{label}.log').open('w') as log:
        result = subprocess.run(args, cwd=engine, stdout=log,
                                stderr=subprocess.STDOUT, timeout=3600)
    text = (output / f'{label}.log').read_text()
    if expect_existing_warning:
        assert result.returncode != 0, 'Expected the existing baseline lint to fail'
        assert 'datafusion/sqllogictest/src/engines/conversion.rs:99:' in text, text[-16000:]
        assert 'needless_pass_by_value' in text, text[-16000:]
        assert 'pub(crate) fn decimal_to_str(value: BigDecimal)' in text, text[-16000:]
    else:
        assert result.returncode == 0, text[-16000:]
    results.append({'label': label, 'commit': subprocess.check_output(
        ['git', 'rev-parse', 'HEAD'], cwd=engine, text=True).strip(),
        'command': args, 'returncode': result.returncode,
        'expected_existing_warning': expect_existing_warning})
    (output / 'lint-results.json').write_text(json.dumps(results, indent=2))
    print(f'{label}: expected outcome confirmed', flush=True)


run('changed_packages_all_features',
    ['datafusion', 'datafusion-common', 'datafusion-datasource-parquet'])
try:
    subprocess.run(['git', 'checkout', '--detach', base], cwd=engine, check=True)
    run('baseline_sql_all_features', ['datafusion-sqllogictest'],
        expect_existing_warning=True)
finally:
    subprocess.run(['git', 'checkout', '--detach', head], cwd=engine, check=True)

print('Changed packages pass strict all-features Clippy. The original base still '
      'has the SQL helper warning; the full workspace lint is not green.', flush=True)
