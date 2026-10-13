"""Execute the pure-Python profiling notebook and save real outputs without Jupyter services.

Run from the repository root: python scripts/run_data_profiling.py
Requires pandas, numpy, matplotlib and IPython; no package installation is performed.
"""
import base64
import contextlib
import io
import json
import os
from pathlib import Path
import sys
import traceback

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import IPython.display

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
path = ROOT / 'notebooks/01_data_profiling.ipynb'
nb = json.loads(path.read_text(encoding='utf-8'))
namespace = {'__name__': '__main__'}
outputs = []

def display(*objects, **kwargs):
    for obj in objects:
        data = {'text/plain': repr(obj)}
        if hasattr(obj, '_repr_html_'):
            html = obj._repr_html_()
            if html:
                data['text/html'] = html
        outputs.append({'output_type': 'display_data', 'data': data, 'metadata': {}})

def show(*args, **kwargs):
    for number in plt.get_fignums():
        buffer = io.BytesIO()
        plt.figure(number).savefig(buffer, format='png', dpi=100, bbox_inches='tight')
        outputs.append({'output_type': 'display_data', 'data': {
            'image/png': base64.b64encode(buffer.getvalue()).decode('ascii'),
            'text/plain': '<Matplotlib figure>'}, 'metadata': {}})
    plt.close('all')

IPython.display.display = display
plt.show = show
execution = 0
for i, cell in enumerate(nb['cells']):
    if cell['cell_type'] != 'code':
        continue
    execution += 1
    outputs = []
    stdout, stderr = io.StringIO(), io.StringIO()
    print(f'Executing cell {i} ({execution})', flush=True)
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exec(compile(''.join(cell['source']), f'{path.name}:cell-{i}', 'exec'), namespace)
    except Exception as error:
        outputs.append({'output_type': 'error', 'ename': type(error).__name__,
                        'evalue': str(error), 'traceback': traceback.format_exc().splitlines()})
        cell['outputs'], cell['execution_count'] = outputs, execution
        path.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + '\n', encoding='utf-8')
        raise
    streams = [{'output_type': 'stream', 'name': name, 'text': stream.getvalue()}
               for name, stream in [('stdout', stdout), ('stderr', stderr)] if stream.getvalue()]
    cell['outputs'], cell['execution_count'] = streams + outputs, execution
path.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + '\n', encoding='utf-8')
print(f'Executed {execution} cells successfully; saved {path}')
