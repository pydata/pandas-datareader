import subprocess
import sys


def test_import_pandas_datareader_smoke():
    cmd = [
        sys.executable,
        "-c",
        "import pandas; import pandas_datareader; print(pandas.__version__)",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
