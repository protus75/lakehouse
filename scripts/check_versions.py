"""Print versions of key packages in the workspace container."""
import subprocess

CONTAINER = "lakehouse-workspace"

PACKAGES = [
    "fitz",            # PyMuPDF
    "pymupdf",
    "pyarrow",
    "pyiceberg",
    "duckdb",
    "rapidfuzz",
    "marker",
    "dlt",
    "dagster",
    "requests",
]

py_code = """
import importlib
pkgs = %r
for name in pkgs:
    try:
        m = importlib.import_module(name)
        v = getattr(m, "__version__", None)
        if v is None and name == "fitz":
            v = getattr(m, "VersionBind", None) or getattr(m, "version", ("?",))[0]
        print(f"{name:12s} {v}")
    except Exception as e:
        print(f"{name:12s} NOT INSTALLED ({type(e).__name__})")
""" % PACKAGES

result = subprocess.run(
    ["docker", "exec", CONTAINER, "python", "-c", py_code],
    capture_output=True, text=True, encoding="utf-8",
)
print(result.stdout, end="")
if result.returncode != 0:
    print(result.stderr)
