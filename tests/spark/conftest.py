"""Keep the host test run from importing PySpark.

The marker in pyproject.toml deselects these tests on the host, but deselection happens
*after* collection, and collection imports the module - which imports pyspark, which is
deliberately not installed here. Without this the whole host suite dies on a collection
error instead of quietly skipping twelve tests.
"""

from importlib.util import find_spec

collect_ignore_glob = [] if find_spec("pyspark") else ["test_*.py"]
