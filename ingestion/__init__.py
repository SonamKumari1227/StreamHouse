"""Spark Structured Streaming jobs that land raw data in Bronze.

Submitted with spark-submit inside the Spark image, never run on the host. The modules here
import pyspark at module level and will not import without it; `make test-spark` runs their
tests in the image, where it exists.
"""
