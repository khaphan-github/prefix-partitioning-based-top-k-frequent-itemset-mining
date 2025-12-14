"""
Parallel processing module for PTF algorithm.

Provides partition-level parallelization using thread pools for efficient
processing of prefix-based partitions on multi-core systems.
"""

from .partition_processor import ParallelPartitionProcessor

__all__ = ['ParallelPartitionProcessor']
