"""
Hybrid Vertical Storage Mode for PTF

Implements optimized tid-set storage using three formats:
- Tid-list: for rare items (low support)
- Dif-list: for common items (high support)  
- Bit-vector: for medium support items

Reduces I/O and memory compared to naive tid-list approach.
"""

from .tid_set_entry import TidSetEntry
from .hybrid_vertical_index import HybridVerticalIndex
from .hybrid_tidset_intersection import HybridTidSetIntersection

__all__ = [
    'TidSetEntry',
    'HybridVerticalIndex',
    'HybridTidSetIntersection',
]
