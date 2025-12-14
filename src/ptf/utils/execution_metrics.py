import time
import psutil
import os
import json
from contextlib import contextmanager
from typing import Optional


class ExecutionMetrics:
    """Track execution time and memory usage."""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.start_memory = None
        self.end_memory = None
        self.process = psutil.Process(os.getpid())
    
    def start(self):
        """Start timing and memory tracking."""
        self.start_time = time.time()
        self.start_memory = self.process.memory_info().rss / 1024  # KB
    
    def stop(self):
        """Stop timing and memory tracking."""
        self.end_time = time.time()
        self.end_memory = self.process.memory_info().rss / 1024  # KB
    
    @property
    def execution_time_ms(self):
        """Get execution time in milliseconds."""
        if self.start_time is not None and self.end_time is not None:
            return (self.end_time - self.start_time) * 1000
        return None
    
    @property
    def execution_time(self):
        """Get execution time in seconds (legacy)."""
        if self.start_time is not None and self.end_time is not None:
            return self.end_time - self.start_time
        return None
    
    @property
    def memory_used_kb(self):
        """Get memory used in KB."""
        if self.start_memory is not None and self.end_memory is not None:
            return self.end_memory - self.start_memory
        return None
    
    @property
    def memory_used(self):
        """Get memory used in MB (legacy)."""
        if self.start_memory is not None and self.end_memory is not None:
            return (self.end_memory - self.start_memory) / 1024
        return None
    
    def to_dict(self):
        """Export metrics as dictionary for JSON serialization."""
        return {
            "execution_time_ms": self.execution_time_ms,
            "memory_used_kb": self.memory_used_kb
        }
    
    def save_json(self, file_path: str):
        """Save metrics to JSON file."""
        with open(file_path, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)


@contextmanager
def track_execution():
    """Context manager to track execution metrics."""
    metrics = ExecutionMetrics()
    metrics.start()
    try:
        yield metrics
    finally:
        metrics.stop()
