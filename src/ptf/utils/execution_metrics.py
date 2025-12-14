import time
import psutil
import os
from contextlib import contextmanager


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
        self.start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
    
    def stop(self):
        """Stop timing and memory tracking."""
        self.end_time = time.time()
        self.end_memory = self.process.memory_info().rss / 1024 / 1024  # MB
    
    @property
    def execution_time(self):
        """Get execution time in seconds."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None
    
    @property
    def memory_used(self):
        """Get memory used in MB."""
        if self.start_memory and self.end_memory:
            return self.end_memory - self.start_memory
        return None


@contextmanager
def track_execution():
    """Context manager to track execution metrics."""
    metrics = ExecutionMetrics()
    metrics.start()
    try:
        yield metrics
    finally:
        metrics.stop()
