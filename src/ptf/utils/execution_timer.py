import time


class ExecutionTimer:
    """
    Context manager to measure execution time of a code block.
    
    Example:
        with ExecutionTimer("Processing data"):
            # code to measure
            pass
    """
    def __init__(self, label: str = "Execution"):
        self.label = label
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        print(f"\n[Starting] {self.label}...")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        execution_time = self.end_time - self.start_time
        print(f"[Completed] {self.label} - Time: {execution_time:.4f} seconds")
        return False
    
    @property
    def elapsed(self) -> float:
        """Get elapsed time if timer has ended."""
        if self.end_time is None:
            return None
        return self.end_time - self.start_time
