import time
from functools import wraps
from tqdm import tqdm
from typing import Callable, Any


def measure_execution_time(func: Callable) -> Callable:
    """
    Decorator to measure and print execution time of a function.
    
    Args:
        func: The function to measure
        
    Returns:
        Wrapped function that prints execution time
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"\n[{func.__name__}] Execution time: {execution_time:.4f} seconds")
        return result
    return wrapper


def show_progress(iterable, desc: str = "", total: int = None):
    """
    Wrapper to show progress using tqdm.
    
    Args:
        iterable: The iterable to wrap
        desc: Description for the progress bar
        total: Total number of items (optional, auto-detected if not provided)
        
    Returns:
        tqdm progress bar wrapper
    """
    return tqdm(iterable, desc=desc, total=total, unit="item", ncols=80)


def execute_with_timing_and_progress(func: Callable, *args, desc: str = "", **kwargs) -> Any:
    """
    Execute a function with both timing and progress display.
    
    Args:
        func: The function to execute
        *args: Positional arguments for the function
        desc: Description for timing output
        **kwargs: Keyword arguments for the function
        
    Returns:
        Result from the function
    """
    start_time = time.time()
    description = desc or func.__name__
    print(f"\n[Starting] {description}...")
    
    result = func(*args, **kwargs)
    
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"[Completed] {description} - Time: {execution_time:.4f} seconds")
    
    return result


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
