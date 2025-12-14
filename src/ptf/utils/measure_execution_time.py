import time
from functools import wraps
from typing import Callable


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
