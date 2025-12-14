import time
from typing import Callable, Any


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
