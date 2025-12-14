from tqdm import tqdm


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
