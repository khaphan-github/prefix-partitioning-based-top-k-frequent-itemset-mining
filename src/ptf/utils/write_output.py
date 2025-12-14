def write_output(text, output_file=None):
    """
    Write text to output file or print to stdout.
    
    Args:
        text: Text to output
        output_file: Optional file object to write to. If None, prints to stdout.
    """
    if output_file:
        output_file.write(text + "\n")
    else:
        print(text)
