# python3 dynamic_synthetic_data.py time_interval_in_minutes

import os
import random
import threading
import time
import sys

def generate_random_file(filename, size_in_mb):
    """Create a file of a specified size filled with random bytes."""
    size_in_bytes = size_in_mb * 1024 * 1024
    with open(filename, 'wb') as f:
        f.write(os.urandom(size_in_bytes))

def create_file_every_interval(delay_in_minutes):
    filename = 'synthetic_data.txt'
    while True:
        random_size = random.randint(1, 100)  # Size between 1MB and 100MB
        generate_random_file(filename, random_size)
        print(f'Created {filename} with size {random_size}MB.')
        time.sleep(delay_in_minutes * 60)  # Convert minutes to seconds

# Set the delay in minutes here
delay_in_minutes = int(sys.argv[1])

# Start the function in a separate thread
t = threading.Thread(target=create_file_every_interval, args=(delay_in_minutes,))
t.daemon = True
t.start()

# Keep the main program running to allow the thread to continue executing
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Program terminated by user.")
