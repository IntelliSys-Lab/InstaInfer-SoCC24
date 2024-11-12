import math
import pickle
import os
import time
import pandas as pd
import random

# Set display options in pandas to show all rows and columns without truncation
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

# Load selected functions and their invocation counts from pickle files
with open('selected_functions.pkl', 'rb') as f:
    selected_functions = pickle.load(f)

with open('selected_function_invocations.pkl', 'rb') as f:
    selected_function_invocations = pickle.load(f)

# Extract a list of selected functions, assuming we are interested in the first 8
selected_functions_list = [selected_functions.iloc[i] for i in range(8)]

# Get invocation counts for these selected functions
invocations_list = [selected_function_invocations[func["HashFunction"]] for func in selected_functions_list]

# Simulate function invocation over a period of 5 minutes
for min in range(1, 6):
    intervals = []

    # Calculate the interval at which each function should be invoked within each minute based on its count
    for i in range(8):
        count = invocations_list[i].get(str(min), 0)
        intervals.append(math.floor(60 / count) if count != 0 else 0)

    n_list = [1] * 8  # Initialize counters for each function

    for sec in range(1, 61):  # Traversal per second
        for i in range(8):
            if intervals[i] != 0 and (n_list[i] * intervals[i] > sec - 1 and n_list[i] * intervals[i] <= sec + 1):
                # We call each inference function "ptestXX"
                function_name = f"ptest0{i + 1}"
                random_delay = random.randint(1000, 3000) / 1000.0
                command = f'nohup bash -c "sleep {random_delay}; wsk action invoke {function_name} --result -i --blocking >> output.log 2>&1; echo $(date +%H:%M:%S) >> output.log" &'
                os.system(command)
                n_list[i] += 1

        time.sleep(1)  # Loop once every second.