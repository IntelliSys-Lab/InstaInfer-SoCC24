import os
import sys
import pandas as pd
import numpy as np
from math import ceil
import pickle

sys.path.insert(1, '/Users/suiyifan/Downloads/azurefunctions-dataset2019/')

save_dir = "/Users/suiyifan/Downloads/azurefunctions-dataset2019/"
store = "/Users/suiyifan/Downloads/azurefunctions-dataset2019/"
buckets = [str(i) for i in range(1, 6)] # Get 5 minutes trace from Azure

datapath = "/Users/suiyifan/Downloads/azurefunctions-dataset2019/"
durations = "function_durations_percentiles.anon.d03.csv"
invocations = "invocations_per_function_md.anon.d03.csv"
mem_fnames = "app_memory_percentiles.anon.d03.csv"

quantiles = [0.0, 0.25, 0.5, 0.75, 1.0]


def load_data(file_path, dtype_dict):
    df = pd.read_csv(file_path, dtype=dtype_dict)
    df.index = df["HashFunction"]
    df = df.drop_duplicates("HashFunction")
    return df


def compute_interarrival_and_cv(invocation_counts):
    interarrival_times = []
    zero_counter = 0
    for value in invocation_counts:
        if value == 0:
            zero_counter += 1
        else:
            if zero_counter > 0:
                if value == 1:
                    interarrival_times.extend([zero_counter + 1] * value)
                else: # if value > 1
                    interarrival_times.extend([zero_counter + 1]) # first invocation
                    interarrival_times.extend([1/(value-1)] * (value-1)) # other invocations
                zero_counter = 0
            else:
                interarrival_times.extend([1/value] * value)
    mean_interarrival_time = np.mean(interarrival_times)
    std_interarrival_time = np.std(interarrival_times)
    cv_interarrival_time = std_interarrival_time / mean_interarrival_time if mean_interarrival_time > 0 else 0
    return mean_interarrival_time, cv_interarrival_time


def gen_traces1():
    global durations
    global invocations
    global memory

    def divive_by_func_num(row):
        return ceil(row["AverageAllocatedMb"] / group_by_app[row["HashApp"]])

    col_types = {str(i): int for i in range(1, 6)}
    durations_file = os.path.join(datapath, durations)
    invocations_file = os.path.join(datapath, invocations)

    durations = load_data(durations_file, col_types)
    invocations = load_data(invocations_file, col_types)

    group_by_app = durations.groupby("HashApp").size()

    sums = invocations.loc[:, "1":"5"].sum(axis=1)
    #print(sums)

    invocations = invocations[sums > 1]  # action must be invoked at least twice
    invocations = invocations.drop_duplicates("HashFunction")

    # Create a DataFrame to store average interarrival time and CV for each function
    stats = pd.DataFrame(columns=['HashFunction', 'AverageInterarrivalTime', 'CV'])

    # Iterate over each function and compute the average interarrival time and CV
    for index, row in invocations.iterrows():
        invocation_counts = row["1":"5"].values
        #print(invocation_counts)
        average_interarrival_time, cv_interarrival_time = compute_interarrival_and_cv(invocation_counts)
        stats = pd.concat([stats, pd.DataFrame([{'HashFunction': index,
                                                 'AverageInterarrivalTime': average_interarrival_time,
                                                 'CV': cv_interarrival_time}])], ignore_index=True)

    # Filter the DataFrame to find functions that meet the specified conditions
    suitable_functions = stats[(stats['CV'] <= 4) & (stats['CV'] >= 1) & (stats['AverageInterarrivalTime'] >= 0.1) & (
                stats['AverageInterarrivalTime'] <=10)]

    if suitable_functions.empty:
        print('No suitable function found.')
        return

    # Randomly select a function
    selected_functions = suitable_functions.sample(n=8)
    print(f'Selected functions: {selected_functions}')
    print(selected_functions['AverageInterarrivalTime'])

    selected_function_invocations = {
        selected_functions.iloc[0]["HashFunction"]: invocations.loc[selected_functions.iloc[0]["HashFunction"],
                                                    "1":"5"],
        selected_functions.iloc[1]["HashFunction"]: invocations.loc[selected_functions.iloc[1]["HashFunction"],
                                                    "1":"5"],
        selected_functions.iloc[2]["HashFunction"]: invocations.loc[selected_functions.iloc[2]["HashFunction"],
                                                    "1":"5"],
        selected_functions.iloc[3]["HashFunction"]: invocations.loc[selected_functions.iloc[3]["HashFunction"],
                                                    "1":"5"],
        selected_functions.iloc[4]["HashFunction"]: invocations.loc[selected_functions.iloc[4]["HashFunction"],
                                                    "1":"5"],
        selected_functions.iloc[5]["HashFunction"]: invocations.loc[selected_functions.iloc[5]["HashFunction"],
                                                    "1":"5"],
        selected_functions.iloc[6]["HashFunction"]: invocations.loc[selected_functions.iloc[6]["HashFunction"],
                                                    "1":"5"],
        selected_functions.iloc[7]["HashFunction"]: invocations.loc[selected_functions.iloc[7]["HashFunction"],
                                                    "1":"5"]
    }

    with open('selected_functions.pkl', 'wb') as f:
        pickle.dump(selected_functions, f)

    with open('selected_function_invocations.pkl', 'wb') as f:
        pickle.dump(selected_function_invocations, f)

gen_traces1()