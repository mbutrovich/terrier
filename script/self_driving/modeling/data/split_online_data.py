#!/usr/bin/env python3

import getopt
import numpy as np
import pandas as pd
import sys

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "io:", ["input=", "output="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(err)  # will print something like "option -a not recognized"
        sys.exit(2)
    input_file = None
    output_folder = None
    print(opts)
    for o, a in opts:
        if o in "--input":
            input = a
        elif o in "--output":
            output = a
        else:
            assert False, "unhandled option"

    if input_file is None or output_folder is None:
        print("Need input and output locations.")
        sys.exit(2)

    df = pd.read_csv(input_file)
    unique_query_ids = pd.unique(df['query_id'])
    np.random.shuffle(unique_query_ids)
    split_query_ids = np.array_split(unique_query_ids, 5)
    for i in range(5):
        training_data = df[~df.query_id.isin(split_query_ids[i])]
        test_data = df[df.query_id.isin(split_query_ids[i])]
        training_data.to_csv("{}/pipeline_training_{}.csv".format(output_folder, i),
                             index=False)
        test_data.to_csv("{}/pipeline_test_{}.csv".format(output_folder, i), index=False)
