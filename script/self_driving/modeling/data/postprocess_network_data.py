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
    output_file = None
    for o, a in opts:
        if o in "--input":
            input_file = a
        elif o in "--output":
            output_file = a
        else:
            assert False, "unhandled option"

    if input_file is None or output_file is None:
        print("Need input and output locations.")
        sys.exit(2)

    df = pd.read_csv(input_file)
    writes = df[df['op_unit'] == 2]  # filter only the writes
    writes = writes.drop(['op_unit'], axis=1)  # drop the op_unit column
    raw_data_map = {}
    for i, row in writes.iterrows():
        key = tuple(row[0:2])
        if key not in raw_data_map:
            raw_data_map[key] = []
        raw_data_map[key].append(row[2:])
    data_list = []
    for key in raw_data_map:
        data_list.append(list(key) + list(np.median(raw_data_map[key], axis=0)))

    final_output = pd.DataFrame(data_list, columns=writes.columns)
    print(final_output)
    final_output.to_csv("{}".format(output_file), index=False)
