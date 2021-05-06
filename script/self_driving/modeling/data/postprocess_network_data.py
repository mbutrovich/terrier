#!/usr/bin/env python3

import getopt
import numpy as np
import pandas as pd
import sys

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "io:cs", ["input=", "output=", "collapse", "strip"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(err)  # will print something like "option -a not recognized"
        sys.exit(2)
    input_file = None
    output_file = None
    collapse = False
    strip = False
    for o, a in opts:
        if o in ['-i', '--input']:
            input_file = a
        elif o in ['-o', '--output']:
            output_file = a
        elif o in ['-c', '--collapse']:
            collapse = True
        elif o in ['-s', '--strip']:
            strip = True
        else:
            assert False, "unhandled option"

    if input_file is None or output_file is None:
        print("Need input and output locations.")
        sys.exit(2)

    df = pd.read_csv(input_file)
    df.columns = df.columns.str.strip()
    writes = df[df['op_unit'] == 2]  # filter only the writes
    writes = writes.drop(['op_unit'], axis=1)  # drop the op_unit column
    if 'query_id' in writes.columns:
        writes = writes[writes['query_id'] != -1]  # remove -1 query_ids

    if strip:
        writes = writes.drop(['query_id'], axis=1)  # drop the query_id column

    if not collapse:
        writes.to_csv("{}".format(output_file), index=False)
        exit()

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
    final_output.to_csv("{}".format(output_file), index=False)
