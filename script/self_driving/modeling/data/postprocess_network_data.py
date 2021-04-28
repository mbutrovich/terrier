#!/usr/bin/env python3

import getopt
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
    writes.to_csv("{}".format(output_file), index=False)
