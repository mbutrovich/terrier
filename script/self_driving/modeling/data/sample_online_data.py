#!/usr/bin/env python3

import getopt
import os
import pandas as pd
import sys

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ior:", ["input=", "output=", "rate="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(err)  # will print something like "option -a not recognized"
        sys.exit(2)
    input_file = None
    output_folder = None
    rate = None
    for o, a in opts:
        if o in ['-i', '--input']:
            input_file = a
        elif o in ['-o', '--output']:
            output_folder = a
        elif o in ['-r', '--rate']:
            rate = int(a)
        else:
            assert False, "unhandled option"

    if input_file is None or output_folder is None or rate is None:
        print("Need input and output locations and rate.")
        sys.exit(2)

    assert (0 < rate < 100), "Invalid sampling rate."

    df = pd.read_csv(input_file)
    df.columns = df.columns.str.strip()

    filename = os.path.basename(input_file)
    (file, ext) = os.path.splitext(filename)

    df = df.sample(frac=rate / 100, random_state=1)
    df.to_csv("{}/{}_{}.csv".format(output_folder, file, rate), index=False)
