# !/usr/bin/python
# -*- coding: utf-8 -*-
import random
from typing import NoReturn

import psycopg2

from config import config
import argparse
import pandas as pd


def main() -> NoReturn:
    """

    :return: none
    """


def get_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "target_size", nargs="?", default=20,
        help="The desired number of records for the scaling class",
        type=int)
    parser.add_argument(
        "input", nargs="?", default="./proto.csv",
        metavar="INPUT_FILE", type=argparse.FileType("r"),
        help="path to the input file (read from stdin if omitted)")
    parser.add_argument(
        "output", nargs="?", default="-",
        metavar="OUTPUT_FILE", type=argparse.FileType("w"),
        help="path to the output file (write to stdout if omitted)")
    args = parser.parse_args()
    print("==" * 20)
    print("Running with\ninput =", args.input.name, "\noutput  =", args.output.name)
    return args


def read_risks(args):
    """

    """
    # target_size = args.target_size
    risk, value, count, synthetic_risk = [], [], [], []
    ratio = 1
    i = 0
    for line in args.input:
        this_risk = line.split(',')[0]
        if i == 0:
            build_output(line, synthetic_risk, args)
            add_record(count, line, risk, value)
            i = i + 1
        else:
            prev_risk = risk[len(risk) - 1]
            if this_risk != prev_risk:
                # the RF changed, let's output the previous one
                # - first just some screen messages
                verbose_output(prev_risk, synthetic_risk)

                # - shuffle the synth list
                random.shuffle(synthetic_risk)

                # - make a df for export a csv list
                # TODO: there must be a better way...
                d = {"value": synthetic_risk}
                pd.DataFrame(d).to_csv('{0}.csv'.format(risk[len(risk) - 1]))

                # empty lists
                count, risk, synthetic_risk, value = [], [], [], []

                # start anew with current RF
                build_output(line, synthetic_risk, args)
                add_record(count, line, risk, value)
            else:
                # same RF: just append
                build_output(line, synthetic_risk, args)
                add_record(count, line, risk, value)
    # the last rf
    #    d = {"value": value, "count": count}
    random.shuffle(synthetic_risk)
    d = {"value": synthetic_risk}
    qq = pd.DataFrame(d)
    print(qq)
    qq.to_csv('{0}.csv'.format(risk[len(risk) - 1]))

    print("*" * 20)
    verbose_output(risk[len(risk) - 1], synthetic_risk)

    return risk, value, count


def verbose_output(risk, synthetic_risk):
    # TODO: case insensitive null
    print("-" * 20)
    print("Risk Factor", risk, ": size of synth data set =", len(synthetic_risk), "with",
          synthetic_risk.count("NULL"), "null values")
    print(synthetic_risk)


def build_output(line, risk_output, args):
    """
    add the value in synth data, proportionally to its original frequency,

    :param line:
    :param risk_output:
    :param args:
    :return:
    """

    # count/tot
    ratio = int((line.split(',')[3])) / int((line.split(',')[1]))
    # number of items of value value in the synth rf list
    rr = int(ratio * args.target_size)
    for t in range(rr):
        risk_output.append(line.split(',')[2])


def add_record(count, line, risk, value):
    risk.append(line.split(',')[0])
    value.append(line.split(',')[2])
    count.append(int(line.split(',')[3]))


if __name__ == '__main__':
    read_risks(get_args())
