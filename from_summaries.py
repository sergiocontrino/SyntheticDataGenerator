# !/usr/bin/python
# -*- coding: utf-8 -*-
import random
from typing import NoReturn

import psycopg2

from config import config
import argparse

import pandas as pd
import numpy as np
# Needed for plotting
import matplotlib.colors
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

# Needed for generating classification, regression and clustering datasets
import sklearn.datasets as dt

# Needed for generating data from an existing dataset
from sklearn.neighbors import KernelDensity
from sklearn.model_selection import GridSearchCV

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
    """parse a fasta file

    :param args: the input file
    :return: the lists of contigs: id, seq, seq lengths
    """

    risk = []
    value = []
    count = []
    risk_output = []
    ratio = 1
    i = 0
    for line in args.input:
        #print("==", i)
        this_risk = line.split(',')[0]
        if i == 0:
            build_output(line, risk, risk_output, value, count, args)
            i = i+1
        else:
            if line.split(',')[0] != risk[len(risk)-1]:
                print("se cambia")
                # -> risk df
                print(risk_output)
                print(len(risk_output))
                print(risk_output.count("Null"))
                random.shuffle(risk_output)
                print(line.split(',')[0])
                #d = {"value": value, "count": count}
                d = {"value": risk_output}
                qq = pd.DataFrame(d)
                print(qq)
                qq.to_csv('{0}.csv'.format(risk[len(risk)-1]))
                #qq.sample(n=20, replace=True).to_csv('{0}.csv'.format(risk[len(risk)-1].join("sampled")))
                # empty lists
                risk = []
                value = []
                count = []
                risk_output = []
                # start anew
                build_output(line, risk, risk_output, value, count, args)
                #add_record(count, line, risk, value)
            else:
                # just append
                build_output(line, risk, risk_output, value, count, args)
                #add_record(count, line, risk, value)
    # the last rf
#    d = {"value": value, "count": count}
    d = {"value": risk_output}
    qq = pd.DataFrame(d)
    print(qq)
    qq.to_csv('{0}.csv'.format(risk[len(risk) - 1]))

    print("*" * 20)
    print(risk_output)
    print(len(risk_output))

#    print("-" * 20)
#    print(risk)
#    print(value)
#    print(count)

#    print(risk.count('sex'))
    return risk, value, count


def build_output(line, risk, risk_output, value, count, args):
    ratio = int((line.split(',')[3])) / int((line.split(',')[1]))
 #   print(ratio, line.split(',')[3])
    rr = int(ratio * args.target_size)
    for t in range(rr):
        risk_output.append(line.split(',')[2])
    #print("->", risk_output)
    risk.append(line.split(',')[0])
    value.append(line.split(',')[2])
    count.append(int(line.split(',')[3]))


def add_record(count, line, risk, value):
    risk.append(line.split(',')[0])
    value.append(line.split(',')[2])
    count.append(int(line.split(',')[3]))


"""
# Define the seed so that results can be reproduced
seed = 11
rand_state = 11

# Define the color maps for plots
color_map = plt.cm.get_cmap('RdYlBu')
color_map_discrete = matplotlib.colors.LinearSegmentedColormap.from_list("", ["red", "cyan", "magenta", "blue"])

rand = np.random.RandomState(seed)

dist_list = ['uniform','normal','exponential','lognormal','chisquare','beta']
param_list = ['-1,1','0,1','1','0,1','2','0.5,0.9']
colors_list = ['green','blue','yellow','cyan','magenta','pink']

fig, ax = plt.subplots(nrows=2, ncols=3, figsize=(12, 7))
plt_ind_list = np.arange(6) + 231

for dist, plt_ind, param, colors in zip(dist_list, plt_ind_list, param_list, colors_list):
    x = eval('rand.' + dist + '(' + param + ',5000)')
    print("->", x)
    plt.subplot(plt_ind)
    plt.hist(x, bins=50, color=colors)
    plt.title(dist)

fig.subplots_adjust(hspace=0.4, wspace=.3)
plt.suptitle('Sampling from Various Distributions', fontsize=20)
#plt.show()
"""

if __name__ == '__main__':
    read_risks(get_args())



