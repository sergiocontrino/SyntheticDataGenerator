# !/usr/bin/python
# -*- coding: utf-8 -*-
from typing import NoReturn
from get_args import get_args
from from_db import do_sampling
from from_summaries import read_categorical_risks
from from_summaries import read_continuous_risks



def main() -> NoReturn:
    """

    :return: none
    """


if __name__ == '__main__':
    args = get_args()

    # TODO: adds other pars
    print("==" * 20)
    # print("Running with\ninput =", args.input.name, "\noutput  =", args.output.name)

    if args.data_source.lower() == 'db':
        print("Scaling class:", str(args.scaling_class))
        print("Target size for synthetic datasets:", str(args.target_size))
        print("Filter threshold:", str(args.filter_threshold))
        print("Filtering dates:", str(args.filter_dates))
        do_sampling(args)
    elif args.numerical:
        read_continuous_risks(args)
    else:
        print("Seed used in the categorical sampling:", str(args.seed))
        read_categorical_risks(args)

