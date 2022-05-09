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
    print("======", args.data_source.lower())
    if args.data_source.lower() == 'db':
        do_sampling(args)
    elif args.numerical:
        read_continuous_risks(args)
    else:
        read_categorical_risks(args)

