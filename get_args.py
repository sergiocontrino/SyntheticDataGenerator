import argparse

# TODO: - add seeding y/n/value?
#       - add which type for summaries
#       - extract printing message (!= in different files)
# note: from summaries uses the target size for all variables


def get_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "scaling_class", nargs="?", default=get_scaling_class(),
        help="The class used as dimension reference in the db",
        type=str)
    parser.add_argument(
        "target_size", nargs="?", default=get_target_size(),
        help="The desired number of records for the scaling class",
        type=int)
    parser.add_argument(
        "-s", "--seed", type=int, default=1,
        help="Set a seed (integer) for the sampling/normal distribution, useful for reproducibility. "
        "Default is 1")
    parser.add_argument(
        "-ns", "--no_seed", action="store_true",
        help="Don't use a seed for the sampling.")
    parser.add_argument(
        "input", nargs="?", default="./numproto.csv",
        metavar="INPUT_FILE", type=argparse.FileType("r"),
        help="path to the input file (read from stdin if omitted)")
    parser.add_argument(
        "output", nargs="?", default="-",
        metavar="OUTPUT_FILE", type=argparse.FileType("w"),
        help="path to the output file (write to stdout if omitted)")
    args = parser.parse_args()
    print("==" * 20)
    print("Running with\ninput =", args.input.name, "\noutput  =", args.output.name)
    print("Target size for synthetic datasets:", str(args.target_size))
    print("Seed used in the categorical sampling:", str(args.seed))
    return args


def get_scaling_class() -> str:
    """
    sets the default class we use to scale the db
    """
    scaling_class = "patient"
    return scaling_class


def get_target_size() -> int:
    """
    sets the default target size of the scaling class
    """
    scaling_size = 5000
    return scaling_size
