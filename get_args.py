import argparse

# TODO: - add seeding y/n/value?
#       - add which type for summaries
#       - extract printing message (!= in different files)
# note: from summaries uses the target size for all variables


def get_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "data_source", nargs="?", type=str, default="summaries",
        help="Data source (database or file of summaries stats) to be used to generate a new synthetic data set."
        "[db, summaries] default is summaries.")
    parser.add_argument(
        "input", nargs="?", default="-",
        metavar="INPUT_FILE", type=argparse.FileType("r"),
        help="path to the input file (read from stdin if omitted)")
    parser.add_argument(
        "output", nargs="?", default="-",
        metavar="OUTPUT_FILE", type=argparse.FileType("w"),
        help="path to the output file (write to stdout if omitted)")
    parser.add_argument(
        "-n", "--numerical", action="store_true",
        help="Flag the summaries in the input file as numerical (continuous).")
    parser.add_argument(
        "-c", "--scaling_class", type=str, default="patient",
        help="Entity (table) in the database used as reference dimension for scaling. "
        "Default is patient")
    parser.add_argument(
        "-t", "--target_size", type=int, default=5000,
        help="The desired number of synthetic records for the scaling class/the variables in the summaries."
        "Default is 5000.")
    parser.add_argument(
        "-s", "--seed", type=int, default=1,
        help="Set a seed (integer) for the sampling/normal distribution, useful for reproducibility. "
        "Default is 1.")
    parser.add_argument(
        "-u", "--no_seed", action="store_true",
        help="Unseeded: don't use a seed for the sampling.")

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
