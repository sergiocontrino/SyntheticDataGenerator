import argparse

# note: from summaries uses the target size for all variables

def get_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "data_source", nargs="?", type=str, default="summaries",
        metavar="SOURCE",
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
        "-c", "--scaling_class", type=str, default="patient",
        help="[db] Entity (table) in the database used as reference dimension for scaling. "
        "Default is patient")
    parser.add_argument(
        "-d", "--filter_dates", action="store_true",
        help="[db] Set the filter to work also on date fields (by default they are not filtered by threshold).")
    parser.add_argument(
        "-f", "--filter_threshold", type=int, default=1,
        help="[db] The minimum number of occurrences for a single value to be used in the generation."
        "Default is 1, i.e. no filtering.")
    parser.add_argument(
        "-n", "--numerical", action="store_true",
        help="[summaries] Flag the summaries in the input file as numerical (continuous).")
    parser.add_argument(
        "-s", "--seed", type=int, default=1,
        help="Set a seed (integer) for the sampling/normal distribution, useful for reproducibility. "
        "Default is 1.")
    parser.add_argument(
        "-t", "--target_size", type=int, default=5000,
        help="[db] The desired number of synthetic records for the scaling class/the variables in the summaries."
        "Default is 5000.")
    parser.add_argument(
        "-u", "--no_seed", action="store_true",
        help="Unseeded: don't use a seed for the sampling.")

    args = parser.parse_args()
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
