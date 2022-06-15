# main
<pre>
usage: main.py [-h] [-c SCALING_CLASS] [-f FILTER_THRESHOLD] [-n] [-s SEED] [-t TARGET_SIZE] [-u] [SOURCE] [INPUT_FILE] [OUTPUT_FILE]

positional arguments:
  SOURCE                Data source (database or file of summaries stats) to be used to generate a new synthetic data set.[db, summaries] default is summaries.
  INPUT_FILE            path to the input file (read from stdin if omitted)
  OUTPUT_FILE           path to the output file (write to stdout if omitted)

optional arguments:
  -h, --help            show this help message and exit
  -c SCALING_CLASS, --scaling_class SCALING_CLASS
                        [db] Entity (table) in the database used as reference dimension for scaling. Default is patient
  -f FILTER_THRESHOLD, --filter_threshold FILTER_THRESHOLD
                        [db] The minimum number of occurrences for a single value to be used in the generation.Default is 1, i.e. no filtering.
  -n, --numerical       [summaries] Flag the summaries in the input file as numerical (continuous).
  -s SEED, --seed SEED  Set a seed (integer) for the sampling/normal distribution, useful for reproducibility. Default is 1.
  -t TARGET_SIZE, --target_size TARGET_SIZE
                        [db] The desired number of synthetic records for the scaling class/the variables in the summaries.Default is 5000.
  -u, --no_seed         Unseeded: don't use a seed for the sampling.
</pre>



# source: db

the script queries a database to get all the tables containing data (you can exclude some), get the number of rows for each and then get values counts for all the (categorical) attributes (columns).
If a threshold >1 is set, it will exclude from the count all values occurring < threshold times in the column.


examples:


``
python main.py db
``

uses all the default values: the reference class is <i>patient</i>, the reference size is <i>5000</i>, seed used for the sampling is <i>1</i>

``
python main.py -c referral -t 30000 db
``

the script runs with <i>referral</i> as the reference class; referral will be used to scale all the other outputs, and <i>30000</i> the sample size for the reference class in the generated data.


db connection to be stored in a database.ini file in the same directory.
format:

```
[label]
host=dbhost
database=dbname
user=dbuser
password=dbpassword
```
tested for postgresql

example of file:
```
[postgresql]
host=localhost
database=mydb
user=myuser
password=mypassword
```
