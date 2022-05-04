# main
<pre>
usage: main.py [-h] [-n] [-c SCALING_CLASS] [-t TARGET_SIZE] [-s SEED] [-u] [data_source] [INPUT_FILE] [OUTPUT_FILE]

positional arguments:
  data_source           Data source (database or file of summaries stats) to be used to generate a new synthetic data set.[db, summaries] default is summaries.
  INPUT_FILE            path to the input file (read from stdin if omitted)
  OUTPUT_FILE           path to the output file (write to stdout if omitted)

optional arguments:
  -h, --help            show this help message and exit
  -n, --numerical       Flag the summaries in the input file as numerical (continuous).
  -c SCALING_CLASS, --scaling_class SCALING_CLASS
                        Entity (table) in the database used as reference dimension for scaling. Default is patient
  -t TARGET_SIZE, --target_size TARGET_SIZE
                        The desired number of synthetic records for the scaling class/the variables in the summaries.Default is 5000.
  -s SEED, --seed SEED  Set a seed (integer) for the sampling/normal distribution, useful for reproducibility. Default is 1.
  -u, --no_seed         Unseeded: don't use a seed for the sampling.
</pre>



# from_db

the script queries a database to get all the tables containing data (you can exclude some), get the number of rows for each and then get values counts for all the (categorical) attributes and dump a summary file with those.
at the same time it produces sample data for the attributes. 

the script focus at the moment at an intermine type db, and will be generalised.

to run:

``
python do_synth.py reference_class reference_sample_size
``

where reference_class is the class used to scale the sampling across all the database, with reference_sample_size the sample size for the reference class in the generated data.

e.g.

``
python do_synth.py patient 4000
``



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
