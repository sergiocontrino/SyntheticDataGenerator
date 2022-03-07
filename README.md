# get_summaries

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
