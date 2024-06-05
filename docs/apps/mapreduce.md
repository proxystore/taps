# MapReduce

Counts words in a text corpus using a mapreduce strategy.

## Installation

This application only requires TaPS to be installed.

## Data

The Enron email dataset is available at https://www.cs.cmu.edu/~enron/.
The following command will download and extract the tarfile to `data/maildir`.

```bash
curl -L https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz | tar -xz -C data/
```

## Example

To see all parameters, run the following command:
```bash
python -m taps.run mapreduce --help
```

**Enron Corpus**

The following command distributes the text files of the Enron Corpus within `data/maildir` across 16 map tasks.
Once the computations have finished, the top 10 most common tokens will be printed.
```bash
python -m taps.run mapreduce --executor process-pool \
    --data-dir data/maildir --map-tasks 16
```

**Randomly Generated**

Here, we will generate 16 random files for each of 16 map tasks.
```bash
python -m taps.run mapreduce --executor process-pool \
    --data-dir /tmp/generated-files --map-tasks 16 \
    --generate true --generated-files 16
```
