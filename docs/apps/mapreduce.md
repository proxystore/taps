# MapReduce

Counts words in a text corpus (randomly generated or the Enron email dataset) using a mapreduce strategy.

## Setup

This workflow does not require any external libraries.

Executing the workflow with the Enron dataset requires downloading the maildir folder from https://www.cs.cmu.edu/~enron/.

## Examples

To see all parameters, run the following command:
```bash
python -m taps.run mapreduce --help
```

The following commands work on any platform, but the specific results were recorded on an M3 MacBook Air.

1. **Simple configuration:**
   ```bash
   python -m taps.run mapreduce --executor thread-pool --mode random --map-task-count 1
   ```
   ```
   Result:
     Total words: 500
     Runtime: 0.01s
   ```
2. **Increased map task count:**
   Optionally, specify the number of most frequent words to save and the
   output file name:
   ```bash
   python -m taps.run mapreduce --executor thread-pool --mode random --map-task-count 10 --n-freq 20 --out my-out.txt
   ```
   ```
   Result:
     Total words: 5000
     Runtime: 0.01s
   ```
3. **Longer paragraphs with longer word length:**
   ```bash
   python -m taps.run mapreduce --executor thread-pool --mode random --map-task-count 10 --word-count 1000000 --word-len-min 2 --word-len-max 2
   ```
   ```
   Result:
     Total words: 10000000
     Runtime: 7.09s
   ```
4. **Invalid word length range:**
   ```bash
   python -m taps.run mapreduce --executor thread-pool --mode random --map-task-count 10 --word-count 1000000 --word-len-min 5 --word-len-max 1
   ```
   ```
   Result:
     Error: ValueError: empty range for randrange() (5, 2, -3)
   ```
5. **Use the Enron email dataset:**
   This requires specifying the root path of the maildir through `--mail-dir`.
   ```bash
   python -m taps.run mapreduce --executor thread-pool --mode enron --map-task-count 10 --mail-dir ~/Downloads/maildir
   ```
   ```
   Result:
     Runtime: 52.12s
   ```
