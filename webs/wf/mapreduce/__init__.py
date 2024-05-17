"""MapReduce Workflow.

External Dependencies:
    This workflow does not require any external libraries.

Data Download Instructions:
    The Enron run mode requires the Maildir folder, which can be
    downloaded from https://www.cs.cmu.edu/~enron/.

Parameters and Results (Tested on M3 MacBook Air):
    To see all parameters, run the following command:
        python -m webs.run mapreduce --help

Example Commands and Results:
1. Basic Run:
    python -m webs.run mapreduce --executor thread-pool --mode random \
    --map-task-count 1
    Result:
        Total words: 500
        Runtime: 0.01s

2. Increased Map Task Count:
    Optionally, specify the number of most frequent words to save
    and the output file name:

    python -m webs.run mapreduce --executor thread-pool --mode random \
    --map-task-count 10 --n-freq 20 --out my-out.txt
    Result:
        Total words: 5000
        Runtime: 0.01s

3. Longer paragraphs with Longer Word Length:
    python -m webs.run mapreduce --executor thread-pool --mode random \
    --map-task-count 10 --word-count 1000000 --word-len-min 2 --word-len-max 2
    Result:
        Total words: 10000000
        Runtime: 7.09s

4. Invalid Word Length Range:
    python -m webs.run mapreduce --executor thread-pool --mode random \
    --map-task-count 10 --word-count 1000000 --word-len-min 5 --word-len-max 1
    Result:
        Error: ValueError: empty range for randrange() (5, 2, -3)

5. Enron Mode:
    Need to specify the root path of the maildir through --mail-dir:

    python -m webs.run mapreduce --executor thread-pool --mode enron \
    --map-task-count 10 --mail-dir ~/Downloads/maildir
    Result:
        Runtime: 52.12s
"""

from __future__ import annotations

import webs.wf.mapreduce.workflow
