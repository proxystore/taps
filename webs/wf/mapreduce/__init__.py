"""Mapreduce workflow.

External dependencies: this workflow does not require external libraries.

Data download instructions: this workflow does not require external data.

Parameters and results (on M3 MacBook Air):

    python -m webs.run mapreduce --executor thread-pool \
        --map-task-word-count 1000000 --map-task-count 2 \
        --word-len-min 1 --word-len-max 1
    ==> runtime=1.19s

    python -m webs.run mapreduce --executor thread-pool \
        --map-task-word-count 1000000 --map-task-count 10 \
        --word-len-min 1 --word-len-max 1
    ==>  runtime=6.10s

    python -m webs.run mapreduce --executor thread-pool \
        --map-task-word-count 1000000 --map-task-count 10 \
        --word-len-min 2 --word-len-max 5
    ==>  runtime=13.57s

    python -m webs.run mapreduce --executor thread-pool \
        --map-task-word-count 100 --map-task-count 1 \
        --word-len-min 5 --word-len-max 1
    ==> ValueError: empty range for randrange() (5, 2, -3)

"""

from __future__ import annotations

import webs.wf.mapreduce.workflow
