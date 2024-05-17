"""Tiled Cholesky decomposition workflow.

Computes the Cholesky decomposition of a randomly generated positive definite
matrix. Based on the workflow from
[this paper](https://www.labri.fr/perso/ejeannot/publications/paap12.pdf){target=_blank}.

Example:
    The following command computes the decomposition of a 1000 x 1000 matrix
    using 100 x 100 block/tile sizes.
    ```bash
    $ python -m webs.run cholesky --executor process-pool --n 1000 --block-size 100
    [2024-05-17 11:09:24.779] RUN   (webs.run) :: Starting workflow (name=cholesky)
    ...
    [2024-05-17 11:09:24.810] WORK  (webs.wf.cholesky.workflow) :: Input matrix: (1000, 1000)
    [2024-05-17 11:09:24.810] WORK  (webs.wf.cholesky.workflow) :: Block size: 100
    [2024-05-17 11:09:25.000] WORK  (webs.wf.cholesky.workflow) :: Output matrix: (1000, 1000)
    [2024-05-17 11:09:25.004] RUN   (webs.run) :: Finished workflow (name=cholesky, runtime=0.23s)
    ```
"""  # noqa: E501

from __future__ import annotations

import webs.wf.cholesky.workflow
