# Tiled Cholesky Decomposition

Computes the Cholesky decomposition of a randomly generated positive definite matrix.
Based on the tiled algorithm from [this paper](https://www.labri.fr/perso/ejeannot/publications/paap12.pdf){target=_blank}.

Tiled Cholesky decomposition is a canonical example of a dataflow-based workflow because of the inter-task dependencies between different tiles on the matrix.
This application is also data-intensive, with task input/output sizes being $O(b^2)$ where $b$ is the side length of each tile.

## Installation

This application requires numpy which can be installed automatically when installing the TaPS package.
```bash
pip install -e .[cholesky]
```

## Example

The following command computes the decomposition of a 10,000 x 10,000 matrix using 1000 x 1000 block/tile sizes.
```bash
$ python -m taps.run --app cholesky --engine.executor process-pool --app.matrix-size 10000 --app.block-size 1000
[2024-05-17 11:09:24.779] RUN   (taps.run) :: Starting application (name=cholesky)
...
[2024-05-17 11:09:24.810] APP  (taps.apps.cholesky) :: Input matrix: (10000, 10000)
[2024-05-17 11:09:24.810] APP  (taps.apps.cholesky) :: Block size: 1000
[2024-05-17 11:09:25.000] APP  (taps.apps.cholesky) :: Output matrix: (10000, 10000)
[2024-05-17 11:09:25.004] RUN   (taps.run) :: Finished application (name=cholesky, runtime=33.20s, tasks=385)
```
Here there are 100 tiles which results in 385 total tasks.
Using a smaller block/tile size would increase the total number of blocks, and therefore the total number of tasks.
This reduces the memory required per-task but also increases the cumulative task overheads.
