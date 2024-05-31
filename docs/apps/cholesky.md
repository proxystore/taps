# Tiled Cholesky Factorization

Computes the Cholesky decomposition of a randomly generated positive definite matrix.
Based on the algorithm from [this paper](https://www.labri.fr/perso/ejeannot/publications/paap12.pdf){target=_blank}.

## Example

The following command computes the decomposition of a 1000 x 1000 matrix using 100 x 100 block/tile sizes.
```bash
$ python -m taps.run cholesky --executor process-pool --matrix-size 1000 --block-size 100
[2024-05-17 11:09:24.779] RUN   (taps.run) :: Starting application (name=cholesky)
...
[2024-05-17 11:09:24.810] APP  (taps.apps.cholesky) :: Input matrix: (1000, 1000)
[2024-05-17 11:09:24.810] APP  (taps.apps.cholesky) :: Block size: 100
[2024-05-17 11:09:25.000] APP  (taps.apps.cholesky) :: Output matrix: (1000, 1000)
[2024-05-17 11:09:25.004] RUN   (taps.run) :: Finished application (name=cholesky, runtime=0.23s)
```
