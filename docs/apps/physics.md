# Physics Simulations

This application is modified from the [Globus Compute Golf Demo](https://github.com/globus-labs/globus-compute-golf-demo){target=_blank}.

Simulates the physics of golf balls landing on a randomly generated golf green using perlin noise for terrain generation and [pybullet3](https://github.com/bulletphysics/bullet3){target=_blank} for physics simulations.
This application is embarrassingly parallel---every ball is simulated in its own task.

## Installation

This application requires numpy, matplotlib, scipy, and pybullet3 which can be installed automatically when installing the TaPS package.
```bash
pip install -e .[physics]
```

## Example

The following command simulated 32 balls where each ball is simulated in a separate task.
```bash
python -m taps.run --app physics --app.simulations 32 --engine.executor process-pool
```
The initial and final ball positions are plotted and saved to `{run_dir}/images/`.
By default, the simulations run at "real time".
I.e., sleeps are added at each timestep to ensure timesteps take as long as they would in real life.
This can be disabled with `--app.real-time false`.
