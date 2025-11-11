from __future__ import annotations

import logging
import math
import pathlib
import random
import sys
import time
from dataclasses import dataclass

import matplotlib.pyplot as plt
import numpy
import pybullet
import pybullet_data
from noise import pnoise2
from numpy.typing import NDArray
from scipy.ndimage import gaussian_filter
from scipy.stats import gaussian_kde

from taps.engine import Engine
from taps.engine import task

logger = logging.getLogger(__name__)

Position = tuple[float, float, float]
Vertices = list[Position]
Indices = list[int]
TerrainPart = tuple[Vertices, Indices]
# The two floats here represent the x, y positions where the terrain
# part starts at.
Terrain = list[tuple[float, float, TerrainPart]]

# PyBullet has a limit of 131,072 vertices for a mesh map on Windows/Linux
# but only 8,192 on MacOS due to the smaller shared memory default on MacOS.
#   https://github.com/bulletphysics/bullet3/issues/1965
# So we need to split our terrain into smaller chunks. This parameter
# controls the maximum number of points in each chunk.
# These numbers are chosen to be half the max vertex count in pybullet.
#  https://github.com/bulletphysics/bullet3/blob/e9c461b0ace140d5c73972760781d94b7b5eee53/examples/SharedMemory/SharedMemoryPublic.h#L1128-L1134
if sys.platform == 'darwin':
    MAX_VERTICES_PER_MESH = 4192
else:
    MAX_VERTICES_PER_MESH = 65536


@dataclass
class TerrainConfig:
    """Terrain configuration."""

    width: int
    height: float
    resolution: int
    scale: float
    octaves: int
    persistence: float
    lacunarity: float
    filter_size: int


@dataclass
class SimulationConfig:
    """Simulation configuration."""

    ball_diameter: float
    ball_mass: float
    tick_rate: int
    total_time: int
    real_time: bool


def create_contour_plot(
    initial_positions: list[Position],
    final_positions: list[Position],
    heightmap: NDArray[numpy.float64],
    config: TerrainConfig,
    filepath: str | pathlib.Path,
) -> None:
    """Write contour plot with initial/final ball positions.

    Args:
        initial_positions: Initial ball positions.
        final_positions: Final ball positions.
        heightmap: Terrain heightmap.
        config: Terrain configuration.
        filepath: Output file.
    """
    fig, axs = plt.subplots(1, 2, sharey=True, figsize=(8, 4))

    x = numpy.linspace(0, config.width, num=config.width * config.resolution)
    y = numpy.linspace(0, config.width, num=config.width * config.resolution)
    for ax, positions in zip(
        axs,
        (initial_positions, final_positions),
        strict=False,
    ):
        handle = ax.contour(x, y, heightmap, levels=10)
        plt.clabel(handle, inline=True)
        px, py = (
            # Clamp balls that may have rolled off the surface to the
            # edge of the plot.
            [max(0, min(p[0], config.width)) for p in positions],
            [max(0, min(p[1], config.width)) for p in positions],
        )
        ax.scatter(px, py, s=16, c='#FFFFFF', zorder=100)

    for i, (ax, title) in enumerate(
        zip(axs, ('Initial', 'Final'), strict=False),
    ):
        ax.set_title(title)
        ax.set_xlim(0, config.width)
        ax.set_ylim(0, config.width)
        ax.set_xlabel('X Position (m)')
        if i == 0:
            ax.set_ylabel('Y Position (m)')
        ax.set_facecolor('#58a177')

    fig.tight_layout(w_pad=2)

    pathlib.Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(filepath, pad_inches=0.05, dpi=300)


def create_terrain_plot(
    initial_positions: list[Position],
    final_positions: list[Position],
    heightmap: NDArray[numpy.float64],
    config: TerrainConfig,
    filepath: str | pathlib.Path,
) -> None:
    """Write 3D terrain mesh with heatmap of final ball positions.

    Args:
        initial_positions: Initial ball positions.
        final_positions: Final ball positions.
        heightmap: Terrain heightmap.
        config: Terrain configuration.
        filepath: Output file.
    """
    fig = plt.figure(figsize=(8, 4))
    axs = [
        fig.add_subplot(1, 2, 1, projection='3d'),
        fig.add_subplot(1, 2, 2, projection='3d'),
    ]
    plt.subplots_adjust(wspace=0.5, hspace=0.5)

    x = numpy.arange(0, config.width, 1 / config.resolution)
    y = numpy.arange(0, config.width, 1 / config.resolution)
    x, y = numpy.meshgrid(x, y)

    for ax, positions in zip(
        axs,
        (initial_positions, final_positions),
        strict=False,
    ):
        px, py, _ = zip(*positions, strict=False)
        xy = numpy.vstack([px, py])
        kde = gaussian_kde(xy)(xy)
        kde_grid = numpy.zeros_like(heightmap)

        for i, (xi, yi) in enumerate(zip(px, py, strict=False)):
            # Clamp positions to be in [0, config.width]. If a ball rolled
            # off the edge, it's position would be outside the mesh map.
            max_index = (config.width * config.resolution) - 1
            xi_scaled = int(xi * config.resolution)
            yi_scaled = int(yi * config.resolution)
            xi_scaled = max(0, min(xi_scaled, max_index))
            yi_scaled = max(0, min(yi_scaled, max_index))
            kde_grid[yi_scaled, xi_scaled] = kde[i]

        kde_smoothed = gaussian_filter(kde_grid, sigma=1)
        ax.plot_surface(
            x,
            y,
            heightmap,
            facecolors=plt.cm.jet(kde_smoothed / kde_smoothed.max()),
            alpha=0.9,
        )

    for ax, title in zip(axs, ('Initial', 'Final'), strict=False):
        ax.set_title(title, pad=-20)
        ax.set_xlim(0, config.width)
        ax.set_ylim(0, config.width)
        ax.set_zlim(0, 2 * heightmap.max())
        ax.set_xlabel('X Position (m)')
        ax.set_ylabel('Y Position (m)')
        ax.set_zlabel('Z Position (m)')

    fig.tight_layout()
    fig.subplots_adjust(wspace=0.15, left=0, right=0.92, bottom=0.05, top=0.98)

    pathlib.Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(filepath, dpi=300)


def generate_initial_positions(
    num_balls: int,
    config: TerrainConfig,
    seed: int | None = None,
) -> list[Position]:
    """Generate initial ball positions.

    Args:
        num_balls: Number of balls.
        config: Terrain config used to bound locations of balls.
        seed: Random seed for initial positions.

    Returns:
        List of ball positions.
    """
    buffer = 0.2 * config.width
    min_width, max_width = buffer, config.width - buffer

    random.seed(seed)

    def _generate() -> Position:
        return (
            random.uniform(min_width, max_width),
            random.uniform(min_width, max_width),
            2 * config.height,
        )

    return [_generate() for _ in range(num_balls)]


def generate_noisemap(
    config: TerrainConfig,
    seed: int | None = None,
) -> NDArray[numpy.float64]:
    """Generate Perlin noise map for terrain generation.

    Args:
        config: Terrain configuration.
        seed: Random seed.

    Returns:
        Noise map.
    """
    dimension = config.width * config.resolution
    heightmap = numpy.zeros((dimension, dimension))

    offset = seed if seed is not None else random.randint(0, 1_000_000)

    for i in range(dimension):
        for j in range(dimension):
            x = (i / config.resolution) / config.scale
            y = (j / config.resolution) / config.scale
            heightmap[i][j] = pnoise2(
                x + offset,
                y + offset,
                octaves=config.octaves,
                persistence=config.persistence,
                lacunarity=config.lacunarity,
            )

    # Smooth terrain with gaussian filter
    heightmap = gaussian_filter(heightmap, config.filter_size)
    # Scale terrain height to be [0, config.height]
    old_min, old_max = heightmap.min(), heightmap.max()
    return (heightmap - old_min) * config.height / old_max


def _generate_vertices(
    heightmap: NDArray[numpy.float64],
    resolution: int,
) -> TerrainPart:
    vertices: Vertices = []
    indices: Indices = []

    width, height = heightmap.shape[0], heightmap.shape[1]

    for i in range(width):
        for j in range(height):
            # Each vertex is represented by (x, y, z), where:
            # x = column index (j), y = row index (i), z = height value
            ii, jj = i / resolution, j / resolution
            vertices.append((jj, ii, heightmap[i][j]))

    for i in range(width - 1):
        for j in range(height - 1):
            # Define two triangles for each grid square
            # Triangle 1
            i1 = i * height + j
            i2 = i1 + 1
            i3 = i1 + height
            # Triangle 2
            i4 = i3
            i5 = i2
            i6 = i3 + 1
            indices.extend([i1, i2, i3, i4, i5, i6])

    return vertices, indices


def generate_vertices(
    heightmap: NDArray[numpy.float64],
    config: TerrainConfig,
) -> Terrain:
    """Generate vertex mesh from heighmap.

    Args:
        heightmap: Terrain heighmap.
        config: Terrain config.

    Returns:
        Vertex mesh.
    """
    width, height = heightmap.shape[0], heightmap.shape[1]
    vertices = width * height
    parts = math.ceil(vertices / MAX_VERTICES_PER_MESH)
    max_width_per_part = math.ceil(width / parts)

    terrain: Terrain = []
    for x1 in range(0, width, max_width_per_part):
        # Add one to right index of slice so that the terrain parts
        # overlap each other slightly.
        x2 = min(width, x1 + max_width_per_part + 1)
        # For now, we only split on axis 0 (referred to as x-axis).
        y = 0 / config.resolution
        heightmap_part = heightmap[x1:x2, :]
        terrain_part = _generate_vertices(heightmap_part, config.resolution)
        terrain.append((x1 / config.resolution, y, terrain_part))

    return terrain


def _create_terrain_body(terrain: Terrain) -> list[int]:
    terrain_ids: list[int] = []

    for part in terrain:
        x, y, (vertices, indices) = part

        collision_shape_id = pybullet.createCollisionShape(
            shapeType=pybullet.GEOM_MESH,
            meshScale=[1, 1, 1],
            vertices=vertices,
            indices=indices,
        )

        visual_shape_id = pybullet.createVisualShape(
            shapeType=pybullet.GEOM_MESH,
            meshScale=[1, 1, 1],
            vertices=vertices,
            indices=indices,
            rgbaColor=[88 / 255, 161 / 255, 119 / 255, 1],
            specularColor=[0.4, 0.4, 0],
        )

        terrain_id = pybullet.createMultiBody(
            baseMass=0,
            baseCollisionShapeIndex=collision_shape_id,
            baseVisualShapeIndex=visual_shape_id,
            basePosition=[y, x, 0],
        )

        terrain_ids.append(terrain_id)

    return terrain_ids


def _create_ball_body(
    position: Position,
    radius: float = 0.1,
    mass: float = 0.1,
    max_velocity: float = 1.0,
) -> int:
    ball_visual_shape_id = pybullet.createVisualShape(
        shapeType=pybullet.GEOM_SPHERE,
        radius=radius,
        rgbaColor=[1, 1, 1, 1],
        specularColor=[0.4, 0.4, 0],
    )

    ball_collision_shape_id = pybullet.createCollisionShape(
        shapeType=pybullet.GEOM_SPHERE,
        radius=radius,
    )

    ball_id = pybullet.createMultiBody(
        baseMass=mass,
        baseCollisionShapeIndex=ball_collision_shape_id,
        baseVisualShapeIndex=ball_visual_shape_id,
        basePosition=position,
    )

    pybullet.changeDynamics(
        ball_id,
        -1,
        lateralFriction=0.2,
        rollingFriction=0.05,
        spinningFriction=0.05,
    )

    initial_velocity = [
        random.uniform(-max_velocity, max_velocity),
        random.uniform(-max_velocity, max_velocity),
        0,
    ]
    pybullet.resetBaseVelocity(ball_id, linearVelocity=initial_velocity)

    return ball_id


@task()
def simulate(
    terrain: Terrain,
    positions: list[Position],
    sim_config: SimulationConfig,
    terrain_config: TerrainConfig,
) -> list[Position]:
    """Simulate balls landing on terrain.

    Args:
        terrain: Terrain.
        positions: Initial ball positions.
        sim_config: Simulation configuration.
        terrain_config: Terrain configuration.

    Returns:
        List of final ball positions.
    """
    pybullet.connect(pybullet.DIRECT)
    pybullet.setAdditionalSearchPath(pybullet_data.getDataPath())
    pybullet.setTimeStep(1 / sim_config.tick_rate)
    pybullet.setGravity(0, 0, -9.81)

    _create_terrain_body(terrain)

    ball_ids = [
        _create_ball_body(
            position,
            radius=sim_config.ball_diameter / 2,
            mass=sim_config.ball_mass,
            max_velocity=0.1 * terrain_config.width,
        )
        for position in positions
    ]

    logger.debug(
        f'Simulating for {sim_config.total_time} '
        f'({sim_config.tick_rate} steps per seconds)',
    )

    for _ in range(sim_config.total_time * sim_config.tick_rate):
        pybullet.stepSimulation()
        if sim_config.real_time:
            time.sleep(1 / sim_config.tick_rate)

    logger.debug('Simulation completed')

    final_positions = [
        pybullet.getBasePositionAndOrientation(ball_id)[0]
        for ball_id in ball_ids
    ]

    pybullet.disconnect()

    return final_positions


class PhysicsApp:
    """Physics simulation application.

    Simulate the physics of golf balls landing and rolling on a golf green.

    Args:
        num_simulations: Number of balls to simulate.
        terrain: Terrain configuration.
        simulation: Simulation configuration.
        seed: Random seed.
    """

    def __init__(
        self,
        num_simulations: int,
        terrain: TerrainConfig,
        simulation: SimulationConfig,
        seed: int | None = None,
    ) -> None:
        self.num_simulations = num_simulations
        self.terrain = terrain
        self.simulation = simulation
        self.seed = seed

    def close(self) -> None:
        """Close the application."""
        pass

    def run(self, engine: Engine, run_dir: pathlib.Path) -> None:
        """Run the application.

        Args:
            engine: Application execution engine.
            run_dir: Run directory.
        """
        terrain_heightmap = generate_noisemap(self.terrain, seed=self.seed)
        terrain_mesh = generate_vertices(terrain_heightmap, self.terrain)
        logger.info('Generated terrain mesh')

        initial_positions = generate_initial_positions(
            self.num_simulations,
            self.terrain,
            seed=self.seed,
        )
        logger.info(f'Generated {len(initial_positions)} initial position(s)')

        logger.info('Submitting simulations...')
        futures = [
            engine.submit(
                simulate,
                terrain_mesh,
                [position],
                sim_config=self.simulation,
                terrain_config=self.terrain,
            )
            for position in initial_positions
        ]
        logger.info('Simulations submitted')

        results = [future.result() for future in futures]
        final_positions = [pos for result in results for pos in result]
        logger.info(f'Received {len(final_positions)} final position(s)')

        contour_plot = run_dir / 'images' / 'contour.png'
        create_contour_plot(
            initial_positions,
            final_positions,
            terrain_heightmap,
            self.terrain,
            contour_plot,
        )
        logger.info(f'Saved contour map to {contour_plot}')

        terrain_plot = run_dir / 'images' / 'terrain.png'
        create_terrain_plot(
            initial_positions,
            final_positions,
            terrain_heightmap,
            self.terrain,
            terrain_plot,
        )
        logger.info(f'Saved terrain map to {terrain_plot}')
