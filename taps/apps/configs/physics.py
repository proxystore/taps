from __future__ import annotations

from typing import Literal

from pydantic import Field

from taps.apps import App
from taps.apps import AppConfig
from taps.plugins import register


@register('app')
class PhysicsConfig(AppConfig):
    """Physics application configuration."""

    name: Literal['physics'] = Field(
        'physics',
        description='Application name.',
    )
    simulations: int = Field(description='Number of parallel simulations.')
    ball_diameter: float = Field(
        0.2,
        description='Golf ball diameter in meters.',
    )
    ball_mass: float = Field(0.05, description='Golf ball mass in kilograms.')
    tick_rate: int = Field(240, description='Simulation steps per seconds.')
    total_time: int = Field(10, description='Simulation runtime in seconds.')
    real_time: bool = Field(True, description='Simulate at real time.')
    seed: int | None = Field(None, description='Random seed.')
    terrain_width: int = Field(
        20,
        description='Terrain width/length in meters.',
    )
    terrain_height: int = Field(3, description='Terrain max height in meters.')
    terrain_resolution: int = Field(
        10,
        description='Terrain vertices per meter.',
    )
    terrain_scale: float = Field(
        10.0,
        description='Noise map scale (how far away map is viewed from).',
    )
    terrain_octaves: int = Field(3, description='Detail level in noise map.')
    terrain_persistence: float = Field(
        0.2,
        description='Octave contributions (amplitude).',
    )
    terrain_lacunarity: float = Field(
        2.0,
        description='Detail added/removed at each octave (frequency).',
    )
    terrain_filter_size: int = Field(
        2,
        description='Terrain smoothing filter size.',
    )

    def get_app(self) -> App:
        """Create an application instance from the config."""
        from taps.apps.physics import PhysicsApp
        from taps.apps.physics import SimulationConfig
        from taps.apps.physics import TerrainConfig

        terrain = TerrainConfig(
            width=self.terrain_width,
            height=self.terrain_height,
            resolution=self.terrain_resolution,
            scale=self.terrain_scale,
            octaves=self.terrain_octaves,
            persistence=self.terrain_persistence,
            lacunarity=self.terrain_lacunarity,
            filter_size=self.terrain_filter_size,
        )

        simulation = SimulationConfig(
            ball_diameter=self.ball_diameter,
            ball_mass=self.ball_mass,
            tick_rate=self.tick_rate,
            total_time=self.total_time,
            real_time=self.real_time,
        )

        return PhysicsApp(
            num_simulations=self.simulations,
            terrain=terrain,
            simulation=simulation,
            seed=self.seed,
        )
