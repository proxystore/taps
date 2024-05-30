from __future__ import annotations

import logging
import pathlib
import time

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from taps.engine import AppEngine
from taps.engine import as_completed
from taps.engine import TaskFuture
from taps.logging import WORK_LOG_LEVEL
from taps.wf.moldesign.chemfunctions import compute_vertical
from taps.wf.moldesign.tasks import combine_inferences
from taps.wf.moldesign.tasks import run_model
from taps.wf.moldesign.tasks import train_model

logger = logging.getLogger(__name__)


class MoldesignApp:
    """Molecular design application.

    Args:
        dataset: Molecule search space dataset.
        initial_count: Number of initial calculations.
        search_count: Number of molecules to evaluate in total.
        batch_size: Number of molecules to evaluate in each batch of
            simulations.
        seed: Random seed.
    """

    def __init__(
        self,
        dataset: str,
        initial_count: int = 8,
        search_count: int = 64,
        batch_size: int = 4,
        seed: int = 0,
    ) -> None:
        self.dataset = dataset
        self.initial_count = initial_count
        self.search_count = search_count
        self.batch_size = batch_size
        self.seed = seed

    def close(self) -> None:
        """Close the application."""
        pass

    def run(self, engine: AppEngine, run_dir: pathlib.Path) -> None:  # noqa: PLR0915
        """Run the application.

        Args:
            engine: Application execution engine.
            run_dir: Run directory.
        """
        start_time = time.monotonic()

        search_space = pd.read_csv(self.dataset, sep='\s+')
        logger.log(
            WORK_LOG_LEVEL,
            f'Loaded search space (size={len(search_space):,})',
        )

        # Submit with some random guesses
        train_data_list = []
        init_mols = list(
            search_space.sample(
                self.initial_count,
                random_state=self.seed,
            )['smiles'],
        )
        sim_futures: dict[TaskFuture[float], str] = {
            engine.submit(compute_vertical, mol): mol for mol in init_mols
        }
        logger.log(WORK_LOG_LEVEL, 'Submitted initial computations')
        logger.info(f'Initial set: {init_mols}')
        already_ran = set()

        # Loop until you finish populating the initial set
        while len(sim_futures) > 0:
            # First, get the next completed computation from the list
            future: TaskFuture[float] = next(
                as_completed(list(sim_futures.keys())),
            )

            # Remove it from the list of still-running task and get the input
            smiles = sim_futures.pop(future)
            already_ran.add(smiles)

            # Check if the run completed successfully
            if future.exception() is not None:
                # If it failed, pick a new SMILES string at random and submit
                smiles = search_space.sample(
                    1,
                    random_state=self.seed,
                ).iloc[0]['smiles']
                new_future = engine.submit(
                    compute_vertical,
                    smiles,
                )
                sim_futures[new_future] = smiles
            else:
                # If it succeeded, store the result
                train_data_list.append(
                    {
                        'smiles': smiles,
                        'ie': future.result(),
                        'batch': 0,
                        'time': time.monotonic() - start_time,
                    },
                )

        logger.log(WORK_LOG_LEVEL, 'Done computing initial set')

        # Create the initial training set as a
        train_data = pd.DataFrame(train_data_list)
        logger.log(
            WORK_LOG_LEVEL,
            f'Created initial training set (size={len(train_data)})',
        )

        # Loop until complete
        batch = 1
        while len(train_data) < self.search_count:
            # Train and predict as show in the previous section.
            train_future = engine.submit(train_model, train_data)
            logger.log(WORK_LOG_LEVEL, 'Submitting inference tasks')
            inference_futures = [
                engine.submit(run_model, train_future, chunk)
                for chunk in np.array_split(search_space['smiles'], 64)
            ]
            predictions = engine.submit(
                combine_inferences,
                *inference_futures,
            ).result()
            logger.log(
                WORK_LOG_LEVEL,
                f'Inference results received (size={len(predictions)})',
            )

            # Sort the predictions in descending order, and submit new
            # molecules from them.
            predictions.sort_values('ie', ascending=False, inplace=True)
            sim_futures = {}
            for smiles in predictions['smiles']:
                if smiles not in already_ran:
                    new_future = engine.submit(compute_vertical, smiles)
                    sim_futures[new_future] = smiles
                    already_ran.add(smiles)
                    if len(sim_futures) >= self.batch_size:
                        break
            logger.log(
                WORK_LOG_LEVEL,
                f'Submitted new computations (size={len(sim_futures)})',
            )

            # Wait for every task in the current batch to complete, and store
            # successful results.
            new_results = []
            for future in as_completed(list(sim_futures.keys())):
                if future.exception() is None:
                    new_results.append(
                        {
                            'smiles': sim_futures[future],
                            'ie': future.result(),
                            'batch': batch,
                            'time': time.monotonic() - start_time,
                        },
                    )

            # Update the training data and repeat
            batch += 1
            train_data = pd.concat(
                (train_data, pd.DataFrame(new_results)),
                ignore_index=True,
            )

        fig, ax = plt.subplots(figsize=(4.5, 3.0))

        ax.scatter(train_data['time'], train_data['ie'])
        ax.step(train_data['time'], train_data['ie'].cummax(), 'k--')

        ax.set_xlabel('Walltime (s)')
        ax.set_ylabel('Ion. Energy (Ha)')

        fig.tight_layout()

        train_data.to_csv(run_dir / 'results.csv', index=False)
