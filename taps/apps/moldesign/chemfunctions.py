"""Functions used to perform chemistry tasks in the Parsl workflow.

While Parsl does offer the ability to run functions defined in a Jupyter
notebook, we define them here to keep the notebook cleaner
"""

from __future__ import annotations

import os
import sys
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from io import StringIO
from typing import Any
from typing import Callable
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import numpy as np
import pandas as pd
from ase.io import read
from ase.optimize import LBFGSLineSearch
from rdkit import Chem
from rdkit import DataStructs
from rdkit.Chem import AllChem
from sklearn.base import BaseEstimator
from sklearn.base import TransformerMixin
from sklearn.neighbors import KNeighborsRegressor
from sklearn.pipeline import Pipeline
from xtb.ase.calculator import XTB

P = ParamSpec('P')
T = TypeVar('T')

# Make a global pool for this particular Python thread
#  Not a great practice, as it will not exit until Python does.
#  Useful on HPC as it limits the number of times we call `fork`
#   and we know the nodes where this run will get purged after tasks complete
try:
    # sched_getaffinity is not available on all systems
    threads = len(os.sched_getaffinity(0))  # type: ignore[attr-defined, unused-ignore]
except AttributeError:
    threads = os.cpu_count() or 1
n_workers = max(threads - 1, 1)  # Get as many threads as we are assigned to


def generate_initial_xyz(mol_string: str) -> str:
    """Generate the XYZ coordinates for a molecule.

    Args:
        mol_string: SMILES string

    Returns:
        - InChI string for the molecule
        - XYZ coordinates for the molecule
    """
    # Generate 3D coordinates for the molecule
    mol = Chem.MolFromSmiles(mol_string)
    if mol is None:
        raise ValueError(f'Parse failure for {mol_string}')
    mol = Chem.AddHs(mol)
    AllChem.EmbedMolecule(mol, randomSeed=1)
    AllChem.MMFFOptimizeMolecule(mol)

    # Save geometry as 3D coordinates
    xyz = f'{mol.GetNumAtoms()}\n'
    xyz += mol_string + '\n'
    conf = mol.GetConformer()
    for i, a in enumerate(mol.GetAtoms()):
        s = a.GetSymbol()
        c = conf.GetAtomPosition(i)
        xyz += f'{s} {c[0]} {c[1]} {c[2]}\n'

    return xyz


def _run_in_process(
    func: Callable[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    """Execute function in process.

    Hack to make each execution run in a separate process. XTB or geoMETRIC
    is bad with file handles.

    Args:
        func: Function to evaluate.
        args: Input arguments.
        kwargs: Keyword arguments.

    Returns:
        The function result.
    """
    with ProcessPoolExecutor(max_workers=1) as exe:
        fut = exe.submit(func, *args, **kwargs)
        return fut.result()


def _compute_vertical(smiles: str) -> float:
    """Run the ionization potential computation.

    Args:
        smiles: SMILES string to evaluate
    Returns:
        Ionization energy in Ha
    """
    # Make the initial geometry
    xyz = generate_initial_xyz(smiles)

    # Make the XTB calculator
    calc = XTB(accuracy=0.05)

    # Parse the molecule
    atoms = read(StringIO(xyz), format='xyz')

    # Compute the neutral geometry. Uses QCEngine
    # (https://github.com/MolSSI/QCEngine) to handle interfaces to XTB.
    atoms.calc = calc
    dyn = LBFGSLineSearch(atoms, logfile=None)
    dyn.run(fmax=0.02, steps=250)

    neutral_energy = atoms.get_potential_energy()

    # Compute the energy of the relaxed geometry in charged form
    charges = np.ones((len(atoms),)) * (1 / len(atoms))
    atoms.set_initial_charges(charges)
    charged_energy = atoms.get_potential_energy()

    return charged_energy - neutral_energy


# Make versions that execute in separate processes
# compute_vertical = update_wrapper(
#     partial(_run_in_process, _compute_vertical),
#     _compute_vertical,
# )
# compute_vertical.__name__ = 'compute_vertical'
compute_vertical = _compute_vertical


def compute_morgan_fingerprints(
    smiles: str,
    fingerprint_length: int,
    fingerprint_radius: int,
) -> np.ndarray:
    """Get Morgan Fingerprint of a specific SMILES string.

    Adapted from:
    https://github.com/google-research/google-research/blob/dfac4178ccf521e8d6eae45f7b0a33a6a5b691ee/mol_dqn/chemgraph/dqn/deep_q_networks.py#L750

    Args:
        smiles: The molecule as a SMILES string.
        fingerprint_length: Bit-length of fingerprint.
        fingerprint_radius: Radius used to compute fingerprint.

    Returns:
        Array with shape `[hparams, fingerprint_length]` of the Morgan \
        fingerprint.
    """
    # Parse the molecule
    molecule = Chem.MolFromSmiles(smiles)

    # Compute the fingerprint
    fingerprint = AllChem.GetMorganFingerprintAsBitVect(
        molecule,
        fingerprint_radius,
        fingerprint_length,
    )
    arr = np.zeros((1,), dtype=np.bool_)

    # ConvertToNumpyArray takes ~ 0.19 ms, while
    # np.asarray takes ~ 4.69 ms
    DataStructs.ConvertToNumpyArray(fingerprint, arr)
    return arr


class MorganFingerprintTransformer(BaseEstimator, TransformerMixin):
    """Class that converts SMILES strings to fingerprint vectors."""

    def __init__(self, length: int = 256, radius: int = 4) -> None:
        self.length = length
        self.radius = radius

    def fit(self, X: Any, y: Any = None) -> Self:  # noqa: N803
        """Fit the transformer."""
        return self  # Do need to do anything

    def transform(self, X: Any, y: Any = None) -> Any:  # noqa: N803
        """Compute the fingerprints.

        Args:
            X: List of SMILES strings.
            y: Ignored.

        Returns:
            Array of fingerprints.
        """
        my_func = partial(
            compute_morgan_fingerprints,
            fingerprint_length=self.length,
            fingerprint_radius=self.radius,
        )
        with ProcessPoolExecutor(max_workers=n_workers) as pool:
            fing = list(pool.map(my_func, X, chunksize=2048))
        return np.vstack(fing)


def train_model(smiles: list[str], properties: list[float]) -> Pipeline:
    """Train a machine learning model using Morgan Fingerprints.

    Args:
        smiles: SMILES strings for each molecule
        properties: List of a property for each molecule

    Returns:
        A trained model.
    """
    model = Pipeline(
        [
            ('fingerprint', MorganFingerprintTransformer()),
            (
                'knn',
                KNeighborsRegressor(
                    n_neighbors=4,
                    weights='distance',
                    metric='jaccard',
                    n_jobs=-1,
                ),
            ),
            # n_jobs = -1 lets the model run all available processors
        ],
    )

    return model.fit(smiles, properties)


def run_model(model: Any, smiles: list[str]) -> pd.DataFrame:
    """Run a model on a list of smiles strings.

    Args:
        model: Trained model that takes SMILES strings as inputs.
        smiles: List of molecules to evaluate.

    Returns:
        A dataframe with the molecules and their predicted outputs.
    """
    pred_y = model.predict(smiles)
    return pd.DataFrame({'smiles': smiles, 'ie': pred_y})


if __name__ == '__main__':
    energy = compute_vertical('OC')
    print(energy)
