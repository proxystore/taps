"""Module adapted from `<https://github.com/Parsl/parsl-docking-tutorial/blob/main/ml_functions.py>`."""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator
from sklearn.base import TransformerMixin
from sklearn.pipeline import Pipeline


def compute_morgan_fingerprints(
    smiles: str,
    fingerprint_length: int,
    fingerprint_radius: int,
) -> tuple[int, int]:
    """Get Morgan Fingerprint of a specific SMILES string.

    Adapted from: `<https://github.com/google-research/google-research/blob/>
    dfac4178ccf521e8d6eae45f7b0a33a6a5b691ee/mol_dqn/chemgraph/dqn/deep_q_networks.py#L750>`
    Args:
      graph (str): The molecule as a SMILES string
      fingerprint_length (int): Bit-length of fingerprint
      fingerprint_radius (int): Radius used to compute fingerprint
    Returns:
      np.array.shape = [hparams, fingerprint_length]. The Morgan fingerprint.
    """
    from rdkit import Chem
    from rdkit import DataStructs
    from rdkit.Chem import rdFingerprintGenerator

    # Parse the molecule
    molecule = Chem.MolFromSmiles(smiles)

    # Compute the fingerprint
    mfpgen = rdFingerprintGenerator.GetMorganGenerator(
        radius=fingerprint_radius,
        fpSize=fingerprint_length,
    )
    fingerprint = mfpgen.GetFingerprint(
        molecule,
    )
    arr = np.zeros((1,), dtype=bool)

    # ConvertToNumpyArray takes ~ 0.19 ms, while
    # np.asarray takes ~ 4.69 ms
    DataStructs.ConvertToNumpyArray(fingerprint, arr)
    return arr


class MorganFingerprintTransformer(BaseEstimator, TransformerMixin):
    """Class that converts SMILES strings to fingerprint vectors."""

    def __init__(self, length: int = 256, radius: int = 4):
        self.length = length
        self.radius = radius

    def fit(
        self,
        X: list[str],  # noqa: N803
        y: np.array[int] | None = None,
    ) -> MorganFingerprintTransformer:
        """Train model.

        Args:
            X (list[str]): list of SMILES strings
            y (np.array[int] | None): array of true fingerprints.
                Defaults to None.

        Returns:
            MorganFingerprintTransformer: the trained model
        """
        return self  # Don't need to do anything

    def transform(
        self,
        X: list[str],  # noqa: N803
        y: np.array[int] | None = None,
    ) -> np.array[int]:
        """Compute the fingerprints.

        Args:
            X: List of SMILES strings
            y (np.array[int] | None): array of true fingerprints.
                Defaults to None.

        Returns:
            np.array[int]: Array of predicted fingerprints
        """
        fps = []
        for x in X:
            fps.append(
                compute_morgan_fingerprints(x, self.length, self.radius),
            )

        return fps


def train_model(training_data: pd.DataFrame) -> Pipeline:
    """Train a machine learning model using Morgan Fingerprints.

    Args:
        training_data: Dataframe with a 'smiles' and 'score' column
            that contains molecule structure and docking score, respectfully.

    Returns:
        sklearn.Pipeline: A trained model
    """
    from sklearn.neighbors import KNeighborsRegressor
    from sklearn.pipeline import Pipeline

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
            ),  # n_jobs = -1 lets the model run all available processors
        ],
    )

    return model.fit(training_data['smiles'], training_data['score'])


def run_model(model: Pipeline, smiles: list[str]) -> pd.DataFrame:
    """Run a model on a list of smiles strings.

    Args:
        model (sklearn.Pipeline): Trained model that
            takes SMILES strings as inputs
        smiles (list[str]): List of molecules to evaluate
    Returns:
        A dataframe with the molecules and their predicted outputs
    """
    import pandas as pd

    pred_y = model.predict(smiles)
    return pd.DataFrame({'smiles': smiles, 'score': pred_y})
