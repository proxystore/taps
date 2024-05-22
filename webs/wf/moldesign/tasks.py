from __future__ import annotations

import pandas as pd
from sklearn.pipeline import Pipeline


def train_model(train_data: pd.DataFrame) -> Pipeline:
    """Train a machine learning model using Morgan Fingerprints.

    Args:
        train_data: Dataframe with a 'smiles' and 'ie' column
            that contains molecule structure and property, respectfully.

    Returns:
        A trained model.
    """
    # Imports for python functions run remotely must be defined inside the
    # function
    from sklearn.neighbors import KNeighborsRegressor
    from sklearn.pipeline import Pipeline

    from webs.wf.moldesign.chemfunctions import MorganFingerprintTransformer

    model = Pipeline(
        [
            ('fingerprint', MorganFingerprintTransformer()),
            # n_jobs = -1 lets the model run all available processors
            (
                'knn',
                KNeighborsRegressor(
                    n_neighbors=4,
                    weights='distance',
                    metric='jaccard',
                    n_jobs=-1,
                ),
            ),
        ],
    )

    return model.fit(train_data['smiles'], train_data['ie'])


def run_model(model: Pipeline, smiles: list[str]) -> pd.DataFrame:
    """Run a model on a list of smiles strings.

    Args:
        model: Trained model that takes SMILES strings as inputs.
        smiles: List of molecules to evaluate.

    Returns:
        A dataframe with the molecules and their predicted outputs.
    """
    pred_y = model.predict(smiles)
    return pd.DataFrame({'smiles': smiles, 'ie': pred_y})


def combine_inferences(*inputs: pd.DataFrame) -> pd.DataFrame:
    """Concatenate a series of inferences into a single DataFrame.

    Args:
        inputs: A list of the component DataFrames.

    Returns:
        A single DataFrame containing the same inferences.
    """
    return pd.concat(inputs, ignore_index=True)
