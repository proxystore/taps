from __future__ import annotations

import logging
import os
import pathlib
import shutil
import subprocess
import uuid
from time import monotonic

import pandas as pd

from taps.apps.docking.train import run_model
from taps.apps.docking.train import train_model
from taps.engine import as_completed
from taps.engine import Engine
from taps.engine import task
from taps.engine import TaskFuture
from taps.logging import APP_LOG_LEVEL

logger = logging.getLogger(__name__)

MGLTOOLS_HOME_ENV = 'MGLTOOLS_HOME'


@task()
def smi_to_pdb(smiles: str, pdb_file: pathlib.Path) -> pathlib.Path:
    """Convert SMILES string to PDB representation.

    The conversion to PDB file will contain atomic coordinates
    that will be used for docking.

    Args:
        smiles: Molecule representation in SMILES format.
        pdb_file: Path of the PDB file to create.

    Returns:
        The created PDB file.
    """
    from rdkit import Chem
    from rdkit.Chem import AllChem

    # Convert SMILES to RDKit molecule object
    mol = Chem.MolFromSmiles(smiles)
    # Add hydrogens to the molecule
    mol = Chem.AddHs(mol)
    # Generate a 3D conformation for the molecule
    AllChem.EmbedMolecule(mol)
    AllChem.MMFFOptimizeMolecule(mol)

    # Write the molecule to a PDB file
    writer = Chem.PDBWriter(pdb_file)
    writer.write(mol)
    writer.close()

    return pdb_file


@task()
def set_element(
    input_pdb: pathlib.Path,
    output_pdb: pathlib.Path,
    tcl_path: pathlib.Path,
) -> pathlib.Path:
    """Add coordinated to the PDB file using VMD.

    Args:
        input_pdb: Path of input PDB file.
        output_pdb: Path to PDB file with atomic coordinates.
        tcl_path: Path to TCL script.

    Returns:
        The newly created PDB file path.
    """
    command = f'vmd -dispdev text -e {tcl_path} -args {input_pdb} {output_pdb}'

    subprocess.check_output(command.split())
    return output_pdb


@task()
def pdb_to_pdbqt(
    pdb_file: pathlib.Path,
    pdbqt_file: pathlib.Path,
    ligand: bool = True,
) -> pathlib.Path:
    """Convert PDB file to PDBQT format.

    PDBQT files are similar to the PDB format, but also includes connectivity
    information.

    Args:
        pdb_file: input PDB file to convert.
        pdbqt_file: output converted PDBQT file.
        ligand: If the molecule is a ligand or not.

    Returns:
        The path to the created PDBQT file.

    Raises:
        RuntimeError: If `MGLTOOLS_HOME` is not set.
    """
    autodocktools_path = os.getenv(MGLTOOLS_HOME_ENV)
    if autodocktools_path is None:
        raise RuntimeError(f'{MGLTOOLS_HOME_ENV} is not set.')

    script, flag = (
        ('prepare_ligand4.py', 'l')
        if ligand
        else ('prepare_receptor4.py', 'r')
    )

    script_path = (
        pathlib.Path(autodocktools_path)
        / 'MGLToolsPckgs/AutoDockTools/Utilities24'
        / script
    )
    command = (
        f'python2.7 {script_path} -{flag} {pdb_file} -o {pdbqt_file} '
        '-U nphs_lps_waters'
    )
    subprocess.check_output(
        command.split(),
        cwd=pdb_file.parent,
        encoding='utf-8',
    )

    return pdbqt_file


@task()
def make_autodock_config(
    input_receptor_pdbqt_file: pathlib.Path,
    input_ligand_pdbqt_file: pathlib.Path,
    output_conf_file: pathlib.Path,
    output_ligand_pdbqt_file: pathlib.Path,
    center: tuple[float, float, float] = (15.614, 53.380, 15.455),
    size: tuple[int, int, int] = (20, 20, 20),
    exhaustiveness: int = 20,
    num_modes: int = 20,
    energy_range: int = 10,
) -> pathlib.Path:
    """Create configuration for AutoDock Vina.

    Create a configuration file for AutoDock Vina by describing
    the target receptor and setting coordinate bounds for the
    docking experiment.

    Args:
        input_receptor_pdbqt_file: Target receptor PDBQT file.
        input_ligand_pdbqt_file: Target ligand PDBQT file.
        output_conf_file: The generated Vina conf file.
        output_ligand_pdbqt_file: Output ligand PDBQT file path.
        center: Center coordinates.
        size: Size of the search space.
        exhaustiveness: Number of monte carlo simulations.
        num_modes: Number of binding modes.
        energy_range: Maximum energy difference between
            the best binding mode and the worst one displayed (kcal/mol).

    Returns:
        Path of created output configuration file
    """
    # Format configuration file
    file_contents = (
        f'receptor = {input_receptor_pdbqt_file}\n'
        f'ligand = {input_ligand_pdbqt_file}\n'
        f'center_x = {center[0]}\n'
        f'center_y = {center[1]}\n'
        f'center_z = {center[2]}\n'
        f'size_x = {size[0]}\n'
        f'size_y = {size[1]}\n'
        f'size_z = {size[2]}\n'
        f'exhaustiveness = {exhaustiveness}\n'
        f'num_modes = {num_modes}\n'
        f'energy_range = {energy_range}\n'
        f'out = {output_ligand_pdbqt_file}\n'
    )
    # Write configuration file
    with open(output_conf_file, 'w') as f:
        f.write(file_contents)

    return output_conf_file


@task()
def autodock_vina(
    config_file: pathlib.Path,
    smiles: str,
    num_cpu: int = 1,
) -> tuple[str, float]:
    """Compute the docking score.

    The docking score captures the potential energy change when the protein
    and ligand are docked. A strong binding is represented by a negative score,
    weaker (or no) binders are represented by positive scores.

    Args:
        config_file: Vina configuration file.
        smiles: The SMILES string of molecule.
        num_cpu: Number of CPUs to use.

    Returns:
        A tuple containing the SMILES string.
    """
    command = ['vina', '--config', str(config_file), '--cpu', str(num_cpu)]
    result = subprocess.check_output(command, encoding='utf-8')

    # find the last row of the table and extract the affinity score
    result_list = result.split('\n')
    last_row = result_list[-3]
    score = last_row.split()
    return (smiles, float(score[1]))


class DockingApp:
    """Protein docking application.

    Based on the
    [Parsl Docking Tutorial](https://github.com/Parsl/parsl-docking-tutorial).

    Args:
        smi_file_name_ligand_path: Path to ligand SMILES string.
        receptor_path: Path to target receptor PDBQT file.
        tcl_path: Path to TCL script.
        initial_simulations: Initial number of simulations to perform.
        num_iterations: Number of infer-simulate-train loops to perform.
        batch_size: Number of simulations per iteration.
        seed: Random seed for sampling.
    """

    def __init__(
        self,
        smi_file_name_ligand_path: pathlib.Path,
        receptor_path: pathlib.Path,
        tcl_path: pathlib.Path,
        initial_simulations: int = 8,
        num_iterations: int = 3,
        batch_size: int = 8,
        seed: int = 0,
    ) -> None:
        self.smi_file_name_ligand = smi_file_name_ligand_path
        self.receptor = receptor_path
        self.tcl_path = tcl_path
        self.initial_simulations = initial_simulations
        self.num_iterations = num_iterations
        self.batch_size = batch_size
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
        docking_futures: list[TaskFuture[tuple[str, float]]] = []
        train_data = []
        smiles_simulated = []

        train_output_file = run_dir / 'training-results.json'
        task_data_dir = run_dir / 'tasks'
        task_data_dir.mkdir(parents=True, exist_ok=True)

        search_space = pd.read_csv(self.smi_file_name_ligand)
        search_space = search_space[['TITLE', 'SMILES']]

        # start with an initial set of random smiles
        selected_smiles = search_space.sample(
            self.initial_simulations,
            random_state=self.seed,
        )
        logger.log(
            APP_LOG_LEVEL,
            f'Submitting {self.initial_simulations} initial simulations',
        )
        for i in range(self.initial_simulations):
            smiles = selected_smiles.iloc[i]['SMILES']
            working_dir = task_data_dir / uuid.uuid4().hex
            working_dir.mkdir()
            future = self._submit_task_for_smiles(engine, smiles, working_dir)
            docking_futures.append(future)
            logger.log(APP_LOG_LEVEL, f'Submitted computations for {smiles}')

        for future in as_completed(docking_futures):
            smiles, score = future.result()
            logger.log(
                APP_LOG_LEVEL,
                f'Computation for {smiles} succeeded with score = {score}',
            )

            train_data.append(
                {'smiles': smiles, 'score': score, 'time': monotonic()},
            )
            smiles_simulated.append(smiles)

        training_df = pd.DataFrame(train_data)

        # train model, run inference, and run more simulations
        for i in range(self.num_iterations):
            logger.log(
                APP_LOG_LEVEL,
                f'Starting iteration {i + 1}/{self.num_iterations}',
            )

            model = train_model(training_df)
            logger.log(APP_LOG_LEVEL, 'Model training finished')

            predictions = run_model(model, search_space['SMILES'])
            predictions.sort_values('score', ascending=True, inplace=True)
            logger.log(APP_LOG_LEVEL, 'Model inference finished')

            train_data = []
            futures = []
            batch_count = 0
            for smiles in predictions['smiles']:
                if smiles not in smiles_simulated:
                    working_dir = task_data_dir / uuid.uuid4().hex
                    working_dir.mkdir()
                    future = self._submit_task_for_smiles(
                        engine,
                        smiles,
                        working_dir,
                    )
                    futures.append(future)
                    batch_count += 1
                    logger.log(
                        APP_LOG_LEVEL,
                        f'Submitted computations for {smiles}',
                    )

                if batch_count >= self.batch_size:
                    break

            for future in as_completed(futures):
                smiles, score = future.result()
                logger.log(
                    APP_LOG_LEVEL,
                    f'Computation for {smiles} succeeded with score = {score}',
                )

                train_data.append(
                    {'smiles': smiles, 'score': score, 'time': monotonic()},
                )
                smiles_simulated.append(smiles)

            training_df = pd.concat(
                (training_df, pd.DataFrame(train_data)),
                ignore_index=True,
            )

        training_df.to_json(train_output_file)
        logger.log(
            APP_LOG_LEVEL,
            f'Training data saved to {train_output_file}',
        )
        shutil.rmtree(task_data_dir)

    def _submit_task_for_smiles(
        self,
        engine: Engine,
        smiles: str,
        working_dir: pathlib.Path,
    ) -> TaskFuture[tuple[str, float]]:
        pdb_file = working_dir / 'in.pdb'
        output_pdb = working_dir / 'coords.pdb'
        pdbqt_file = working_dir / 'coords.pdbqt'
        vina_conf_file = working_dir / 'config.txt'
        output_ligand_pdbqt = working_dir / 'out.pdb'

        smi_future = engine.submit(smi_to_pdb, smiles, pdb_file=pdb_file)
        element_future = engine.submit(
            set_element,
            smi_future,
            output_pdb=output_pdb,
            tcl_path=self.tcl_path,
        )
        pdbqt_future = engine.submit(
            pdb_to_pdbqt,
            element_future,
            pdbqt_file=pdbqt_file,
        )
        config_future = engine.submit(
            make_autodock_config,
            self.receptor,
            pdbqt_future,
            vina_conf_file,
            output_ligand_pdbqt,
        )
        return engine.submit(autodock_vina, config_future, smiles)
