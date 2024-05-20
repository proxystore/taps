from __future__ import annotations

import logging
import pathlib
import sys

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import os
import subprocess
import uuid
from pathlib import Path
from time import monotonic

import pandas as pd

from webs.context import ContextManagerAddIn
from webs.executor.workflow import as_completed
from webs.executor.workflow import TaskFuture
from webs.executor.workflow import WorkflowExecutor
from webs.wf.docking.config import DockingWorkflowConfig
from webs.wf.docking.train import run_model
from webs.wf.docking.train import train_model
from webs.workflow import register

logger = logging.getLogger(__name__)

autodocktools_path = os.getenv('MGLTOOLS_HOME')


def smi_to_pdb(smiles: str, pdb_file: pathlib.Path) -> pathlib.Path:
    """Convert SMILES string to PDB representation.

    The conversion to PDB file will contain atomic coordinates
    that will be used for docking.

    Args:
        smiles (str): the molecule representation in
            SMILES format
        pdb_file (pathlib.Path): the path of the PDB file
            to create

    Returns:
        pathlib.Path: The created PDB file
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


def set_element(
    input_pdb: pathlib.Path,
    output_pdb: pathlib.Path,
    tcl_path: pathlib.Path,
) -> pathlib.Path:
    """Add coordinated to the PDB file using VMD.

    Args:
        input_pdb (pathlib.Path): path of input PDB file.
        output_pdb (pathlib.Path): path to PDB file with atomic coordinates
        tcl_path (pathlib.Path): path to TCL script

    Returns:
        pathlib.Path: the newly created PDB file path
    """
    command = f'vmd -dispdev text -e {tcl_path} -args {input_pdb} {output_pdb}'

    result = subprocess.check_output(command.split())
    logger.info(result)
    return output_pdb


def pdb_to_pdbqt(
    pdb_file: pathlib.Path,
    pdbqt_file: pathlib.Path,
    ligand: bool = True,
) -> pathlib.Path:
    """Convert PDB file to PDBQT format.

    PDBQT files are similar to the PDB format, but also
    includes connectivity information

    Args:
        pdb_file (pathlib.Path): input PDB file to convert
        pdbqt_file (pathlib.Path): output converted PDBQT file
        ligand (bool, optional): If the molecule is a ligand or not.
            Defaults to True.

    Returns:
        pathlib.Path: the path to the created PDBQT file
    """
    autodocktools_path: str | None = os.getenv('MGLTOOLS_HOME')
    assert autodocktools_path is not None

    script, flag = (
        ('prepare_ligand4.py', 'l')
        if ligand
        else ('prepare_receptor4.py', 'r')
    )

    command = (
        f"{'python2.7'} "
        f"""{(Path(autodocktools_path)
            / 'MGLToolsPckgs/AutoDockTools/Utilities24'
            / script)} """
        f" -{flag} {pdb_file} "
        f" -o {pdbqt_file} "
        f" -U nphs_lps_waters"
    )
    result = subprocess.check_output(
        command.split(),
        cwd=pdb_file.parent,
        encoding='utf-8',
    )
    logger.info(result)

    return pdbqt_file


def make_autodock_config(  # noqa: PLR0913
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
        input_receptor_pdbqt_file (pathlib.Path): target receptor PDBQT file
        input_ligand_pdbqt_file (pathlib.Path): target ligand PDBQT file
        output_conf_file (pathlib.Path): the generated Vina conf file
        output_ligand_pdbqt_file (pathlib.Path): output ligand PDBQT file path
        center (Tuple[float, float, float]): center coordinates.
            Defaults to (15.614, 53.380, 15.455).
        size (Tuple[int, int, int]): size of the search space.
            Defaults to (20, 20, 20).
        exhaustiveness (int, optional): number of monte carlo simulations.
            Defaults to 20.
        num_modes (int, optional): number of binding modes. Defaults to 20.
        energy_range (int, optional): maximum energy difference between
            the best binding mode and the worst one displayed (kcal/mol).
            Defaults to 10.

    Returns:
        pathlib.Path: path of created output configuration file
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


def autodock_vina(
    config_file: pathlib.Path,
    smiles: str,
    num_cpu: int = 1,
) -> tuple[str, float] | str:
    """Compute the docking score.

    The docking score captures the potential energy change
    when the protein and ligand are docked. A strong binding
    is represented by a negative score, weaker (or no) binders
    are represented by positive scores.

    Args:
        config_file (pathlib.Path): Vina configuration file
        smiles (str): the SMILES string of molecule
        num_cpu (int, optional): number of CPUs to use. Defaults to 1.

    Returns:
        tuple[str, float] | str: the docking score for the associated
            molecule or the error statement
    """
    autodock_vina_exe = 'vina'
    try:
        command = f'{autodock_vina_exe} --config {config_file} --cpu {num_cpu}'
        result = subprocess.check_output(command.split(), encoding='utf-8')

        # find the last row of the table and extract the affinity score
        result_list = result.split('\n')
        last_row = result_list[-3]
        score = last_row.split()
        return (smiles, float(score[1]))
    except subprocess.CalledProcessError as e:
        return (
            f"Command '{e.cmd}' returned non-zero exit status {e.returncode}"
        )
    except Exception as e:
        return f'Error: {e}'


def cleanup(  # noqa: PLR0913
    dock_result: tuple[str, float] | str,
    pdb: pathlib.Path,
    pdb_coords: pathlib.Path,
    pdb_qt: pathlib.Path,
    autodoc_config: pathlib.Path,
    docking: pathlib.Path,
) -> None:
    """Cleanup output directory.

    Args:
        dock_result (tuple[str, float] | str): Docking score output
        pdb (pathlib.Path): pdb file generated from SMILES string
        pdb_coords (pathlib.Path): pdb file with atomic coordinates
        pdb_qt (pathlib.Path): pdqt file
        autodoc_config (pathlib.Path): autodock vina config file
        docking (pathlib.Path): output ligand file
    """
    pdb.unlink(missing_ok=True)
    pdb_coords.unlink(missing_ok=True)
    pdb_qt.unlink(missing_ok=True)
    autodoc_config.unlink(missing_ok=True)
    docking.unlink(missing_ok=True)


@register()
class DockingWorkflow(ContextManagerAddIn):
    """Protein docking workflow.

    Args:
        config: Workflow configuration.
    """

    name = 'docking'
    config_type = DockingWorkflowConfig

    def __init__(self, config: DockingWorkflowConfig) -> None:
        self.smi_file_name_ligand = pathlib.Path(config.smi_file_name_ligand)
        self.receptor = pathlib.Path(config.receptor)
        self.tcl_path = pathlib.Path(config.tcl_path)
        super().__init__()

    @classmethod
    def from_config(cls, config: DockingWorkflowConfig) -> Self:
        """Initialize a workflow from a config.

        Args:
            config: Workflow configuration.

        Returns:
            Workflow.
        """
        return cls(config)

    def run(self, executor: WorkflowExecutor, run_dir: pathlib.Path) -> None:  # noqa: PLR0915
        """Run the workflow.

        Args:
            executor: Workflow task executor.
            run_dir: Run directory.
        """
        futures: list[TaskFuture[tuple[str, float] | str]] = []
        train_data = []
        smiles_simulated = []
        initial_count = 5
        num_loops = 3
        batch_size = 3
        train_output_file = pathlib.Path('training_results.json')

        search_space = pd.read_csv(self.smi_file_name_ligand)
        search_space = search_space[['TITLE', 'SMILES']]

        print(search_space.head(5))

        # start with an initial set of random smiles
        for _ in range(initial_count):
            selected = search_space.sample(1).iloc[0]
            _, smiles = selected['TITLE'], selected['SMILES']

            # workflow
            fname = uuid.uuid4().hex

            pdb_file = f'{fname}.pdb'
            output_pdb = f'{fname}-coords.pdb'
            pdbqt_file = f'{fname}-coords.pdbqt'
            vina_conf_file = f'{fname}-config.txt'
            output_ligand_pdbqt = f'{fname}-out.pdb'

            smi_future = executor.submit(smi_to_pdb, smiles, pdb_file=pdb_file)
            element_future = executor.submit(
                set_element,
                smi_future,
                output_pdb=output_pdb,
                tcl_path=self.tcl_path,
            )
            pdbqt_future = executor.submit(
                pdb_to_pdbqt,
                element_future,
                pdbqt_file=pdbqt_file,
            )
            config_future = executor.submit(
                make_autodock_config,
                self.receptor,
                pdbqt_future,
                vina_conf_file,
                output_ligand_pdbqt,
            )
            dock_future = executor.submit(autodock_vina, config_future, smiles)
            _ = executor.submit(
                cleanup,
                dock_future,
                smi_future,
                element_future,
                pdbqt_future,
                config_future,
                output_ligand_pdbqt,
            )

            futures.append(dock_future)

        # wait for all the futures to finish
        while len(futures) > 0:
            future = next(as_completed(futures))
            dock_score = future.result()

            assert isinstance(dock_score, tuple), dock_score
            smiles, score = dock_score

            futures.remove(future)

            logger.info(f'Computation for {smiles} succeeded: {score}')

            train_data.append(
                {
                    'smiles': smiles,
                    'score': score,
                    'time': monotonic(),
                },
            )
            smiles_simulated.append(smiles)

        training_df = pd.DataFrame(train_data)

        # train model, run inference, and run more simulations
        for i in range(num_loops):
            logger.info(f'\nStarting batch {i}')
            m = train_model(training_df)
            predictions = run_model(m, search_space['SMILES'])
            predictions.sort_values(
                'score',
                ascending=True,
                inplace=True,
            )

            train_data = []
            futures = []
            batch_count = 0
            for smiles in predictions['smiles']:
                if smiles not in smiles_simulated:
                    fname = uuid.uuid4().hex

                    pdb_file = f'{fname}.pdb'
                    output_pdb = f'{fname}-coords.pdb'
                    pdbqt_file = f'{fname}-coords.pdbqt'
                    vina_conf_file = f'{fname}-config.txt'
                    output_ligand_pdbqt = f'{fname}-out.pdb'

                    smi_future = executor.submit(
                        smi_to_pdb,
                        smiles,
                        pdb_file=pdb_file,
                    )
                    element_future = executor.submit(
                        set_element,
                        smi_future,
                        output_pdb=output_pdb,
                        tcl_path=self.tcl_path,
                    )
                    pdbqt_future = executor.submit(
                        pdb_to_pdbqt,
                        element_future,
                        pdbqt_file=pdb_file,
                    )
                    config_future = executor.submit(
                        make_autodock_config,
                        self.receptor,
                        pdbqt_future,
                        vina_conf_file,
                        output_ligand_pdbqt_file=output_ligand_pdbqt,
                    )
                    dock_future = executor.submit(
                        autodock_vina,
                        config_future,
                        smiles,
                    )
                    executor.submit(
                        cleanup,
                        dock_future,
                        smi_future,
                        element_future,
                        pdbqt_future,
                        config_future,
                        output_ligand_pdbqt,
                    )

                    futures.append(dock_future)

                    batch_count += 1

                if batch_count > batch_size:
                    break

            # wait for all the workflows to complete
            while len(futures) > 0:
                future = next(as_completed(futures))
                dock_score = future.result()

                assert isinstance(dock_score, tuple), dock_score

                smiles, score = dock_score
                futures.remove(future)

                logger.info(f'Computation for {smiles} succeeded: {score}')

                train_data.append(
                    {
                        'smiles': smiles,
                        'score': score,
                        'time': monotonic(),
                    },
                )
                smiles_simulated.append(smiles)

            training_df = pd.concat(
                (training_df, pd.DataFrame(train_data)),
                ignore_index=True,
            )

        training_df.to_json(train_output_file)
