"""Start a Jupyter Lab instance."""

from pathlib import Path
from textwrap import dedent

import click

from .utils import find_jupyter, find_setup_script
from .slurm_job_manager import SlurmJobManager
from .ray_cluster import SBATCH_ARGS


@click.command()
@click.option(
    "--type",
    required=True,
    type=click.Choice(list(SBATCH_ARGS)),
    help="Type of allocation for job.",
)
@click.option("--account", required=True, type=str, help="Account to be used.")
@click.option(
    "--runtime-h", type=int, default=16, show_default=True, help="Job runtime in hours."
)
@click.option("--qos", type=str, help="QOS to use.")
@click.option("--reservation", type=str, help="Reservation to use.")
@click.option(
    "--setup-script",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path),
    help="Path to setup script.",
)
def run_jupyter(
    type: str,
    account: str,
    runtime_h: int,
    qos: str | None,
    reservation: str | None,
    setup_script: Path | None,
):
    """Start a Jupyter Lab instance."""
    name = "jupyter"

    sbatch_args = SBATCH_ARGS[type]
    sbatch_args.append(f"--account {account}")
    sbatch_args.append(f"--runtime {runtime_h}:00:00")
    if qos:
        sbatch_args.append(f"--qos {qos}")
    if reservation:
        sbatch_args.append(f"--reservation {reservation}")

    jupyter_executable = find_jupyter()
    setup_script = find_setup_script(setup_script)

    script = rf"""
    . "/etc/profile"
    . '{setup_script!s}'

    export RAY_SCHEDULER_EVENTS=0

    HOST="$(hostname)"
    PORT=8888

    echo "Jupyter url: http://$HOST:$PORT"

    set -Eeuo pipefail
    set -x

    exec '{jupyter_executable!s}' lab \
        --ip "$HOST" --port "$PORT" \
        --ServerApp.disable_check_xsrf=True \
        --notebook-dir="$HOME" \
        --no-browser
    """
    script = dedent(script)

    sjm = SlurmJobManager(cancel_on_close=False)
    job = sjm.submit(name=name, sbatch_args=sbatch_args, script=script)

    print(f"Job ID: {job.job_id}")
    print(f"Output file: {job.output_file!s}")
