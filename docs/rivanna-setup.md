# How to setup ray-hpc-workflows on Rivanna

## Setup your personal Miniforge environment

For this setup we shall use the Conda package manager.
We shall not be using the conda modules from Rivanna's module system.
They are frequently out of date.

Execute the following commands once you are logged into Rivanna.

```
# Download Miniforge.
$ wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh

# Install Miniforge.
# During installation accept license terms.
# Accept the default installation directory ($HOME/miniforge3)
# Allow conda to update your shell profile.
$ sh Miniforge3-Linux-x86_64.sh

# Execute bash to allow the new shell config to be used.
$ exec bash

# Conda environments can get pretty big.
# We shall now configure conda to use /project/bii_nssac for environments.
# Note, we don't want to use /scratch due to rolling 90 day deletion policy.
$ mkdir -p /project/bii_nssac/people/$USER/conda-envs
$ mkdir -p /project/bii_nssac/people/$USER/conda-pkgs-cache

# Next create/update your conda config ($HOME/.condarc)
$ Replace <USERNAME> below with your Rivanna username.
$ cat $HOME/.condarc
channels:
  - parantapa
  - conda-forge
auto_activate_base: false
auto_update_conda: false
add_pip_as_python_dependency: true
envs_dirs:
  - /project/bii_nssac/people/<USERNAME>/conda-envs
pkgs_dirs:
  - /project/bii_nssac/people/<USERNAME>/conda-pkgs-cache

# Update conda with the new config.
$ conda update -n base --all --yes
```

## Setup Cross-Desktop Group (XDG) user directories

Ray HPC workflows uses the XDG user directories
for storing temporary and runtime files.
On Rivanna we use `/scratch` for this

```
# Add the the following lines at the end of your $HOME/.bashrc
export XDG_CACHE_HOME="/scratch/$USER/var/cache"
export XDG_DATA_HOME="/scratch/$USER/var/data"
export XDG_STATE_HOME="/scratch/$USER/var/state"
export XDG_RUNTIME_DIR="/scratch/$USER/var/runtime"
```

Create the XDG directories.

```
# Execute bash to allow the new shell config to be used.
$ exec bash

# Now create the directories
$ mkdir -p "$XDG_CACHE_HOME"
$ mkdir -p "$XDG_DATA_HOME"
$ mkdir -p "$XDG_STATE_HOME"
$ mkdir -p "$XDG_RUNTIME_DIR"
```

## Create a conda environment for ray-hpc-workflows

We shall create a separate environment to install ray-hpc-workflows.

```
# Create and activate a conda environment called myenv
$ conda create -n myenv --yes python=3.12 nodejs=20 jupyterlab=4 nb_conda_kernels jupyterlab_widgets "numpy<2" scipy cmake ccache
$ conda activate myenv

# Clone ray-hpc-workflows repository in your home directory
$ cd $HOME
$ git clone https://github.com/NSSAC/ray-hpc-workflows.git


# Install ray-hpc-workflows
$ cd ray-hpc-workflows
$ pip install -e .
```

## Create default setup script for ray-hpc-workflows

We shall create a setup script for ray-hpc-workflows.
This is used to setup environments when they workflow creates slurm jobs.

```
$ cat $HOME/default-env.sh
module load gcc/13.3.0
module load openmpi/5.0.3
module load hdf5/1.14.4.3
module load cuda/12.4.1

export CMAKE_BUILD_PARALLEL_LEVEL=40
export CMAKE_BUILD_TYPE=Release

export _GLIBCXX_USE_CXX11_ABI=1

export HDF5_USE_FILE_LOCKING=FALSE
export CFLAGS="-march=x86-64 -mavx -mavx2 -mfma"
export CXXFLAGS="-march=x86-64 -mavx -mavx2 -mfma"

CONDA_ENVS_DIR="/project/bii_nssac/people/$USER/conda-envs"

export RAY_EXECUTABLE="${CONDA_ENVS_DIR}/myenv/bin/ray"
```

## Setup password for Jupyter Lab

Ensure that `myenv` is still activated.

```
$ jupyter lab password
```

## Set up Foxy Proxy on your local computer to access Jupyter

Install FoxyProxy Standard for your browser.

* [FoxyProxy Standard for Google Chrome](https://chromewebstore.google.com/detail/foxyproxy/gcknhkkoolaabfmlnjonogaaifnjlfnp?pli=1)
* [FoxyProxy Standard for Firefox](https://addons.mozilla.org/en-US/firefox/addon/foxyproxy-standard/)
* [FoxyProxy Standard for Edge](https://microsoftedge.microsoft.com/addons/detail/foxyproxy/flcnoalcefgkhkinjkffipfdhglnpnem)

Download the FoxyProxy configuration for Rivanna.

* [FoxyProxy Configuration for Rivanna](../extra/Rivanna-FoxyProxy_2024-06-06.json)

From your browser's addon section configure configure FoxyProxy
and import the configuration file.
Once imported, ensure that FoxyProxy is configured to enable proxy by patterns.

## Start jupyter from ray-hpc-workflows on Rivanna

Ensure you are logged into Rivanna via ssh.
Also ensure that `myenv` is still activated.

```
$ run-jupyter --type rivanna:bii --account <ACCOUNT-NAME> --runtime-h 2
```

Wait for the job to start.
You can monitor it using the `squeue` command.

```
$ squeue -u $USER
```

Once the job has started, note the hostname assigned to the job.
This should be under the NODELIST column of squeue's output.
This should be of the form `udc-XXX-XXX`

## Setup a socks proxy over ssh on your local computer to enable your browser to connect to Rivanna

On Linux and MacOS systems the following command should work.
Run the command in a new terminal.

```
ssh -o ExitOnForwardFailure=yes -N -D 127.0.0.1:12001 <username>@rivanna.hpc.virginia.edu
```

## Connect to the Jupyter notebook on Rivanna from your local browser

Use the following url: `http://udc-XXX-XXX:8888`
Replace the hostname part `udc-XXX-XXX` with the actual hostname where
jupyter notebook is running.

## Stop Jupyter Lab on Rivanna when no longer in use

Once your session is over ensure that you cancel any running Jupyter Lab or
ray-worker sessions.

Use `squeue` to see if any are running and use `scancel` to stop those jobs.
