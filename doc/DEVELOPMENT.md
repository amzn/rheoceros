# RheocerOS

# Development

## Setup Dev-Env / Debugging

We maintain the requirements file ("./requirements.txt"). Create your Python (3.7+) environment following
these steps. 

    2.1-
    Install Python via Conda
        https://ecshackweek.github.io/tutorial/installing-python-with-conda/

    Update conda’s listing of packages for your system: 
    >> conda update conda

    Test your installation: 
    >> conda list
    For a successful installation, a list of installed packages appears.

    Test that Python 3 is your default Python: 

    >> python -V
    You should see something like Python 3.7.x :: Anaconda, Inc.
    ----
    Create conda environment:

    >> conda create --name RheocerOS python=3.7 jinja2 pytest sphinx mypy pylint (... FUTURE to be extended)
    >> conda activate RheocerOS 
    To deactivate an active environment, use
    >> conda deactivate

    Now we can use this isolated environment and its python as the base interpreter.
    Note: If we are going to use this environment within PyCharm, you might want to use Conda Package Manager,
    to manage project packages in stead of PyCharm's default pip. 

    2.2- Simply jump to (3) below and use PyCharm for both Python setup and your environment. Choose
    the right Python version (3.7) during the initial auto-download that PyCharm will attempt.

3- For the rest (actual app development using RheocerOS):

3.1- VirtualEnv: RheocerOS maintains "requirements.txt" file for an easy setup of your own virtual-env. 

    3.1.1- You can use PyCharm to setup the rest (as Project Interpreter) once the above steps are done.
        - Choose "Base Interpreter" from the steps above (Conda or PyCharm auto-download of Python 3.7 from "python.org")
        - For more details, see https://www.jetbrains.com/help/idea/creating-virtual-environment.html
    3.1.2- TODO VIM setup. Also an example for console based development outside an IDE.
    
3.2- Conda Environment: 
    3.2.1- TODO PyCharm: download RheocerOS Conda env and then use it as "Existing Environment" 
    while creating a Conda Environment in PyCharm. https://www.jetbrains.com/help/idea/conda-support-creating-conda-virtual-environment.html
    3.2.2- TODO VIM setup. Also an example for console based development outside an IDE.

3.3- TBD

## Container Based Dev-Env

TODO

    ### Using CPython3

## Building

## Testing


## Running


## Deploying

