# RheocerOS

# Development

## Setup Dev-Env / Debugging

We maintain the requirements file ("./requirements.txt"). 

Create your Python (3.7+) environment.

### Python

#### Option 1: Install Python via Conda

First install miniconda: https://conda.io/miniconda.html.

Update conda’s listing of packages for your system: 

    >> conda update conda

Test your installation: 

    >> conda list

For a successful installation, a list of installed packages appears.

Test that Python 3 is your default Python: 

    >> python -V

You should see something like 
    
    Python 3.7.x :: Anaconda, Inc.

----
Create conda environment:

    >> conda create --name RheocerOS python=3.7
    >> conda activate RheocerOS 

To deactivate an active environment, use

    >> conda deactivate

Now we can use this isolated environment and its python as the base interpreter.
Note: If we are going to use this environment within PyCharm, you might want to use Conda Package Manager,
to manage project packages in stead of PyCharm's default pip. 

#### Option 2: Within PyCharm

Download PyCharm from http://www.jetbrains.com/pycharm/download/

Simply jump to the next section below and use PyCharm for both Python setup and your environment. Choose
the right Python version (3.7) during the auto-installation by PyCharm.

### Setup PyCharm

* pull package and open it as new project
* mark 'src' folder as source
* Interpreter
    * Choose "Base Interpreter" from the steps above (Conda or PyCharm auto-download of Python 3.7+ from "python.org")
    * Choose 'virtualenv' For more details, see https://www.jetbrains.com/help/idea/creating-virtual-environment.html
    * Check the creation of 'venv' as a root directory.
* VirtualEnv: RheocerOS maintains "requirements.txt" and "requirements_test.txt" files for an easy setup of your own virtual-env. 
  * "requirements_test.txt": Use as is (for test-runs) or modify (for your local experimentation in ipython, local Jupyter notebook, etc)
    * Refer [Testing](#Testing) section below run unit or integ tests
  * "requirements.txt": restore the RELEASE configuration before integ-tests or application activations against cloud

## Building

## Testing

### Unit-tests

See [test/README.md](./test/README.md)

### integ-tests

See [test_integration README.md](./test_integration/README.md)

## Running

## Deploying

## Container Based Dev-Env

