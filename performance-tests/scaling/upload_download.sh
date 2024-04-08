# 1) Uploads the synthetic data to the HAPI server. This is a one-time setup,
# not interesting for performance measurement.

# 2) Runs the FHIR pipeline to fetch from HAPI. That's what we want to measure
# and test with different HAPI / DB configurations.

set -e # Fail on errors.
set -x # Show each command.

source ./variables.sh

GITS_DIR=~/gits
VENV=venv_fhir

cd $GITS_DIR/fhir-data-pipes

# Install Python virtual env.
[[ -d $VENV ]] || python3 -m venv $VENV
source $VENV/bin/activate
# Install dependencies for uploader.
pip install -r ./synthea-hiv/uploader/requirements.txt

python "$DIR_WITH_THIS_SCRIPT/upload_download.py"
