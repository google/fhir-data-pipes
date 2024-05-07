# 1) Uploads the synthetic data to the HAPI server. This is a one-time setup,
# not interesting for performance measurement.

# 2) Runs the FHIR pipeline to fetch from HAPI. That's what we want to measure
# and test with different HAPI / DB configurations.

set -e # Fail on errors.
set -x # Show each command.

source ./variables.sh

GITS_DIR=~/gits
VENV=~/venv_fhir
NOHUP_OUT=~/nohup-up.out

cd $GITS_DIR/fhir-data-pipes

# Install Python virtual env.
if [[ -d $VENV ]]; then
  source $VENV/bin/activate
else
  python3 -m venv $VENV
  source $VENV/bin/activate
  # Install dependencies for uploader.
  pip install -r ./synthea-hiv/uploader/requirements.txt
fi

nohup python "$DIR_WITH_THIS_SCRIPT/upload_download.py" >> $NOHUP_OUT 2>&1 &
tail -F $NOHUP_OUT

