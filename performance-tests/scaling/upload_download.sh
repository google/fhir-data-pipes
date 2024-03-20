set -e # Fail on errors.
set -x # Show each command.

DIR_WITH_THIS_SCRIPT="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
source "$DIR_WITH_THIS_SCRIPT/variables.sh"

GITS_DIR=~/gits

cd $GITS_DIR/fhir-data-pipes

# Install Python virtual env.
[[ -d "venv" ]] || python3 -m venv venv
source venv/bin/activate

# Install dependencies for uploader.
pip install -r ./synthea-hiv/uploader/requirements.txt

python "$DIR_WITH_THIS_SCRIPT/upload_download.py"
