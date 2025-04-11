# see if rga exists
if ! command -v rga &> /dev/null
then
    echo "rga could not be found, please install it first"
    exit 1
fi
# see if rga-preproc exists
if ! command -v rga-preproc &> /dev/null
then
    echo "rga-preproc could not be found, please install it first"
    exit 1
fi
# see if pipenv exists
if ! command -v pipenv &> /dev/null
then
    echo "pipenv could not be found, please install it first"
    exit 1
fi

mkdir -p data/feeds data/files data/.dagster_home
export DAGSTER_HOME=$(pwd)/data/.dagster_home
export DAGSTER_FEEDS_DIR=$(pwd)/data/feeds
export DAGSTER_FILES_DIR=$(pwd)/data/files

pipenv run prefect config set PREFECT_RESULTS_PERSIST_BY_DEFAULT=true
pipenv run prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
prefect config set PREFECT_LOCAL_STORAGE_PATH='./.prefect/storage'

pipenv run dagster dev -m dagster_project --working-directory .