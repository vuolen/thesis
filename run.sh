# see if rga exists
if ! command -v rga &> /dev/null
then
    echo "rga could not be found, please install it first"
    exit 1
fi
# see if pipenv exists
if ! command -v pipenv &> /dev/null
then
    echo "pipenv could not be found, please install it first"
    exit 1
fi

mkdir -p data/feeds data/files data/.dagster_home
DAGSTER_HOME=$(pwd)/data/.dagster_home
DAGSTER_FEEDS_DIR=$(pwd)/data/feeds
DAGSTER_FILES_DIR=$(pwd)/data/files

pipenv run dagster dev -m dagster_project --working-directory .