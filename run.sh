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

mkdir -p data/feeds data/files

# if arg is server, run the server else run the script
if [ "$1" == "server" ]; then
    pipenv run prefect server start
else if [ "$1" == "deploy" ]; then
    pipenv run python -m prefect_project.datasets
else
    echo "Please provide a valid argument: server or deploy"
    exit 1
fi