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

# if arg is server, run the server, if deploy, run the deploy script, else echo error
if [ "$1" == "server" ]; then
    echo "Running server"
    pipenv run prefect server start
elif [ "$1" == "deploy" ]; then
    echo "Running deploy"
    pipenv run python -m prefect_project.datasets
elif [ "$1" == "scrapyd" ]; then
    echo "Running scrapyd"
    pipenv run scrapyd 
elif [ "$1" == "gui" ]; then
    echo "Running gui"
    pipenv run python3 gui/index.py
else
    echo "Please specify a command: server or deploy"
    exit 1
fi