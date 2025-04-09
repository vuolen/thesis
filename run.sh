mkdir -p data/feeds data/files data/.dagster_home

docker build --tag="thesis_scraper" .

docker run --rm \
    -v "$(pwd)/data:/app/data" \
    -p 3000:3000 \
    thesis_scraper