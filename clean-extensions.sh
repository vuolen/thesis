find thesis_scraper/scraped -name "*.html; charset=UTF-8" -exec sh -c 'mv "$1" "${1%.html; charset=UTF-8}.html"' _ {} \;
find thesis_scraper/scraped -name "*.html; charset=utf-8" -exec sh -c 'mv "$1" "${1%.html; charset=utf-8}.html"' _ {} \;
find thesis_scraper/scraped -name "*.html; charset=utf-8.html" -exec sh -c 'mv "$1" "${1%.html; charset=utf-8.html}.html"' _ {} \;
find thesis_scraper/scraped -name "*.json; charset=UTF-8" -exec sh -c 'mv "$1" "${1%.json; charset=UTF-8}.json"' _ {} \;
find thesis_scraper/scraped -name "*.json; charset=utf-8" -exec sh -c 'mv "$1" "${1%.json; charset=utf-8}.json"' _ {} \;
