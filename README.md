# SEC Python Web Scraper
This repository contains a Python Web scraper for parsing 13F filings (mutual fund holdings) from SEC's website, [EDGAR](https://www.sec.gov/edgar/searchedgar/companysearch.html), and writing a .tsv file from the data.


## Requirements

#### Getting Started
- `pip install -r requirements.txt` (or `pipenv install` if you are using pipenv)
- `python scraper.py` (or `pipenv run python scraper.py`)
- When prompted, enter the 10-digit CIK number of a mutual fund

#### Key Dependencies

- [Requests](https://2.python-requests.org/en/master/), Python library for making HTTP requests
- [lxml](https://lxml.de/), Python library for processing XML and HTML
- [Beautiful Soup](https://pypi.org/project/beautifulsoup4/), Python library for scraping information from Web pages
- [re](https://docs.python.org/3/library/re.html), Python module for using regular expressions
- [csv](https://docs.python.org/3/library/csv.html), Python module for parsing and writing CSV and TSV files

## Contributor
- [Gary Pang](https://github.com/CodeWritingCow)

## References
- [SEC: Frequently Asked Questions About Form 13F](https://www.sec.gov/divisions/investment/13ffaq.htm)


# TODO

- form 4 Form 4 - Insider Buying/Selling
