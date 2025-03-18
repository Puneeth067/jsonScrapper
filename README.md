# JSON Data Scraper

## Overview

This script fetches employee data from an API, processes it, and stores it in CSV & Parquet.

## Setup

### Set Environment

```
python -m venv env
env/Scripts/activate
```

### Install Dependencies

```sh
pip install -r requirements.txt
```

### Run command

```sh
python -m ingestion.src.main
```

### Test Command

```sh
python -m unittest ingestion.src.tests
```
