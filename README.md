# Data Scraper

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

#### Ingestion:

```
python -m ingestion.src.main

```

#### Processing:

```
python -m processing.src.main
```

### Test Command

```
python -m unittest ingestion.src.tests

```

```
python -m unittest processing.src.tests

```
