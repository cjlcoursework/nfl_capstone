nfl_capstone
==============================

------------
    Project: NFL statistics capstone 
    Phase:   Data Wrangling
------------

Manifest
------------
    Raw data located in s3:

        - nfl_teams.csv,  -- list of all NFL teasm and abbreviations
        - nfl_teams_scraped.csv,  -- another list of all NFL teasm and abbreviations scraped from html
        - spreadspoke_scores.csv  -- historical scores from all NFL games - one row per game

        - nflplaybyplay2009to2016/NFL Play by Play 2009-2016 (v3).csv,   -- gameplay data one row per play
        - nflplaybyplay2009to2016/NFL Play by Play 2009-2017 (v4).csv,
        - nflplaybyplay2009to2016/NFL Play by Play 2009-2018 (v5).csv,

        - games.csv,   -- not used yet
        - nfl_stadiums.csv,  -- not use yet


    Source code:
        - data/s3utils.py -- to get raw data from s3
        - features/wrangling/get_metrics.py -- to collect metrics from a generic dataset
        - features/wrangling/database_loader.py -- abstract sink that writes to csv or Postgres

       
    Notebooks:
        - 01-cjl-review.ipynb   -- initial review and cleanup
        - 02-cjl-clean.ipynb    -- more extensive cleaning
        - 03-cjl-clean.ipynb   -- merge and conform spreadspoke_scores.csv with NFL Play by Play*.csv

------------


Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
