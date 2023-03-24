Springboard Data Science Capstone 2
==============================

------------
    Project: NFL statistics capstone 
    Author: Chris Lomeli
    Cohort:  January 2023
    Phase:   Data Wrangling
------------



During an NFL football game the offensive coaching staff would like to predict the next best play to call for each situation that comes up

### Approach
1. Gather the data from s3 or API
2. Stage to the local data directory that does not get uploaded to git
2. Clean the data 
3. Create a **metrics** dataset, somewhat like pandas_profiling, but not generalized for public use
4. Clean and verify that the various datasets will join to one another
5. Normalize the data into facts and dimensions so that it can also land in a data lake and be available for this and other uses.
6. profile the fact dataset and use it to perform EDA, feature engineering and modeling

### Project phases
- [x] Data Wrangling - current submission
- [ ] EDA - TBD
- [ ] Feature engineering - TBD
- [ ] Modeling - TBD


### Sources
Nextgen stats scrapped from nflverse -- https://github.com/nflverse
<br>
Static Gameplay data: https://www.kaggle.com/code/rtatman/data-cleaning-challenge-handling-missing-values
<br>
Historical scores: https://www.kaggle.com/code/kerneler/starter-nfl-scores-and-betting-data-80af2656-8
<br>
Team data: https://www.kaggle.com/code/kerneler/starter-nfl-scores-and-betting-data-80af2656-8


### Additional sources
- from sportsreference.nfl.boxscore import Boxscores, Boxscore
  Boxscores(1,2020).games


### Getting Started
- The notebooks are currently configured to pull static raw data from a public AWS s3 bucket
  - the other configuration option is the nflverse API
- The metrics data is configured to output to csv files
  - the other option is Postgres, which I am using

- order of operation - the notebooks depend on one another and should be run in order



Manifest
------------
    Raw data located in s3:

        - nfl_teams.csv,  -- list of all NFL teasm and abbreviations
        - nfl_teams_scraped.csv,  -- another list of all NFL teasm and abbreviations scraped from html
        - spreadspoke_scores.csv  -- historical scores from all NFL games - one row per game

        - nflplaybyplay2009to2016/NFL Play by Play 2009-2016 (v3).csv,   -- gameplay data one row per play - todo: get this data from the nflverse API
        - nflplaybyplay2009to2016/NFL Play by Play 2009-2017 (v4).csv,
        - nflplaybyplay2009to2016/NFL Play by Play 2009-2018 (v5).csv,

        - games.csv,   -- not used yet
        - nfl_stadiums.csv,  -- not use yet


    Source code:
        - data/s3utils.py -- used to get raw data from s3
        - data/configurations.py -- a place for schemas, and any other future configurations
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
