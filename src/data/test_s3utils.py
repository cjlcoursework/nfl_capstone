import os

from src.data.s3utils import missing_catalog_files, download_from_s3

def test_catalog():
    files_needed = missing_catalog_files(subdir=os.path.abspath("../../data/raw"))
    print("files not found:", files_needed)

def test_s3():
    download_from_s3(prefix="nfl_capstone/data/raw", local_dir=os.path.abspath("../../data/raw"), wishlist=['NFL Play by Play 2009-2018 (v5).csv'])


def test_s32():
    download_from_s3(prefix="nfl_capstone/data/raw", local_dir=os.path.abspath("../../data/raw"), wishlist=['NFL Play by Play 2009-2017 (v4).csv', 'dimensions.csv'])

def test_py():
    import nfl_data_py as nfl
    pbp = nfl.import_pbp_data([2009],  downcast=True, cache=False, alt_path=None)
    type(pbp)