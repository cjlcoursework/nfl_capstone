import os
import boto3

AWS_S3_BUCKET = "cjl-project-data"
s3_client = boto3.client('s3')

"""
This is just a helper to download the static raw data from s3 
There are not many free good NFL API's 

This is only written for the current application and is not for reuse in other applications without significant improvement.
"""


# Hard code the raw files we care about
extensions = ['csv', 'parquet', 'json']  # these are the extensions we care about

catalog = ['games.csv',
           'nfl_stadiums.csv',
           'nfl_teams.csv',
           'nfl_teams_scraped.csv',
           'nflplaybyplay2009to2016/NFL Play by Play 2009-2016 (v3).csv',
           'nflplaybyplay2009to2016/NFL Play by Play 2009-2017 (v4).csv',
           'nflplaybyplay2009to2016/NFL Play by Play 2009-2018 (v5).csv',
           'spreadspoke_scores.csv']




def missing_catalog_files(subdir: str) -> list[str]:
    """
    What's missing from our catalog?
    :param subdir: the local subdirectory
    :return:
    """
    files_needed = []
    for file in catalog:
        full_path = os.path.join(subdir, file)
        print(f"Looking for {full_path}")
        if not os.path.exists(full_path):
            files_needed.append(os.path.basename(full_path))
    return files_needed


def download_from_s3(prefix, local_dir, wishlist):
    """
    Download select files from s3

    :param prefix:  the s3 subfolder
    :param local_dir: the local directory where we want our raw files
    :param wishlist: specify which files we need
    :return: None
    """

    # just get all the file names from s3 - this could be more efficient
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(AWS_S3_BUCKET)
    for obj in bucket.objects.filter(Prefix = prefix):

        # look for the extensions we want or continue
        if not obj.key[-3:] in extensions:
            print(f"ignore {obj.key} extension")
            continue

        # parse out what potentially nested local subdirectory we want the files to go
        sub_folder = os.path.dirname(obj.key)[len(prefix) + 1:]
        base_name = os.path.basename(obj.key)
        local_destination = os.path.join(local_dir, sub_folder, base_name)
        local_destination_dir = os.path.dirname(local_destination)

        # but if the file is not in the wishlist passed in, then just print and continue
        if base_name not in wishlist:
            print(f"Skip {base_name}: we don't need it right now.")
            continue

        # if the local files exists, then skip it todo - should have a force flag to overwrite if desired
        if os.path.exists(local_destination):
            print(f"Already exists:  {local_destination}")
            continue

        # if the required local directories don't exist then make them
        if not os.path.exists(local_destination_dir):
            os.makedirs(local_destination_dir)

        # DOWNLOAD our file
        print(f"DOWNLOAD {obj.key} to {local_destination}")
        bucket.download_file(obj.key, local_destination) # save to same path
