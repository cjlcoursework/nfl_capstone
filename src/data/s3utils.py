import os
import boto3

AWS_S3_BUCKET = "cjl-project-data"
s3_client = boto3.client('s3')

catalog = ['games.csv',
           'nfl_stadiums.csv',
           'nfl_teams.csv',
           'nfl_teams_scraped.csv',
           'nflplaybyplay2009to2016/NFL Play by Play 2009-2016 (v3).csv',
           'nflplaybyplay2009to2016/NFL Play by Play 2009-2017 (v4).csv',
           'nflplaybyplay2009to2016/NFL Play by Play 2009-2018 (v5).csv',
           'spreadspoke_scores.csv']

def missing_catalog_files(subdir):
    files_needed = []
    for file in catalog:
        full_path = os.path.join(subdir, file)
        print(f"Looking for {full_path}")
        if not os.path.exists(full_path):
            files_needed.append(os.path.basename(full_path))
    return files_needed


def download_from_s3(prefix, local_dir, wishlist):
    extensions = ['csv', 'parquet', 'json']
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(AWS_S3_BUCKET)
    for obj in bucket.objects.filter(Prefix = prefix):
        if not obj.key[-3:] in extensions:
            print(f"ignore {obj.key} extension")
            continue

        sub_folder = os.path.dirname(obj.key)[len(prefix) + 1:]
        base_name = os.path.basename(obj.key)
        local_destination = os.path.join(local_dir, sub_folder, base_name)
        local_destination_dir = os.path.dirname(local_destination)

        if (base_name not in wishlist):
            print(f"we don't need {base_name} right now.")
            continue

        if os.path.exists(local_destination):
            print(f"already exists:  {local_destination}")
            continue

        if not os.path.exists(local_destination_dir):
            os.makedirs(local_destination_dir)

        print(f"would download {obj.key} to {local_destination}")
        bucket.download_file(obj.key, local_destination) # save to same path
