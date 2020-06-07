#
# covid-br/job.py - Fetch daily data about Covid from the brazilian health ministry and agregate with past data
#

# Imports
import boto3
import requests
import json
import os
from datetime import date, timedelta, datetime
from pprint import pprint

# Vars
health_ministry_json_data_url = os.environ.get('HEALTH_MINISTRY_JSON_DATA_URL')
covid_br_url = os.environ.get('COVID_BR_URL')
destination_bucket = os.environ.get('DESTINATION_BUCKET')

# Action! o/
def lambda_handler(event, context):
    ## Fetch Current Data
    health_ministry_data = requests.get(health_ministry_json_data_url)
    current_data = json.loads(health_ministry_data.text)

    ## Fetch Yesterday Data
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    yesterday_json_data_url = '%s/%s.json' % (covid_br_url, yesterday)
    covid_br_yesterday_data = requests.get(yesterday_json_data_url)
    yesterday_data = json.loads(covid_br_yesterday_data.text)

    # Calculate New Values and setup output dict
    output = dict()
    if yesterday_data['casos']['novos'] == current_data['confirmados']['novos'] and yesterday_data['obitos']['novos'] == current_data['obitos']['novos']:
        output = {
            'casos': {
                'novos': yesterday_data['casos']['novos'],
                'total': yesterday_data['casos']['total'],
            },
            'obitos': {
                'novos': yesterday_data['obitos']['novos'],
                'total': yesterday_data['obitos']['total'],
            },
            'dt_updated': yesterday_data['dt_updated'],
        }
    else:
        output = {
            'casos': {
                'novos':  current_data['confirmados']['novos'],
                'total': yesterday_data['casos']['total'] + current_data['confirmados']['novos'],
            },
            'obitos': {
                'novos': current_data['obitos']['novos'],
                'total': yesterday_data['obitos']['total'] + current_data['obitos']['novos'],
            },
            'dt_updated': current_data['dt_updated'],
        }

    # Upload New Values
    today = date.today().strftime('%Y-%m-%d')
    client = boto3.client('s3')
    client.put_object(Body=json.dumps(output), Bucket=destination_bucket, Key='%s.json' % (today))
    client.put_object(Body=json.dumps(output), Bucket=destination_bucket, Key='latest.json')
