## Google Authentification
!pip install google-auth-httplib2 google-api-python-client

import re
import google_auth_httplib2
from google.oauth2 import service_account
from googleapiclient import discovery
import hvac

import collections.abc
import json
import datetime

import pytz
import requests

from skt.gcp import bq_to_pandas

import uuid
import httplib2

env = "prd"
tap_name = "recipes_cross"

sheet_id = "1YlyegB4mXl2CwDpjECFO_6lGdR6uRRcH2CrthcBck7w"
ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'


def _get_proxy():
    var = "http://10.40.84.229:10203"
    PROXY = re.sub(r"https?://", "", var[: var.rindex(":")])
    PORT = int(var[var.rindex(":") + 1 :])
    proxy_http = httplib2.Http(
        proxy_info=httplib2.ProxyInfo(httplib2.socks.PROXY_TYPE_HTTP, PROXY, PORT)
    )
    return proxy_http

def _get_google_credentials():
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    path = "gspread_key.json"
    credentials = service_account.Credentials.from_service_account_file(
        path, scopes=scopes
    )
    return credentials


# BAP API Functions
API_PRD_KEY = "7fe8acef-47cb-4517-8ce5-64b427ade548"
API_STG_KEY = "734146c6-a1b5-4ac7-a0ed-401fede08877"
BAP_PRD_URL = "https://recipe.bap.apollo-ai.io/api/v1/recipes"
BAP_STG_URL = "https://recipe.stg.bap.apollo-ai.io/api/v1/recipes"

def get_recipe(recipe_name, env):
    api_key = API_PRD_KEY if env == "prd" else API_STG_KEY
    bap_url = BAP_PRD_URL if env == "prd" else BAP_STG_URL
    headers = {
        "x-api-key": api_key,
    }
    url = f"{bap_url}/{recipe_name}"
    res = requests.get(url, headers=headers)
    return res.json().get("data")

def create_recipe(recipe, env):
    api_key = API_PRD_KEY if env == "prd" else API_STG_KEY
    bap_url = BAP_PRD_URL if env == "prd" else BAP_STG_URL
    headers = {
        "x-api-key": api_key,
    }
    res = requests.post(bap_url, data=json.dumps(recipe), headers=headers)
    res.raise_for_status()
    return res

def update_recipe(recipe_name, recipe, env):
    api_key = API_PRD_KEY if env == "prd" else API_STG_KEY
    bap_url = BAP_PRD_URL if env == "prd" else BAP_STG_URL
    headers = {
        "x-api-key": api_key,
    }
    url = f"{bap_url}/{recipe_name}"
    res = requests.put(url, data=json.dumps(recipe), headers=headers)
    res.raise_for_status()
    return res

def recipe_enable(recipe_name, env):
    api_key = API_PRD_KEY if env == "prd" else API_STG_KEY
    bap_url = BAP_PRD_URL if env == "prd" else BAP_STG_URL
    headers = {
        "x-api-key": api_key,
    }
    url = f"{bap_url}/{recipe_name}/enable"
    res = requests.post(url, headers=headers)
    res.raise_for_status()
    return res

def recipe_disable(recipe_name, env):
    api_key = API_PRD_KEY if env == "prd" else API_STG_KEY
    bap_url = BAP_PRD_URL if env == "prd" else BAP_STG_URL
    headers = {
        "x-api-key": api_key,
    }
    url = f"{bap_url}/{recipe_name}/disable"
    res = requests.post(url, headers=headers)
    res.raise_for_status()
    return res


def recipe_delete(recipe_name, env):
    api_key = API_PRD_KEY if env == "prd" else API_STG_KEY
    bap_url = BAP_PRD_URL if env == "prd" else BAP_STG_URL
    headers = {
        "x-api-key": api_key,
    }    
    url = f"{bap_url}/{recipe_name}"
    res = requests.delete(url, headers=headers)
    res.raise_for_status()
    return res


## Reading GCP table
query = '''
    SELECT pr_title
        FROM `skt-datahub.adot_datamart.adot_tv_poc_target_drama`
            where partition_id = 14
                order by type
    '''

drama_name = list(bq_to_pandas(query)["pr_title"])
#drama_name = ['하지말라면더하고19', '365:운명을거스르는1년', '낭만닥터김사부2', '낭만닥터김사부', '더킹:영원의군주']
drama_cnt = len(drama_name)


for i in range (0, drama_cnt):
    if drama_name[i] == '하지말라면더하고19':  drama_name[i] ='하지 말라면 더 하고 19'
    
recipe_names = []
name_prefix = ["BN_", "AN_"]
name_body = "CROSSDOMAIN_"
name_postfix = []

for i in range (0, drama_cnt):
    name_postfix.append("DM0%d"%(i+1)) if i < 10 else name_postfix.append("DM%d"%(i+1))

for prefix in name_prefix:
    for postfix in name_postfix:
        recipe_names.append(prefix + name_body + postfix)

        
## Recipe Generation
def get_timestamp():
    tz = pytz.timezone("UTC")
    now = str(datetime.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S.%f%z"))
    return now[:-2] + ":" + now[-2:]

now_timestamp = get_timestamp()
start_date_AN = datetime.date.today().strftime("%Y-%m-%d")
end_date_AN = start_date_AN

start_date_BN = (datetime.date.today()+datetime.timedelta(days=1)).strftime("%Y-%m-%d")
end_date_BN = start_date_BN


def generating_recipe(drama_name, prefix, postfix, environment):
    start_date = start_date_BN if prefix == "BN_" else start_date_AN
    end_date = end_date_BN if prefix == "BN_" else end_date_AN
    cron = '30 8 * * *' if prefix == "BN_" else '30 17 * * *'
    banoon = ", 오전" if prefix == "BN_" else ", 오후"
                
    recipe = {}
    recipe["uid"] = str(uuid.uuid4())
    recipe["recipe_name"] = prefix + name_body + postfix
    recipe["recipe_type"] = "schedule"
    recipe["description"] = "교차추천, 드라마>OST, " + drama_name + banoon
    recipe["domain"] = "Entertainment_Music"
    recipe["start_date"] = start_date
    recipe["end_date"] = end_date
    recipe["skip_config"] = {}
    recipe["skip_config"]["period"] = 86400
    recipe["skip_config"]["count"] = 0
    recipe["enabled"] = True
    recipe["target_table"] = "bap-target-" + prefix[:2] + "-" + name_body + postfix
    recipe["condition"] = {}
    recipe["condition"]["type"] = "condition"
    recipe["condition"]["params"] = {}
    recipe["condition"]["params"]["min"] = 15
    recipe["condition"]["params"]["max"] = 99
    recipe["condition"]["condition_name"] = "AGE"
    recipe["action"] = {}
    recipe["action"]["notification"] = {}
    recipe["action"]["notification"]["title_t_text"] = {}
    recipe["action"]["notification"]["title_t_text"]["formal"] = drama_name + " 재밌게 본 당신!"
    recipe["action"]["notification"]["title_t_text"]["informal"] = drama_name + ", 재밌게 봤었지?"
    recipe["action"]["notification"]["message_t_text"] = {}
    recipe["action"]["notification"]["message_t_text"]["formal"] = "OST 다시 들어보실래요?"
    recipe["action"]["notification"]["message_t_text"]["informal"] = "OST 다시 들어볼래?"
    recipe["action"]["notification"]["action"] = {}
    recipe["action"]["notification"]["action"]["type"] = "EVENT"
    recipe["action"]["notification"]["action"]["text"] = "드라마 " + drama_name + " OST 들려줘"
    recipe["action"]["notification"]["action"]["play_service_id"] = "apollo.builtin.music"
    recipe["action"]["notification"]["type"] = "os"
    recipe["action"]["notification"]["save"] = True
    recipe["action"]["notification"]["separated"] = True
    recipe["action"]["tts_trigger"] = None
    recipe["action"]["templates"] = None
    recipe["tags"] = ['context', 'cross_domain']
    recipe["console_metadata"] = {}
    recipe["console_metadata"]["actionEditMode"] = "form"
    recipe["created_at"] = now_timestamp
    recipe["updated_at"] = now_timestamp
    recipe["schedule"] = {}
    recipe["schedule"]["cron"] = cron
    
    return(recipe)


# Wrting Recipes to Sheet
credentials = _get_google_credentials()
authorized_http = google_auth_httplib2.AuthorizedHttp(credentials, http=_get_proxy())
service = discovery.build('sheets', 'v4', http=authorized_http, cache_discovery=False)

def write_range(range_name, values):
    body = {
        'values': values
    }
    result = service.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=range_name,
        valueInputOption='USER_ENTERED', body=body).execute()
    
def writing_to_sheet(recipe, tap_name, index):
    content = [[recipe["enabled"]], [recipe["recipe_name"]], [recipe["description"]], [str(recipe["tags"])], [recipe["domain"]],
               [recipe["start_date"]], [recipe["end_date"]], [recipe["schedule"]["cron"]], [recipe["skip_config"]["period"]],
               [recipe["skip_config"]["count"]], [recipe["target_table"]], ["NA"],
               [recipe["condition"]["params"]["min"]], [recipe["condition"]["params"]["max"]],
               [recipe["action"]["notification"]["save"]], [recipe["action"]["notification"]["separated"]],
               [recipe["action"]["notification"]["type"]],
               [recipe["action"]["notification"]["title_t_text"]["formal"]],
               [recipe["action"]["notification"]["message_t_text"]["formal"]],
               [recipe["action"]["notification"]["title_t_text"]["informal"]],
               [recipe["action"]["notification"]["message_t_text"]["informal"]],
               [recipe["action"]["notification"]["action"]["type"]],
               [recipe["action"]["notification"]["action"]["text"]], [recipe["action"]["notification"]["action"]["play_service_id"]],
               [str(recipe["action"]["tts_trigger"])]]
    
    pin = int((index-20)/26)
    SINGLE_LETTER_COLUMN = '%s!%s2:%s%d'%(tap_name, ALPHABET[6+index], ALPHABET[6+index], len(content)+2)
    DOUBLE_LETTER_COLUMN = '%s!%s2:%s%d'%(tap_name, ALPHABET[pin]+ALPHABET[index-pin*26-20], ALPHABET[pin]+ALPHABET[index-pin*26-20], len(content)+2)
    range_name = SINGLE_LETTER_COLUMN if index < 20 else DOUBLE_LETTER_COLUMN

    write_range(range_name, content)
    print("Writing %s finished"%recipe["recipe_name"])


# Recipe writing to Google Sheet
iteration = 0
for prefix in name_prefix:
    for i in range (0, len(name_postfix)):
        drama = drama_name[i]
        postfix = name_postfix[i]
        recipe_name = prefix+name_body+postfix
        recipe = generating_recipe(drama, prefix, postfix, env)
        
        flag = True if recipe["enabled"] in [True, "true", 1] else False

        writing_to_sheet(recipe, tap_name, iteration)
        iteration += 1


# delete recipes reigh-side
blank = []
for i in range (0, 25): blank.append([""])
for index in range (iteration, iteration + 5): 
    pin = int((index-20)/26)
    if index < 20 :
        range_name = '%s!%s2:%s%d'%(tap_name, ALPHABET[6+index], ALPHABET[6+index], len(blank)+1)
    elif index >= 20:
        range_name = '%s!%s2:%s%d'%(tap_name, ALPHABET[pin]+ALPHABET[index-pin*26-20], ALPHABET[pin]+ALPHABET[index-pin*26-20], len(blank)+1)
    write_range(range_name, blank)        


## Upload Recipes to BAP 
for prefix in name_prefix:
    for i in range (0, len(name_postfix)):
        drama = drama_name[i]
        postfix = name_postfix[i]
        recipe_name = prefix+name_body+postfix
        recipe = generating_recipe(drama, prefix, postfix, env)
        flag = True if recipe["enabled"] in [True, "true", 1] else False

        present_recipe = get_recipe(recipe_name, env)

        if present_recipe:
            print(f"Recipe Name: %s already exists"%recipe_name)
            print(f"Updating {recipe_name}...")

            del recipe["uid"]
            del recipe["recipe_type"]
            del recipe["enabled"]
            del recipe["created_at"]
            del recipe["updated_at"]

            res = update_recipe(recipe_name, recipe, env)
            recipe_enable(recipe_name, env) if flag == True else recipe_disable(recipe_name, env)
            print("updated")

        else:
            print(f"Creating {recipe_name}...")
            create_recipe(recipe, env)
            print("created")
