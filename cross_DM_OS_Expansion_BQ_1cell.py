<<<<<<< HEAD
{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 106,
   "id": "7ddc9b67-6107-41d6-a7e7-cc49375d9d94",
   "metadata": {
    "execution": {
     "iopub.execute_input": "2023-05-16T00:12:01.342437Z",
     "iopub.status.busy": "2023-05-16T00:12:01.341984Z",
     "iopub.status.idle": "2023-05-16T00:12:12.083366Z",
     "shell.execute_reply": "2023-05-16T00:12:12.082854Z",
     "shell.execute_reply.started": "2023-05-16T00:12:01.342413Z"
    },
    "tags": []
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Requirement already satisfied: google-auth-httplib2 in /opt/conda/lib/python3.8/site-packages (0.1.0)\n",
      "Requirement already satisfied: google-api-python-client in /opt/conda/lib/python3.8/site-packages (2.86.0)\n",
      "Requirement already satisfied: six in /opt/conda/lib/python3.8/site-packages (from google-auth-httplib2) (1.16.0)\n",
      "Requirement already satisfied: httplib2>=0.15.0 in /opt/conda/lib/python3.8/site-packages (from google-auth-httplib2) (0.20.4)\n",
      "Requirement already satisfied: google-auth in /opt/conda/lib/python3.8/site-packages (from google-auth-httplib2) (1.35.0)\n",
      "Requirement already satisfied: uritemplate<5,>=3.0.1 in /opt/conda/lib/python3.8/site-packages (from google-api-python-client) (4.1.1)\n",
      "Requirement already satisfied: google-api-core!=2.0.*,!=2.1.*,!=2.2.*,!=2.3.0,<3.0.0dev,>=1.31.5 in /opt/conda/lib/python3.8/site-packages (from google-api-python-client) (1.32.0)\n",
      "Requirement already satisfied: protobuf<4.0.0dev,>=3.12.0 in /opt/conda/lib/python3.8/site-packages (from google-api-core!=2.0.*,!=2.1.*,!=2.2.*,!=2.3.0,<3.0.0dev,>=1.31.5->google-api-python-client) (3.20.1)\n",
      "Requirement already satisfied: packaging>=14.3 in /opt/conda/lib/python3.8/site-packages (from google-api-core!=2.0.*,!=2.1.*,!=2.2.*,!=2.3.0,<3.0.0dev,>=1.31.5->google-api-python-client) (21.3)\n",
      "Requirement already satisfied: requests<3.0.0dev,>=2.18.0 in /opt/conda/lib/python3.8/site-packages (from google-api-core!=2.0.*,!=2.1.*,!=2.2.*,!=2.3.0,<3.0.0dev,>=1.31.5->google-api-python-client) (2.28.0)\n",
      "Requirement already satisfied: setuptools>=40.3.0 in /opt/conda/lib/python3.8/site-packages (from google-api-core!=2.0.*,!=2.1.*,!=2.2.*,!=2.3.0,<3.0.0dev,>=1.31.5->google-api-python-client) (60.7.1)\n",
      "Requirement already satisfied: pytz in /opt/conda/lib/python3.8/site-packages (from google-api-core!=2.0.*,!=2.1.*,!=2.2.*,!=2.3.0,<3.0.0dev,>=1.31.5->google-api-python-client) (2021.3)\n",
      "Requirement already satisfied: googleapis-common-protos<2.0dev,>=1.6.0 in /opt/conda/lib/python3.8/site-packages (from google-api-core!=2.0.*,!=2.1.*,!=2.2.*,!=2.3.0,<3.0.0dev,>=1.31.5->google-api-python-client) (1.56.2)\n",
      "Requirement already satisfied: rsa<5,>=3.1.4 in /opt/conda/lib/python3.8/site-packages (from google-auth->google-auth-httplib2) (4.8)\n",
      "Requirement already satisfied: pyasn1-modules>=0.2.1 in /opt/conda/lib/python3.8/site-packages (from google-auth->google-auth-httplib2) (0.2.8)\n",
      "Requirement already satisfied: cachetools<5.0,>=2.0.0 in /opt/conda/lib/python3.8/site-packages (from google-auth->google-auth-httplib2) (4.2.4)\n",
      "Requirement already satisfied: pyparsing!=3.0.0,!=3.0.1,!=3.0.2,!=3.0.3,<4,>=2.4.2 in /opt/conda/lib/python3.8/site-packages (from httplib2>=0.15.0->google-auth-httplib2) (3.0.7)\n",
      "Requirement already satisfied: pyasn1<0.5.0,>=0.4.6 in /opt/conda/lib/python3.8/site-packages (from pyasn1-modules>=0.2.1->google-auth->google-auth-httplib2) (0.4.8)\n",
      "Requirement already satisfied: urllib3<1.27,>=1.21.1 in /opt/conda/lib/python3.8/site-packages (from requests<3.0.0dev,>=2.18.0->google-api-core!=2.0.*,!=2.1.*,!=2.2.*,!=2.3.0,<3.0.0dev,>=1.31.5->google-api-python-client) (1.26.8)\n",
      "Requirement already satisfied: certifi>=2017.4.17 in /opt/conda/lib/python3.8/site-packages (from requests<3.0.0dev,>=2.18.0->google-api-core!=2.0.*,!=2.1.*,!=2.2.*,!=2.3.0,<3.0.0dev,>=1.31.5->google-api-python-client) (2022.6.15)\n",
      "Requirement already satisfied: idna<4,>=2.5 in /opt/conda/lib/python3.8/site-packages (from requests<3.0.0dev,>=2.18.0->google-api-core!=2.0.*,!=2.1.*,!=2.2.*,!=2.3.0,<3.0.0dev,>=1.31.5->google-api-python-client) (3.3)\n",
      "Requirement already satisfied: charset-normalizer~=2.0.0 in /opt/conda/lib/python3.8/site-packages (from requests<3.0.0dev,>=2.18.0->google-api-core!=2.0.*,!=2.1.*,!=2.2.*,!=2.3.0,<3.0.0dev,>=1.31.5->google-api-python-client) (2.0.11)\n",
      "\n",
      "\u001b[1m[\u001b[0m\u001b[34;49mnotice\u001b[0m\u001b[1;39;49m]\u001b[0m\u001b[39;49m A new release of pip available: \u001b[0m\u001b[31;49m22.1.2\u001b[0m\u001b[39;49m -> \u001b[0m\u001b[32;49m23.1.2\u001b[0m\n",
      "\u001b[1m[\u001b[0m\u001b[34;49mnotice\u001b[0m\u001b[1;39;49m]\u001b[0m\u001b[39;49m To update, run: \u001b[0m\u001b[32;49mpip install --upgrade pip\u001b[0m\n",
      "query: \n",
      "    SELECT pr_title\n",
      "        FROM `skt-datahub.adot_datamart.adot_tv_poc_target_drama`\n",
      "            where partition_id = 14\n",
      "                order by type\n",
      "    \n",
      "destination: skt-datahub._775c5ccab1096b3cccd7ac34a5db11c0a354fb07.anon902a75835f56a95cea76a10e4d9cc6d658dfeb80b9a9b556db8dbbee4257506d\n",
      "total_rows: 2\n",
      "slot_secs: 0.049\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "Downloading: 100%|██████████| 2/2 [00:00<00:00,  2.84rows/s]\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Writing BN_CROSSDOMAIN_DM01 finished\n",
      "Writing BN_CROSSDOMAIN_DM02 finished\n",
      "Writing AN_CROSSDOMAIN_DM01 finished\n",
      "Writing AN_CROSSDOMAIN_DM02 finished\n",
      "Recipe Name: BN_CROSSDOMAIN_DM01 already exists\n",
      "Updating BN_CROSSDOMAIN_DM01...\n",
      "updated\n",
      "Recipe Name: BN_CROSSDOMAIN_DM02 already exists\n",
      "Updating BN_CROSSDOMAIN_DM02...\n",
      "updated\n",
      "Recipe Name: AN_CROSSDOMAIN_DM01 already exists\n",
      "Updating AN_CROSSDOMAIN_DM01...\n",
      "updated\n",
      "Recipe Name: AN_CROSSDOMAIN_DM02 already exists\n",
      "Updating AN_CROSSDOMAIN_DM02...\n",
      "updated\n"
     ]
    }
   ],
   "source": [
    "## Google Authentification\n",
    "!pip install google-auth-httplib2 google-api-python-client\n",
    "\n",
    "import re\n",
    "import google_auth_httplib2\n",
    "from google.oauth2 import service_account\n",
    "from googleapiclient import discovery\n",
    "import hvac\n",
    "\n",
    "import collections.abc\n",
    "import json\n",
    "import datetime\n",
    "\n",
    "import pytz\n",
    "import requests\n",
    "\n",
    "from skt.gcp import bq_to_pandas\n",
    "\n",
    "import uuid\n",
    "import httplib2\n",
    "\n",
    "env = \"prd\"\n",
    "tap_name = \"recipes_cross\"\n",
    "\n",
    "sheet_id = \"1YlyegB4mXl2CwDpjECFO_6lGdR6uRRcH2CrthcBck7w\"\n",
    "ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'\n",
    "\n",
    "\n",
    "def _get_proxy():\n",
    "    var = \"http://10.40.84.229:10203\"\n",
    "    PROXY = re.sub(r\"https?://\", \"\", var[: var.rindex(\":\")])\n",
    "    PORT = int(var[var.rindex(\":\") + 1 :])\n",
    "    proxy_http = httplib2.Http(\n",
    "        proxy_info=httplib2.ProxyInfo(httplib2.socks.PROXY_TYPE_HTTP, PROXY, PORT)\n",
    "    )\n",
    "    return proxy_http\n",
    "\n",
    "def _get_google_credentials():\n",
    "    scopes = [\n",
    "        \"https://www.googleapis.com/auth/drive\",\n",
    "        \"https://www.googleapis.com/auth/drive.file\",\n",
    "        \"https://www.googleapis.com/auth/spreadsheets\",\n",
    "    ]\n",
    "    path = \"gspread_key.json\"\n",
    "    credentials = service_account.Credentials.from_service_account_file(\n",
    "        path, scopes=scopes\n",
    "    )\n",
    "    return credentials\n",
    "\n",
    "\n",
    "# BAP API Functions\n",
    "API_PRD_KEY = \"7fe8acef-47cb-4517-8ce5-64b427ade548\"\n",
    "API_STG_KEY = \"734146c6-a1b5-4ac7-a0ed-401fede08877\"\n",
    "BAP_PRD_URL = \"https://recipe.bap.apollo-ai.io/api/v1/recipes\"\n",
    "BAP_STG_URL = \"https://recipe.stg.bap.apollo-ai.io/api/v1/recipes\"\n",
    "\n",
    "def get_recipe(recipe_name, env):\n",
    "    api_key = API_PRD_KEY if env == \"prd\" else API_STG_KEY\n",
    "    bap_url = BAP_PRD_URL if env == \"prd\" else BAP_STG_URL\n",
    "    headers = {\n",
    "        \"x-api-key\": api_key,\n",
    "    }\n",
    "    url = f\"{bap_url}/{recipe_name}\"\n",
    "    res = requests.get(url, headers=headers)\n",
    "    return res.json().get(\"data\")\n",
    "\n",
    "def create_recipe(recipe, env):\n",
    "    api_key = API_PRD_KEY if env == \"prd\" else API_STG_KEY\n",
    "    bap_url = BAP_PRD_URL if env == \"prd\" else BAP_STG_URL\n",
    "    headers = {\n",
    "        \"x-api-key\": api_key,\n",
    "    }\n",
    "    res = requests.post(bap_url, data=json.dumps(recipe), headers=headers)\n",
    "    res.raise_for_status()\n",
    "    return res\n",
    "\n",
    "def update_recipe(recipe_name, recipe, env):\n",
    "    api_key = API_PRD_KEY if env == \"prd\" else API_STG_KEY\n",
    "    bap_url = BAP_PRD_URL if env == \"prd\" else BAP_STG_URL\n",
    "    headers = {\n",
    "        \"x-api-key\": api_key,\n",
    "    }\n",
    "    url = f\"{bap_url}/{recipe_name}\"\n",
    "    res = requests.put(url, data=json.dumps(recipe), headers=headers)\n",
    "    res.raise_for_status()\n",
    "    return res\n",
    "\n",
    "def recipe_enable(recipe_name, env):\n",
    "    api_key = API_PRD_KEY if env == \"prd\" else API_STG_KEY\n",
    "    bap_url = BAP_PRD_URL if env == \"prd\" else BAP_STG_URL\n",
    "    headers = {\n",
    "        \"x-api-key\": api_key,\n",
    "    }\n",
    "    url = f\"{bap_url}/{recipe_name}/enable\"\n",
    "    res = requests.post(url, headers=headers)\n",
    "    res.raise_for_status()\n",
    "    return res\n",
    "\n",
    "def recipe_disable(recipe_name, env):\n",
    "    api_key = API_PRD_KEY if env == \"prd\" else API_STG_KEY\n",
    "    bap_url = BAP_PRD_URL if env == \"prd\" else BAP_STG_URL\n",
    "    headers = {\n",
    "        \"x-api-key\": api_key,\n",
    "    }\n",
    "    url = f\"{bap_url}/{recipe_name}/disable\"\n",
    "    res = requests.post(url, headers=headers)\n",
    "    res.raise_for_status()\n",
    "    return res\n",
    "\n",
    "\n",
    "def recipe_delete(recipe_name, env):\n",
    "    api_key = API_PRD_KEY if env == \"prd\" else API_STG_KEY\n",
    "    bap_url = BAP_PRD_URL if env == \"prd\" else BAP_STG_URL\n",
    "    headers = {\n",
    "        \"x-api-key\": api_key,\n",
    "    }    \n",
    "    url = f\"{bap_url}/{recipe_name}\"\n",
    "    res = requests.delete(url, headers=headers)\n",
    "    res.raise_for_status()\n",
    "    return res\n",
    "\n",
    "\n",
    "## Reading GCP table\n",
    "query = '''\n",
    "    SELECT pr_title\n",
    "        FROM `skt-datahub.adot_datamart.adot_tv_poc_target_drama`\n",
    "            where partition_id = 14\n",
    "                order by type\n",
    "    '''\n",
    "\n",
    "drama_name = list(bq_to_pandas(query)[\"pr_title\"])\n",
    "#drama_name = ['하지말라면더하고19', '365:운명을거스르는1년', '낭만닥터김사부2', '낭만닥터김사부', '더킹:영원의군주']\n",
    "drama_cnt = len(drama_name)\n",
    "\n",
    "\n",
    "for i in range (0, drama_cnt):\n",
    "    if drama_name[i] == '하지말라면더하고19':  drama_name[i] ='하지 말라면 더 하고 19'\n",
    "    \n",
    "recipe_names = []\n",
    "name_prefix = [\"BN_\", \"AN_\"]\n",
    "name_body = \"CROSSDOMAIN_\"\n",
    "name_postfix = []\n",
    "\n",
    "for i in range (0, drama_cnt):\n",
    "    name_postfix.append(\"DM0%d\"%(i+1)) if i < 10 else name_postfix.append(\"DM%d\"%(i+1))\n",
    "\n",
    "for prefix in name_prefix:\n",
    "    for postfix in name_postfix:\n",
    "        recipe_names.append(prefix + name_body + postfix)\n",
    "\n",
    "        \n",
    "## Recipe Generation\n",
    "def get_timestamp():\n",
    "    tz = pytz.timezone(\"UTC\")\n",
    "    now = str(datetime.datetime.now(tz).strftime(\"%Y-%m-%d %H:%M:%S.%f%z\"))\n",
    "    return now[:-2] + \":\" + now[-2:]\n",
    "\n",
    "now_timestamp = get_timestamp()\n",
    "start_date_AN = datetime.date.today().strftime(\"%Y-%m-%d\")\n",
    "end_date_AN = start_date_AN\n",
    "\n",
    "start_date_BN = (datetime.date.today()+datetime.timedelta(days=1)).strftime(\"%Y-%m-%d\")\n",
    "end_date_BN = start_date_BN\n",
    "\n",
    "\n",
    "def generating_recipe(drama_name, prefix, postfix, environment):\n",
    "    start_date = start_date_BN if prefix == \"BN_\" else start_date_AN\n",
    "    end_date = end_date_BN if prefix == \"BN_\" else end_date_AN\n",
    "    cron = '30 8 * * *' if prefix == \"BN_\" else '30 17 * * *'\n",
    "    banoon = \", 오전\" if prefix == \"BN_\" else \", 오후\"\n",
    "                \n",
    "    recipe = {}\n",
    "    recipe[\"uid\"] = str(uuid.uuid4())\n",
    "    recipe[\"recipe_name\"] = prefix + name_body + postfix\n",
    "    recipe[\"recipe_type\"] = \"schedule\"\n",
    "    recipe[\"description\"] = \"교차추천, 드라마>OST, \" + drama_name + banoon\n",
    "    recipe[\"domain\"] = \"Entertainment_Music\"\n",
    "    recipe[\"start_date\"] = start_date\n",
    "    recipe[\"end_date\"] = end_date\n",
    "    recipe[\"skip_config\"] = {}\n",
    "    recipe[\"skip_config\"][\"period\"] = 86400\n",
    "    recipe[\"skip_config\"][\"count\"] = 0\n",
    "    recipe[\"enabled\"] = True\n",
    "    recipe[\"target_table\"] = \"bap-target-\" + prefix[:2] + \"-\" + name_body + postfix\n",
    "    recipe[\"condition\"] = {}\n",
    "    recipe[\"condition\"][\"type\"] = \"condition\"\n",
    "    recipe[\"condition\"][\"params\"] = {}\n",
    "    recipe[\"condition\"][\"params\"][\"min\"] = 15\n",
    "    recipe[\"condition\"][\"params\"][\"max\"] = 99\n",
    "    recipe[\"condition\"][\"condition_name\"] = \"AGE\"\n",
    "    recipe[\"action\"] = {}\n",
    "    recipe[\"action\"][\"notification\"] = {}\n",
    "    recipe[\"action\"][\"notification\"][\"title_t_text\"] = {}\n",
    "    recipe[\"action\"][\"notification\"][\"title_t_text\"][\"formal\"] = drama_name + \" 재밌게 본 당신!\"\n",
    "    recipe[\"action\"][\"notification\"][\"title_t_text\"][\"informal\"] = drama_name + \", 재밌게 봤었지?\"\n",
    "    recipe[\"action\"][\"notification\"][\"message_t_text\"] = {}\n",
    "    recipe[\"action\"][\"notification\"][\"message_t_text\"][\"formal\"] = \"OST 다시 들어보실래요?\"\n",
    "    recipe[\"action\"][\"notification\"][\"message_t_text\"][\"informal\"] = \"OST 다시 들어볼래?\"\n",
    "    recipe[\"action\"][\"notification\"][\"action\"] = {}\n",
    "    recipe[\"action\"][\"notification\"][\"action\"][\"type\"] = \"EVENT\"\n",
    "    recipe[\"action\"][\"notification\"][\"action\"][\"text\"] = \"드라마 \" + drama_name + \" OST 들려줘\"\n",
    "    recipe[\"action\"][\"notification\"][\"action\"][\"play_service_id\"] = \"apollo.builtin.music\"\n",
    "    recipe[\"action\"][\"notification\"][\"type\"] = \"os\"\n",
    "    recipe[\"action\"][\"notification\"][\"save\"] = True\n",
    "    recipe[\"action\"][\"notification\"][\"separated\"] = True\n",
    "    recipe[\"action\"][\"tts_trigger\"] = None\n",
    "    recipe[\"action\"][\"templates\"] = None\n",
    "    recipe[\"tags\"] = ['context', 'cross_domain']\n",
    "    recipe[\"console_metadata\"] = {}\n",
    "    recipe[\"console_metadata\"][\"actionEditMode\"] = \"form\"\n",
    "    recipe[\"created_at\"] = now_timestamp\n",
    "    recipe[\"updated_at\"] = now_timestamp\n",
    "    recipe[\"schedule\"] = {}\n",
    "    recipe[\"schedule\"][\"cron\"] = cron\n",
    "    \n",
    "    return(recipe)\n",
    "\n",
    "\n",
    "# Wrting Recipes to Sheet\n",
    "credentials = _get_google_credentials()\n",
    "authorized_http = google_auth_httplib2.AuthorizedHttp(credentials, http=_get_proxy())\n",
    "service = discovery.build('sheets', 'v4', http=authorized_http, cache_discovery=False)\n",
    "\n",
    "def write_range(range_name, values):\n",
    "    body = {\n",
    "        'values': values\n",
    "    }\n",
    "    result = service.spreadsheets().values().update(\n",
    "        spreadsheetId=sheet_id, range=range_name,\n",
    "        valueInputOption='USER_ENTERED', body=body).execute()\n",
    "    \n",
    "def writing_to_sheet(recipe, tap_name, index):\n",
    "    content = [[recipe[\"enabled\"]], [recipe[\"recipe_name\"]], [recipe[\"description\"]], [str(recipe[\"tags\"])], [recipe[\"domain\"]],\n",
    "               [recipe[\"start_date\"]], [recipe[\"end_date\"]], [recipe[\"schedule\"][\"cron\"]], [recipe[\"skip_config\"][\"period\"]],\n",
    "               [recipe[\"skip_config\"][\"count\"]], [recipe[\"target_table\"]], [\"NA\"],\n",
    "               [recipe[\"condition\"][\"params\"][\"min\"]], [recipe[\"condition\"][\"params\"][\"max\"]],\n",
    "               [recipe[\"action\"][\"notification\"][\"save\"]], [recipe[\"action\"][\"notification\"][\"separated\"]],\n",
    "               [recipe[\"action\"][\"notification\"][\"type\"]],\n",
    "               [recipe[\"action\"][\"notification\"][\"title_t_text\"][\"formal\"]],\n",
    "               [recipe[\"action\"][\"notification\"][\"message_t_text\"][\"formal\"]],\n",
    "               [recipe[\"action\"][\"notification\"][\"title_t_text\"][\"informal\"]],\n",
    "               [recipe[\"action\"][\"notification\"][\"message_t_text\"][\"informal\"]],\n",
    "               [recipe[\"action\"][\"notification\"][\"action\"][\"type\"]],\n",
    "               [recipe[\"action\"][\"notification\"][\"action\"][\"text\"]], [recipe[\"action\"][\"notification\"][\"action\"][\"play_service_id\"]],\n",
    "               [str(recipe[\"action\"][\"tts_trigger\"])]]\n",
    "    \n",
    "    pin = int((index-20)/26)\n",
    "    SINGLE_LETTER_COLUMN = '%s!%s2:%s%d'%(tap_name, ALPHABET[6+index], ALPHABET[6+index], len(content)+2)\n",
    "    DOUBLE_LETTER_COLUMN = '%s!%s2:%s%d'%(tap_name, ALPHABET[pin]+ALPHABET[index-pin*26-20], ALPHABET[pin]+ALPHABET[index-pin*26-20], len(content)+2)\n",
    "    range_name = SINGLE_LETTER_COLUMN if index < 20 else DOUBLE_LETTER_COLUMN\n",
    "\n",
    "    write_range(range_name, content)\n",
    "    print(\"Writing %s finished\"%recipe[\"recipe_name\"])\n",
    "\n",
    "\n",
    "# Recipe writing to Google Sheet\n",
    "iteration = 0\n",
    "for prefix in name_prefix:\n",
    "    for i in range (0, len(name_postfix)):\n",
    "        drama = drama_name[i]\n",
    "        postfix = name_postfix[i]\n",
    "        recipe_name = prefix+name_body+postfix\n",
    "        recipe = generating_recipe(drama, prefix, postfix, env)\n",
    "        \n",
    "        flag = True if recipe[\"enabled\"] in [True, \"true\", 1] else False\n",
    "\n",
    "        writing_to_sheet(recipe, tap_name, iteration)\n",
    "        iteration += 1\n",
    "\n",
    "\n",
    "# delete recipes reigh-side\n",
    "blank = []\n",
    "for i in range (0, 25): blank.append([\"\"])\n",
    "for index in range (iteration, iteration + 5): \n",
    "    pin = int((index-20)/26)\n",
    "    if index < 20 :\n",
    "        range_name = '%s!%s2:%s%d'%(tap_name, ALPHABET[6+index], ALPHABET[6+index], len(blank)+1)\n",
    "    elif index >= 20:\n",
    "        range_name = '%s!%s2:%s%d'%(tap_name, ALPHABET[pin]+ALPHABET[index-pin*26-20], ALPHABET[pin]+ALPHABET[index-pin*26-20], len(blank)+1)\n",
    "    write_range(range_name, blank)        \n",
    "\n",
    "\n",
    "## Upload Recipes to BAP \n",
    "for prefix in name_prefix:\n",
    "    for i in range (0, len(name_postfix)):\n",
    "        drama = drama_name[i]\n",
    "        postfix = name_postfix[i]\n",
    "        recipe_name = prefix+name_body+postfix\n",
    "        recipe = generating_recipe(drama, prefix, postfix, env)\n",
    "        flag = True if recipe[\"enabled\"] in [True, \"true\", 1] else False\n",
    "\n",
    "        present_recipe = get_recipe(recipe_name, env)\n",
    "\n",
    "        if present_recipe:\n",
    "            print(f\"Recipe Name: %s already exists\"%recipe_name)\n",
    "            print(f\"Updating {recipe_name}...\")\n",
    "\n",
    "            del recipe[\"uid\"]\n",
    "            del recipe[\"recipe_type\"]\n",
    "            del recipe[\"enabled\"]\n",
    "            del recipe[\"created_at\"]\n",
    "            del recipe[\"updated_at\"]\n",
    "\n",
    "            res = update_recipe(recipe_name, recipe, env)\n",
    "            recipe_enable(recipe_name, env) if flag == True else recipe_disable(recipe_name, env)\n",
    "            print(\"updated\")\n",
    "\n",
    "        else:\n",
    "            print(f\"Creating {recipe_name}...\")\n",
    "            create_recipe(recipe, env)\n",
    "            print(\"created\")\n"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.12"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
=======
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
>>>>>>> dd6ace3e14b9687cb8c8bb7b38f494211ba21061