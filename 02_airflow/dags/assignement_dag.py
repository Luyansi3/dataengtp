import pendulum
from datetime import timedelta
import requests
import pandas as pd
import logging
import json
import random
from faker import Faker

from airflow import DAG
from airflow.operators.bash import BashOperator

from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger= logging.getLogger(__name__)

START_DATE = pendulum.datetime(2020, 6, 25, tz="UTC")
prev_ep = "1588612377" #"{{ prev_data_interval_start_success.int_timestamp if prev_data_interval_start_success else data_interval_start.int_timestamp }}"

with DAG(
    dag_id="dnd_dag",
    start_date=START_DATE,
    schedule="0 0 * * *",          # daily at 00:00 UTC
    catchup=False,
    max_active_tasks=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    template_searchpath=["/opt/airflow/data/"],  # for SQL files
    tags=["example"],
) as dag:
    
    def _fetch(epoch:str, output_folder: str, filename:str, url:str, endpoint:str):
        logger.info("test et tout")
        response = requests.get(f"{url}/{endpoint}")
        json_res = response.json()
        logger.info(json_res)
        random_result = random.sample(json_res.get("results", []), 5)
        logger.info(f"{output_folder}/{epoch}_{filename}.json")
        with open(f"{output_folder}/{epoch}_{filename}.json", "w") as f:
            json.dump(random_result, f, ensure_ascii=False)

    def _gen_name(epoch:str, output_folder: str, filename:str):
        fake = Faker()
        names = [fake.name() for _ in range(5)]
        with open(f"{output_folder}/{epoch}_{filename}.json", "w") as f:
            json.dump(names, f)

    def _gen_attributes(epoch:str, output_folder: str, filename:str):
        attributes = [[random.randint(6,18) for _ in range(6)] for _ in range(5)]
        with open(f"{output_folder}/{epoch}_{filename}.json", "w") as f:
            json.dump(attributes, f)

    def _gen_levels(epoch:str, output_folder: str, filename:str):
        levels = [random.randint(1,3) for _ in range(5)]
        with open(f"{output_folder}/{epoch}_{filename}.json", "w") as f:
            json.dump(levels, f)

    def _fetch_spells(epoch:str, output_folder: str, filename:str, classes_filenames: str, levels_filenames: str, url:str, endpoint:str):
        with open(f"{output_folder}/{epoch}_{classes_filenames}", "r") as read:
            classes = json.load(read)
        
        with open(f"{output_folder}/{epoch}_{levels_filenames}", "r") as read:
            levels = json.load(read)


        spells = []

        for i in range(len(classes)):
            response = requests.get(f"{url}/classes/{classes[i].get("index", "")}/{endpoint}")
            curr_level = levels[i]
            json_res = response.json()
            spells_fetch = json_res.get("results", [])
            if response.status_code != 401 and spells_fetch != []:
                k=0
                spells_random=[]
                while k != (curr_level+3):
                    random_spell = spells_fetch[random.randint(0, len(spells_fetch)-1)]
                    if random_spell["level"] > 2 :
                        k -=1
                    else:
                        spells_random.append(random_spell["index"])
                    k += 1
                logger.info(f"for {classes[i]} selected {spells_random}")
            else:
                spells_random= []
            spells.append(spells_random)

        with open(f"{output_folder}/{epoch}_{filename}.json", "w") as f:
            json.dump(spells, f)

    def _fetch_proficiencies(epoch:str, output_folder: str, filename:str, classes_filenames: str, url:str, endpoint:str):
        with open(f"{output_folder}/{epoch}_{classes_filenames}", "r") as read:
            classes = json.load(read)
        
        proficiencies = []

        for i in range(len(classes)):
            response = requests.get(f"{url}/classes/{classes[i].get("index", "")}/{endpoint}")
            json_res = response.json()
            proficiencies_fetch = json_res.get("results", [])
            random_proficiencies = proficiencies_fetch[random.randint(0, len(proficiencies_fetch)-1)]
        
            proficiencies.append(random_proficiencies)

        with open(f"{output_folder}/{epoch}_{filename}.json", "w") as f:
            json.dump(proficiencies, f)

    def _json_to_csv(epoch:str, output_folder: str, filename:str, classes_filenames: str,
                      levels_filenames: str,
                      attributes_filenames: str,
                      languages_filenames: str,
                      names_filenames: str,
                      races_filenames: str,
                      proficiencies_filenames: str,
                      spells_filenames:str):
         
        with open(f"{output_folder}/{epoch}_{classes_filenames}", "r") as read:
            classes = json.load(read)
        with open(f"{output_folder}/{epoch}_{levels_filenames}", "r") as read:
            levels = json.load(read)
        with open(f"{output_folder}/{epoch}_{attributes_filenames}", "r") as read:
            attributes = json.load(read)
        with open(f"{output_folder}/{epoch}_{languages_filenames}", "r") as read:
            languages = json.load(read)
        with open(f"{output_folder}/{epoch}_{names_filenames}", "r") as read:
            names = json.load(read)
        with open(f"{output_folder}/{epoch}_{races_filenames}", "r") as read:
            races = json.load(read)
        with open(f"{output_folder}/{epoch}_{proficiencies_filenames}", "r") as read:
            proficiencies = json.load(read)
        with open(f"{output_folder}/{epoch}_{spells_filenames}", "r") as read:
            spells = json.load(read)

        rows = []
        for i in range(5):  # all lists have exactly 5 elements
            row = {
                "classes": classes[i]["index"],
                "levels": levels[i],
                "attributes": json.dumps(attributes[i]),
                "languages": languages[i]["index"],
                "names": names[i],
                "races": races[i]["index"],
                "proficiencies": proficiencies[i]["index"],
                "spells": str(spells[i])
            }
            rows.append(row)
                
        df = pd.DataFrame(rows)
        df.to_csv(f"{output_folder}/{epoch}_{filename}.csv", index=False)

    def _create_query(epoch:str, output_folder: str, filename:str, csv_filename:str):
        df = pd.read_csv(f"{output_folder}/{epoch}_{csv_filename}")
        with open(f"{output_folder}/{filename}.sql", "w") as f:
            f.write(
                "CREATE TABLE IF NOT EXISTS characters (\n"
                "  classe TEXT,\n"
                "  level TEXT,\n"
                "  attributes TEXT,\n"
                "  language TEXT,\n"
                "  name TEXT,\n"
                "  race TEXT,\n"
                "  proficiencie TEXT,\n"
                "  spells TEXT\n"
                ");\n"
            )
            for _, row in df.iterrows():
                classe = row.get("classes", "")
                level = row.get("levels", "")
                attribute = row.get("attributes", "")
                language = row.get("languages", "")
                name = row.get("names", "")
                race = row.get("races", "")
                proficiencie = row.get("proficiencies", "")
                spell = row.get("spells", "")
                f.write(
                    "INSERT INTO characters VALUES ("
                    f"'{classe}', '{level}', $${attribute}$$, '{language}', '{name}', '{race}', '{proficiencie}', $${spell}$$"
                    ");\n"
                )

    get_classes = PythonOperator(
        task_id="get_classes",
        python_callable=_fetch,
        op_kwargs={
            "epoch": "{{ data_interval_start.int_timestamp }}",
            "output_folder": "/opt/airflow/data",
            "filename": "classes",
            "url": "https://www.dnd5eapi.co/api/2014",
            "endpoint": "classes"
        },
    )

    get_races = PythonOperator(
        task_id="get_races",
        python_callable=_fetch,
        op_kwargs={
            "epoch": "{{ data_interval_start.int_timestamp }}",
            "output_folder": "/opt/airflow/data",
            "filename": "races",
            "url": "https://www.dnd5eapi.co/api/2014",
            "endpoint": "races"
        },
    )

    get_languages = PythonOperator(
        task_id="get_languages",
        python_callable=_fetch,
        op_kwargs={
            "epoch": "{{ data_interval_start.int_timestamp }}",
            "output_folder": "/opt/airflow/data",
            "filename": "languages",
            "url": "https://www.dnd5eapi.co/api/2014",
            "endpoint": "languages"
        },
    )

    get_proficiencies = PythonOperator(
        task_id="get_proficiencies",
        python_callable=_fetch_proficiencies,
        op_kwargs={
            "epoch": "{{ data_interval_start.int_timestamp }}",
            "output_folder": "/opt/airflow/data",
            "filename": "proficiencies",
            "classes_filenames": "classes.json",
            "url": "https://www.dnd5eapi.co/api/2014",
            "endpoint": "proficiencies"
        },
    )

    get_names = PythonOperator(
        task_id="get_names",
        python_callable=_gen_name,
        op_kwargs={
            "epoch": "{{ data_interval_start.int_timestamp }}",
            "output_folder": "/opt/airflow/data",
            "filename": "names",
        },
    )

    get_attributes = PythonOperator(
        task_id="get_attributes",
        python_callable=_gen_attributes,
        op_kwargs={
            "epoch": "{{ data_interval_start.int_timestamp }}",
            "output_folder": "/opt/airflow/data",
            "filename": "attributes",
        },
    )

    get_levels = PythonOperator(
        task_id="get_levels",
        python_callable=_gen_levels,
        op_kwargs={
            "epoch": "{{ data_interval_start.int_timestamp }}",
            "output_folder": "/opt/airflow/data",
            "filename": "levels",
        },
    )

    get_spells = PythonOperator(
        task_id="get_spells",
        python_callable=_fetch_spells,
        op_kwargs={
            "epoch": "{{ data_interval_start.int_timestamp }}",
            "output_folder": "/opt/airflow/data",
            "filename": "spells",
            "classes_filenames": "classes.json",
            "levels_filenames":"levels.json",
            "url": "https://www.dnd5eapi.co/api/2014",
            "endpoint": "spells"
        },
    )

    json_to_csv = PythonOperator(
        task_id="json_to_csv",
        python_callable=_json_to_csv,
        op_kwargs={
            "epoch": "{{ data_interval_start.int_timestamp }}",
            "output_folder": "/opt/airflow/data",
            "filename": "correct_filtered",
            "classes_filenames": "classes.json",
            "levels_filenames":"levels.json",
            "attributes_filenames":"attributes.json",
            "languages_filenames":"languages.json",
            "names_filenames":"names.json",
            "races_filenames":"races.json",
            "proficiencies_filenames":"proficiencies.json",
            "spells_filenames":"spells.json",
        },
    )



    create_query = PythonOperator(
        task_id="create_query",
        python_callable=_create_query,
        op_kwargs={
            "epoch": "{{ data_interval_start.int_timestamp }}",
            "output_folder": "/opt/airflow/data",
            "filename": "insert_queries",
            "csv_filename": "correct_filtered.csv"
        },
    )


    insert_query = SQLExecuteQueryOperator(
        task_id="insert_query",
        conn_id="postgres_default",   # same connection as before; change if needed
        sql="insert_queries.sql",
        autocommit=True,
    )
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed",
    )




[get_names, get_classes, get_attributes, get_races, get_languages, get_levels] >> get_spells >> get_proficiencies >> json_to_csv >> create_query >> insert_query >> end