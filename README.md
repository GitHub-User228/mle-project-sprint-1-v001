# Проект 1 спринта

Добро пожаловать в репозиторий-шаблон Практикума для проекта 1 спринта. Цель проекта — создать базовое решение для предсказания стоимости квартир Яндекс Недвижимости.

Полное описание проекта хранится в уроке «Проект. Разработка пайплайнов подготовки данных и обучения модели» на учебной платформе.

Здесь укажите имя вашего бакета: s3-student-mle-20240730-73c4e0c760

PS: Я привык оформлять оформлять код и комментарии полностью на английском языке, но если будет необходимость, то я могу сделать на русском.

## Airflow

### Project Structure

**[docker-compose.yaml](part1_airflow/docker-compose.yaml)**: This file contains the configuration for the Docker Compose for running the Airflow service.

**[Dockerfile](part1_airflow/Dockerfile)**: This Dockerfile sets up a Python with all necessary packages for all subservices.

**[requirements.txt](part1_airflow/requirements.txt)**: This file contains the list of Python packages required for the project.

**[dags](part1_airflow/dags)**: This directory contains Python code for Airflow DAGs. It includes DAGs for two stages:
- ETL for Data Preparation: [prepare_flats.py](part1_airflow/dags/prepare_flats.py)
- ETL for Data Cleaning: [clean_flats.py](part1_airflow/dags/clean_flats.py)

**[plugins](plugins)**: This directory contains Python code for Airflow plugins. It includes plugins for:
- Data Preparation: [prepare_flats.py](part1_airflow/plugins/steps/prepare_flats.py)
- Data Cleaning: [clean_flats.py](part1_airflow/plugins/steps/clean_flats.py)
- Telegram callback: [messages.py](part1_airflow/plugins/steps/messages.py)

There are also some utility functions stored in [utils.py](part1_airflow/plugins/utils.py).

**[data_cleaning.ipynb](part1_airflow/notebooks/data_cleaning.ipynb)**: This Jupyter notebooks covers data cleaning part in details.

PS: Unfortunately, the telegram callback plugin only works via `request` library, but not via `TelegramHook` for some unknown reason.


