import os
from pathlib import Path
from typing import Literal

import json
import yaml
import joblib
from dotenv import load_dotenv
from ensure import ensure_annotations
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.base import Engine


@ensure_annotations
def read_yaml(path: Path) -> dict:
    """
    Reads a yaml file, and returns a dict.

    Args:
        path_to_yaml (Path):
            Path to the yaml file

    Returns:
        Dict:
            The yaml content as a dict.

    Raises:
        ValueError:
            If the file is not a YAML file
        FileNotFoundError:
            If the file is not found.
        yaml.YAMLError:
            If there is an error parsing the yaml file.
        Exception:
            If an unexpected error occurs while reading YAML file.
    """
    if path.suffix not in [".yaml", ".yml"]:
        raise ValueError(f"The file {path} is not a YAML file")

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise Exception(
            f"An error occurred while creating the directory: {e}"
        ) from e

    try:
        with open(path, "r") as file:
            content = yaml.safe_load(file)
        return content
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found: {e}")
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing YAML file: {e}")
    except Exception as e:
        raise Exception(
            f"An unexpected error occurred while reading YAML file: {e}"
        )


def create_connection(
    db_name: Literal["SOURCE", "DESTINATION"] = "DESTINATION"
) -> Engine:
    """
    Creates a SQLAlchemy engine connection to the specified database.

    Args:
        db_name (Literal["SOURCE", "DESTINATION"]):
             The name of the database to connect to.
             Defaults to "DESTINATION".

    Returns:
        Engine:
            A SQLAlchemy engine object representing the database
            connection.

    Raises:
        ValueError:
            If the specified database name is not "SOURCE" or
            "DESTINATION".
        FileNotFoundError:
            If the .env file is not found.
        Exception:
            If an unexpected error occurs while creating the database connection.
    """

    try:
        load_dotenv()
    except FileNotFoundError as e:
        raise FileNotFoundError(".env file not found") from e

    host = os.environ.get(f"DB_{db_name}_HOST")
    port = os.environ.get(f"DB_{db_name}_PORT")
    db = os.environ.get(f"DB_{db_name}_NAME")
    username = os.environ.get(f"DB_{db_name}_USER")
    password = os.environ.get(f"DB_{db_name}_PASSWORD")

    if not all([host, port, db, username, password]):
        raise ValueError(
            f"Missing environment variables for {db_name} database connection"
        )

    try:
        print(f"postgresql://{username}:{password}@{host}:{port}/{db}")
        conn = create_engine(
            f"postgresql://{username}:{password}@{host}:{port}/{db}",
            connect_args={"sslmode": "require"},
        )
        return conn
    except SQLAlchemyError as e:
        raise Exception(
            "An error occurred while creating the database connection"
        ) from e


@ensure_annotations
def save_pkl(model: object, path: Path):
    """
    Saves a model object to a file using joblib.

    Args:
        model (object):
            The model object to be saved.
        path (Path):
            The path to the file where the model will be saved.

    Raises:
        ValueError:
            If the file does not have a .pkl extension.
        FileNotFoundError:
            If the directory to save the model does not exist.
        IOError:
            If an I/O error occurs during the saving process.
        Exception:
            If an unexpected error occurs during the saving process.
    """

    if path.suffix != ".pkl":
        raise ValueError(f"The file {path} is not a pkl file")

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise Exception(
            f"An error occurred while creating the directory: {e}"
        ) from e

    try:
        with open(path, "wb") as f:
            joblib.dump(model, f)
        print(f"Model file saved at: {path}")
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Directory '{path.parent}' does not exist: {e}"
        ) from e
    except IOError as e:
        raise IOError(
            f"An I/O error occurred while saving the model: {e}"
        ) from e
    except Exception as e:
        raise Exception(
            f"An unexpected error occurred while saving the model: {e}"
        ) from e


@ensure_annotations
def read_pkl(path: Path) -> object:
    """
    Reads a model object from a file using joblib.

    Args:
        path (Path):
            The path to the file with the model to load.

    Returns:
        object:
            The loaded model object.

    Raises:
        ValueError:
            If the file does not have a .pkl extension.
        FileNotFoundError:
            If the file does not exist.
        IOError:
            If an I/O error occurs during the loading process.
        Exception:
            If an unexpected error occurs while loading the model.
    """

    if path.suffix != ".pkl":
        raise ValueError(f"The file {path} is not a pkl file")

    try:
        with open(path, "rb") as f:
            model = joblib.load(f)
        return model
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File '{path}' does not exist: {e}") from e
    except IOError as e:
        raise IOError(
            f"An I/O error occurred while loading the model: {e}"
        ) from e
    except Exception as e:
        raise Exception(
            f"An unexpected error occurred while loading the model: {e}"
        ) from e


@ensure_annotations
def save_dict_as_json(data: dict, path: Path):
    """
    Saves a dictionary object to a file in JSON format.

    Args:
        data (dict):
            The dictionary object to be saved.
        path (Path):
            The path to the file where the dictionary will be saved.

    Raises:
        FileNotFoundError:
            If the directory to save the dictionary does not exist.
        IOError:
            If an I/O error occurs during the saving process.
        Exception:
            If an unexpected error occurs during the saving process.
    """

    if path.suffix != ".json":
        raise ValueError(f"The file {path} is not a json file")

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise Exception(
            f"An error occurred while creating the directory {path.parent}: {e}"
        ) from e

    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=4)
        print(f"JSON file saved at: {path}")
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Directory '{path.parent}' does not exist: {e}"
        ) from e
    except IOError as e:
        raise IOError(
            f"An I/O error occurred while saving the JSON file: {e}"
        ) from e
    except Exception as e:
        raise Exception(
            f"An unexpected error occurred while saving the JSON file: {e}"
        ) from e
