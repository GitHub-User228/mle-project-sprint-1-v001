import yaml
from typing import Dict
from pathlib import Path
from ensure import ensure_annotations


@ensure_annotations
def read_yaml(path: Path) -> Dict:
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
            If there are missing environment variables
            or the file is not a YAML file
        FileNotFoundError:
            If the file is not found.
        yaml.YAMLError:
            If there is an error parsing the yaml file.
        TemplateError:
            If there is an error rendering the template.
    """
    if path.suffix not in [".yaml", ".yml"]:
        raise ValueError(f"The file {path} is not a YAML file")
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
