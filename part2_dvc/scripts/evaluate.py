import pandas as pd
from pathlib import Path
from sklearn.model_selection import KFold, cross_validate

from metrics import *
from sklearn.metrics import *
from utils import read_yaml, read_pkl, save_dict_as_json


def evaluate_model() -> None:
    """
    Evaluates a pipeline using cross-validation.
    """

    # 1. Reading hyperparams
    params = read_yaml(path=Path("params.yaml"))

    # 2. Reading pipeline and data
    pipeline = read_pkl(path=Path("models/fitted_model.pkl"))
    data = pd.read_csv(Path("data/initial_data.csv"))

    # 3. Cross-validation
    scoring = dict(
        [
            (
                k,
                globals()[k],
            )
            for k in params["metrics"]
        ]
    )
    cv_strategy = KFold(
        n_splits=params["n_splits"],
        shuffle=True,
        random_state=params["random_state_kfold"],
    )
    cv_res = cross_validate(
        pipeline,
        data,
        data[params["target_col"]],
        cv=cv_strategy,
        n_jobs=params["n_jobs"],
        scoring=scoring,
    )
    cv_res = mean_cv_scores(cv_res=cv_res, ndigits=params["ndigits"])

    # 4. Saving cv results
    save_dict_as_json(data=cv_res, path=Path("cv_results/cv_res.json"))


if __name__ == "__main__":
    evaluate_model()
