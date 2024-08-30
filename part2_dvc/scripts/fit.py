from pathlib import Path

import pandas as pd
from sklearn.pipeline import Pipeline
from catboost import CatBoostRegressor
from category_encoders import CatBoostEncoder
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder

from utils import read_yaml, save_pkl


def fit_model() -> None:
    """
    Fits a pipeline with a machine learning model and preprocessors
    using the data from the "data/initial_data.csv" file
    and the hyperparameters defined in the "params.yaml" file.

    The fitted pipeline is then saved.
    """

    # 1. Reading hyperparams
    params = read_yaml(path=Path("params.yaml"))

    # 2. Reading data
    data = pd.read_csv(Path("data/initial_data.csv"))

    # 3. Main part with pipeline fitting
    numerical_features = data.select_dtypes(
        include=["float64", "int64"]
    ).columns.to_list()
    binary_cat_features = data.columns[data.nunique() == 2].to_list()
    non_binary_cat_features = ["building_type_int"]

    for col in [
        "building_id",
        "building_type_int",
        params["target_col"],
    ]:
        numerical_features.remove(col)

    preprocessor = ColumnTransformer(
        [
            (
                "binary",
                OneHotEncoder(drop=params["one_hot_drop"]),
                binary_cat_features,
            ),
            ("num", "passthrough", numerical_features),
            (
                "non-binary",
                CatBoostEncoder(cols=non_binary_cat_features),
                non_binary_cat_features,
            ),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )

    model = CatBoostRegressor(
        verbose=params["verbose_catboost"],
        random_state=params["random_state_catboost"],
    )

    pipeline = Pipeline([("preprocessor", preprocessor), ("model", model)])
    pipeline.fit(data, data[params["target_col"]])

    # 4. Saving pipeline model
    save_pkl(model=pipeline, path=Path("models/fitted_model.pkl"))


if __name__ == "__main__":
    fit_model()
