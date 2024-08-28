import numpy as np
from sklearn.metrics import make_scorer


def mean_cv_scores(cv_res: dict, ndigits: int = 4) -> dict:
    """
    Calculates the mean of the cross-validation's scores in a dictionary
    for regression task.

    Args:
        cv_res (dict):
            A dictionary containing the cross-validation scores.
        ndigits (int):
            The number of decimal places to round the mean scores to.
            Defaults to 4.

    Returns:
        dict:
            A dictionary containing the mean cross-validation scores
            for regression task.
    """

    cv_res_mean = {}

    for key, value in cv_res.items():
        cv_res_mean[key] = round(value.mean(), ndigits)
        if "neg_" in key:
            cv_res_mean[key.replace("neg_", "")] = cv_res_mean[key] * -1
            del cv_res_mean[key]

    return cv_res_mean


def mae_score(y_true, y_pred):
    return np.mean(np.abs(10**y_true - 10**y_pred))


def rmse_score(y_true, y_pred):
    return np.sqrt(np.mean((10**y_true - 10**y_pred) ** 2))


def mape_score(y_true, y_pred):
    return np.mean(np.abs((10**y_true - 10**y_pred) / 10**y_true))


def r2_score(y_true, y_pred):
    return 1 - np.sum((10**y_true - 10**y_pred) ** 2) / np.sum(
        (10**y_true - np.mean(10**y_true)) ** 2
    )


neg_mae = make_scorer(mae_score, greater_is_better=False)
neg_rmse = make_scorer(rmse_score, greater_is_better=False)
neg_mape = make_scorer(mape_score, greater_is_better=False)
r2 = make_scorer(r2_score, greater_is_better=True)
