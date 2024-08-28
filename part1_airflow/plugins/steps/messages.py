import requests
from pathlib import Path
from airflow.providers.telegram.hooks.telegram import TelegramHook

from utils import read_yaml

config = read_yaml(Path("./config/tg_callback.yaml"))


def send_telegram_success_message(
    context: dict,
) -> None:
    """
    Sends a Telegram message to notify that a DAG has been successfully
    executed.

    Args:
        context (dict):
            The context dictionary containing information about the
            DAG execution.
    """

    message = (
        f"DAG {context['dag']} with run_id {context['run_id']} has been "
        f"successfully executed."
    )
    requests.post(
        url=f"https://api.telegram.org/bot{config['token']}/sendMessage",
        data={"chat_id": config["chat_id"], "text": message},
    )
    # hook = TelegramHook(
    #     # telegram_conn_id="telegram_callback",
    #     token=tg_callback_config["token"],
    #     chat_id=tg_callback_config["chat_id"],
    # )
    # hook.send_message(
    #     {"chat_id": tg_callback_config["chat_id"], "text": message}
    # )


def send_telegram_failure_message(context: dict) -> None:
    """
    Sends a Telegram message to notify that a DAG execution has failed.

    Args:
        context (dict):
            The context dictionary containing information about the
            DAG execution.
    """

    message = (
        f"DAG {context['dag']} execution with run_id {context['run_id']} "
        f"has failed: {context['task_instance_key_str']}"
    )
    requests.post(
        url=f"https://api.telegram.org/bot{config['token']}/sendMessage",
        data={"chat_id": config["chat_id"], "text": message},
    )
    # hook = TelegramHook(
    #     # telegram_conn_id="telegram_callback",
    #     token=tg_callback_config["token"],
    #     chat_id=tg_callback_config["chat_id"],
    # )
    # hook.send_message(
    #     {"chat_id": tg_callback_config["chat_id"], "text": message}
    # )
