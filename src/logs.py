import logging
import os
import json
import datetime

from models import Config

logger = logging.getLogger(__name__)


def save_log_file(changes_by_brand) -> None:
    dry_result = []

    for brand_changes in changes_by_brand.values():
        for change in brand_changes:
            dry_result.append(change)

    if len(dry_result) == 0:
        logger.ingo("There is no changes in this run. No logse saved.")

    save_path = (
        f"./logs/{Config.brand()}/{datetime.datetime.now().strftime('%Y-%m-%d')}.json"
    )

    # If the save directory doesn't exist, create it
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    with open(save_path, "w") as file:
        file.write(json.dumps(dry_result, indent=4, ensure_ascii=False))

    logger.info(f"Logs for the run saved into {save_path}")
    return save_path
