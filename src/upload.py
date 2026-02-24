import osmapi
import os
import logging
import json
import datetime

from pathlib import Path
from requests_oauthlib import OAuth2Session
from osmapi.errors import ApiError

from utils import timer
from models import Config

logger = logging.getLogger(__name__)


class BulkUpload:
    """
    Bulk uploads a changeset to the OSM server.
    Batch by departement and brand wikidata
    """

    @timer
    def __init__(self, changes: list, session: OAuth2Session):
        self.changes = changes
        self.brand_name = changes[0]["tag"]["brand"]
        self.brand_wikidata = changes[0]["tag"]["brand:wikidata"]
        self.changesets = []

        self.api = osmapi.OsmApi(
            api=os.getenv("OSM_API_HOST"),
            session=session,
        )

    def save_log_file(self) -> Path:
        if len(self.changes) == 0:
            logger.ingo("There is no changes in this run. No logse saved.")

        save_path = Path(
            f"./logs/{self.brand_wikidata}/{datetime.datetime.now().strftime('%Y-%m-%d')}.json"
        )

        # If the save directory doesn't exist, create it
        os.makedirs(save_path.parent, exist_ok=True)

        with open(save_path, "w") as file:
            file.write(json.dumps(self.changes, indent=4, ensure_ascii=False))

        logger.debug(f"Logs for the run saved into {save_path}")
        return save_path

    def upload_one_dpt(self):
        if len(self.changes) == 0:
            return

        brand_name = self.changes[0]["tag"]["brand"]
        with self.api.Changeset(
            {
                "comment": f"Importation des données ATP (dép. {Config.departement_number()}; {brand_name})",
                "created_by": "atp2osm-import",
                "source": "https://alltheplaces.xyz",
                "wiki": "https://wiki.openstreetmap.org/wiki/Automated_edits/atp2osm_bot",
                "bot": "yes",
            }
        ) as changeset:
            logger.debug(
                f"{os.getenv('OSM_API_HOST').rstrip('/')}/changeset/{changeset}"
            )
            changingNodes = []
            changingRelations = []
            for poi in self.changes:
                poi["changeset"] = changeset

                if poi["node_type"] == "node":
                    changingNodes.append(poi)

                if poi["node_type"] == "relation":
                    changingRelations.append(poi)
            try:
                self.api.ChangesetUpload(
                    [
                        {"type": "node", "action": "modify", "data": changingNodes},
                        {
                            "type": "relation",
                            "action": "modify",
                            "data": changingRelations,
                        },
                    ]
                )

                # Add to changeset list to save it in logs
                self.changesets.append(changeset)
            except ApiError as error:
                logger.error(f"OSM API error for changeset upload: {error.status}")
