import osmapi
import os
import logging

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
        self.api = osmapi.OsmApi(
            api=os.getenv("OSM_API_HOST"),
            session=session,
        )

        self.upload_brand(changes)

    def upload_brand(self, brand_changes: list):
        brand_name = brand_changes[0]["tag"]["brand"]
        with self.api.Changeset(
            {
                "comment": f"Importation des données ATP (dép. {Config.departement_number()}; {brand_name})",
                "created_by": "atp2osm-import",
                "source": "https://alltheplaces.xyz",
                "wiki": "https://wiki.openstreetmap.org/wiki/Automated_edits/atp2osm_bot",
                "bot": "yes",
            }
        ) as changeset:
            logger.info(
                f"{os.getenv('OSM_API_HOST').rstrip('/')}/changeset/{changeset}"
            )
            changingNodes = []
            changingRelations = []
            for poi in brand_changes:
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
            except ApiError as error:
                logger.error(f"OSM API error for changeset upload: {error.status}")
