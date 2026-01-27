import osmapi
import os
import logging

from oauthcli import OpenStreetMapAuth
from models import Config
from osmapi.errors import ApiError
from utils import timer

logger = logging.getLogger(__name__)


class BulkUpload:
    """
    Bulk uploads a changeset to the OSM server.
    Batch by departement and brand wikidata
    """

    @timer
    def __init__(self, all_brands_changes):
        self.all_brands_changes = all_brands_changes
        auth = OpenStreetMapAuth(
            client_id=os.getenv("OSM_OAUTH_CLIENT_ID"),
            client_secret=os.getenv("OSM_OAUTH_CLIENT_SECRET"),
            scopes=["write_api"],
            url=os.getenv("OSM_API_HOST"),
        ).auth_code()
        self.api = osmapi.OsmApi(
            api=os.getenv("OSM_API_HOST"),
            session=auth.session,
        )

        for brand in all_brands_changes:
            self.upload_brand(all_brands_changes[brand], brand)

    def upload_brand(self, brand_changes: list, brand: str):
        with self.api.Changeset(
            {
                "comment": f"Importation des données ATP ({Config.departement_number()}; {brand})",
                "created_by": "atp2osm-import",
                "source": "https://alltheplaces.xyz",
                "bot": "yes",
            }
        ) as changeset:
            logger.info(
                f"https://master.apis.dev.openstreetmap.org/changeset/{changeset}"
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
