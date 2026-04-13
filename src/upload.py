import osmapi
import os
import logging
import json
import datetime
import xml.etree.ElementTree as ET

from pathlib import Path
from requests_oauthlib import OAuth2Session
from osmapi.errors import ApiError

logger = logging.getLogger(__name__)


class BulkUpload:
    """
    Bulk uploads a changeset to the OSM server.
    Batch by departement and brand wikidata
    """

    def __init__(self, changes: list, session: OAuth2Session):
        self.changes = changes
        self.brand_name = changes[0]["atp_brand"]
        self.brand_wikidata = changes[0]["tag"].get("brand:wikidata") or "unknown"
        self.changesets = []

        self.is_dev = os.getenv("APP_ENV").upper() == "DEVELOPMENT"
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
            file.write(json.dumps(self.changesets, indent=4, ensure_ascii=False))

        logger.debug(f"Logs for the run saved into {save_path}")
        return save_path

    def upload(self) -> list[str]:
        """Upload all changes. Returns a list of error messages; empty list means full success."""
        if len(self.changes) == 0:
            return []

        changes_by_dpt = self._sorted_by_dpt()
        errors = []

        for dpt, dpt_changes in changes_by_dpt.items():
            try:
                with self.api.Changeset(
                    {
                        "comment": f"Importation des données ATP (dép. {dpt}; {self.brand_name})",
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
                    for poi in dpt_changes:
                        poi["changeset"] = changeset

                        if poi["node_type"] == "node":
                            changingNodes.append(poi)
                        elif poi["node_type"] == "way":
                            # DEV: the dev OSM instance returns 404 on node/way lookups
                            # because it does not mirror production data, so uploads are skipped.
                            # Uncomment the _write_osc call below to inspect generated OSC files.
                            if not self.is_dev:
                                self.api.WayUpdate(
                                    {
                                        "id": poi["id"],
                                        "version": poi["version"],
                                        "changeset": changeset,
                                        "tag": poi["tag"],
                                        "nd": poi["members"],
                                    }
                                )
                            # if self.is_dev:
                            #     self._write_osc(changeset, "way", poi["id"], "modify", {
                            #         "id": poi["id"], "version": poi["version"],
                            #         "changeset": changeset, "tag": poi["tag"], "nd": poi["members"],
                            #     })
                        elif poi["node_type"] == "relation":
                            type_map = {"n": "node", "w": "way", "r": "relation"}
                            relation_data = {
                                "id": poi["id"],
                                "version": poi["version"],
                                "changeset": changeset,
                                "tag": poi["tag"],
                                "member": [
                                    {
                                        "type": type_map[m["type"]],
                                        "ref": m["ref"],
                                        "role": m["role"],
                                    }
                                    for m in (poi["members"] or [])
                                ],
                            }
                            if not self.is_dev:
                                self.api.RelationUpdate(relation_data)
                            # Uncomment to inspect relation OSC output in dev:
                            # if self.is_dev:
                            #     self._write_osc(changeset, "relation", poi["id"], "modify", relation_data)

                    # DEV: the dev OSM instance returns 404 on node lookups because it does
                    # not mirror production data, so node uploads are skipped.
                    # Uncomment the _write_osc loop below to inspect generated OSC files.
                    if changingNodes and not self.is_dev:
                        self.api.ChangesetUpload(
                            [
                                {
                                    "type": "node",
                                    "action": "modify",
                                    "data": changingNodes,
                                }
                            ]
                        )
                    # if self.is_dev:
                    #     for node in changingNodes:
                    #         self._write_osc(changeset, "node", node["id"], "modify", node)

                    # Add to changeset list to save it in logs
                    self.changesets.append(changeset)
            except ApiError as error:
                msg = f"OSM API error for dept {dpt}: HTTP {error.status}"
                logger.error(msg)
                errors.append(msg)
            except Exception as unknown:
                msg = f"Unknown error for dept {dpt}: {unknown}"
                logger.error(msg)
                errors.append(msg)

        return errors

    def _write_osc(
        self,
        changeset: int,
        element_type: str,
        element_id: int,
        action: str,
        data: dict,
    ) -> None:
        osc_dir = Path("./data/atp2osm/changesets")
        osc_dir.mkdir(parents=True, exist_ok=True)
        osc_path = osc_dir / f"{changeset}_{element_type}_{element_id}.osc"

        root = ET.Element("osmChange", version="0.6")
        action_el = ET.SubElement(root, action)
        el = ET.SubElement(
            action_el,
            element_type,
            {
                "id": str(element_id),
                "version": str(data.get("version", "")),
                "changeset": str(changeset),
            },
        )

        if element_type == "node":
            if "lat" in data:
                el.set("lat", str(data["lat"]))
            if "lon" in data:
                el.set("lon", str(data["lon"]))
        elif element_type == "way":
            for ref in data.get("nd") or []:
                ET.SubElement(el, "nd", ref=str(ref))
        elif element_type == "relation":
            for member in data.get("member") or []:
                ET.SubElement(
                    el,
                    "member",
                    {
                        "type": member["type"],
                        "ref": str(member["ref"]),
                        "role": member.get("role", ""),
                    },
                )

        for k, v in (data.get("tag") or {}).items():
            ET.SubElement(el, "tag", k=k, v=str(v))

        tree = ET.ElementTree(root)
        ET.indent(tree, space="  ")
        with open(osc_path, "wb") as f:
            tree.write(f, encoding="utf-8", xml_declaration=True)

        logger.debug(f"DEV: OSC written to {osc_path}")

    def _sorted_by_dpt(self):
        sorted_changes = {}
        for change in self.changes:
            dpt = change["departement_number"]
            if dpt in sorted_changes:
                sorted_changes[dpt].append(change)
            else:
                sorted_changes[dpt] = [change]

        return sorted_changes
