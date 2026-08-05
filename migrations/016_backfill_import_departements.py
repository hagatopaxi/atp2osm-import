"""Reprise de l'existant : reconstruit import_departements depuis les logs.

Chaque intégration a laissé un fichier `logs/<brand_wikidata>/<date>.json`
contenant deux tableaux JSON concaténés :

  1. les POIs soumis — avec leur département, leurs tags avant/après, et la
     clé `changeset` posée au moment de l'envoi (donc y compris pour un
     département dont l'envoi a ensuite échoué) ;
  2. les identifiants des changesets *réussis*.

Le croisement des deux donne, département par département, l'effectif, le
changeset tenté et son sort — soit exactement une ligne import_departements.
Les tags avant/après donnent au passage tags_count, et le compte des POIs
réellement envoyés donne items_count.

Les cas success, partial_* et error_* sont tous couverts :
  - un département dont le changeset figure dans le tableau 2 → success ;
  - sinon → error_osm_api / error_unknown, suffixe pris sur le statut de
    l'intégration (les deux niveaux ne peuvent pas diverger : le suffixe
    vient déjà des mêmes erreurs).

Quelques logs n'ont pas gardé la clé `changeset` sur les POIs : les
identifiants réussis ayant été notés dans l'ordre des départements, ils leur
sont alors redonnés dans ce même ordre.

Ne sont pas repris : les abandons (`cancelled`, aucun changeset tenté), les
intégrations vides, celles dont le fichier de log manque, et les tout premiers
logs, antérieurs à l'enregistrement du département. Ces dernières gardent la
trace de leurs changesets dans leur commentaire, faute de mieux.
"""

import json
import logging
import os
import pathlib
import re
from datetime import timedelta

from src.matching import get_stats
from src.migrate import Migration

logger = logging.getLogger(__name__)

LOGS_DIR = pathlib.Path(
    os.environ.get("ATP2OSM_LOGS_DIR", pathlib.Path(__file__).parent.parent / "logs")
)

# "OSM API error for dept 94: ..." / "Unknown error for dept 94: ..." — le
# commentaire de l'intégration nomme chaque département en échec et dit de
# quelle erreur il s'agit.
FAILED_DPT_RE = re.compile(r"(OSM API|Unknown) error for dept (\w+):")
ERROR_KINDS = {"OSM API": "error_osm_api", "Unknown": "error_unknown"}


class BackfillImportDepartements(Migration):
    def migrate(self):
        with self.conn.cursor() as cursor:
            cursor.execute(
                """SELECT id, brand_wikidata, brand_name, import_date, status, comment,
                          items_count, tags_count, changeset_ids
                   FROM import_history
                   WHERE status <> 'cancelled'
                     AND (items_count IS NULL OR items_count > 0)
                     AND NOT EXISTS (
                         SELECT 1 FROM import_departements dpt WHERE dpt.import_id = import_history.id
                     )
                   ORDER BY import_date"""
            )
            rows = [
                dict(zip([c.name for c in cursor.description], r))
                for r in cursor.fetchall()
            ]

        # Un fichier de log par (dossier, jour) : deux intégrations tombant sur
        # le même fichier se sont écrasées l'une l'autre. Comme les lignes sont
        # triées par date, la dernière est celle que le fichier décrit ; les
        # précédentes restent sans détail.
        paths = {row["id"]: self._find_log_path(row) for row in rows}
        claimed = {}
        for row in rows:
            if paths[row["id"]]:
                claimed[paths[row["id"]]] = row["id"]

        done = 0
        for row in rows:
            path = paths[row["id"]]
            if path is None:
                logger.warning(
                    "Pas de log pour l'intégration %s (%s, %s) : sans détail.",
                    row["id"], row["brand_wikidata"], row["import_date"].date(),
                )
                self._keep_changeset_ids(row)
                continue

            if claimed[path] != row["id"]:
                logger.warning(
                    "Import %s non repris : le log %s décrit l'intégration %s",
                    row["id"], path, claimed[path],
                )
                self._keep_changeset_ids(row)
                continue

            changes, succeeded = self._read_log(path)

            # Les logs les plus anciens ne notaient ni le département ni le
            # changeset : rien à répartir, l'intégration reste sans détail.
            if not changes or any("departement_number" not in c for c in changes):
                logger.warning(
                    "Log %s au format ancien (sans département) : "
                    "intégration %s laissée sans détail.", path, row["id"],
                )
                self._keep_changeset_ids(row)
                continue

            self._write(row, changes, succeeded)
            done += 1

        logger.info("Reprise terminée : %d intégration(s) détaillée(s).", done)

    def _find_log_path(self, row):
        """Le fichier de log d'une intégration, ou None.

        Le nom du fichier vient de l'heure locale du serveur et la date de
        l'intégration est en UTC : la veille et le lendemain sont essayés.

        Le dossier, lui, porte le brand:wikidata *OSM* du premier POI, qui
        n'est pas toujours celui de la marque ATP (`Q246/` pour la marque
        `Q699709`), et vaut `unknown` quand ce tag manque. À défaut du dossier
        attendu, on cherche donc le log de la journée dont les POIs portent le
        nom de la marque.
        """
        day = row["import_date"].date()
        for date in (day, day - timedelta(days=1), day + timedelta(days=1)):
            expected = LOGS_DIR / row["brand_wikidata"] / f"{date}.json"
            if expected.is_file():
                return expected
            for path in sorted(LOGS_DIR.glob(f"*/{date}.json")):
                changes, _ = self._read_log(path)
                if changes and changes[0].get("atp_brand") == row["brand_name"]:
                    logger.info(
                        "Intégration %s (%s) retrouvée dans %s, rattachée par son nom.",
                        row["id"], row["brand_wikidata"], path.parent.name,
                    )
                    return path
        return None

    def _keep_changeset_ids(self, row):
        """changeset_ids disparaît juste après (migration 017) : pour une
        intégration qu'on ne sait pas détailler, on garde au moins la trace de
        ses changesets dans le commentaire."""
        if not row["changeset_ids"]:
            return
        trace = "Changesets : " + ", ".join(str(c) for c in row["changeset_ids"])
        with self.conn.cursor() as cursor:
            cursor.execute(
                "UPDATE import_history SET comment = CONCAT_WS(' — ', comment, %s::text) WHERE id = %s",
                (trace, row["id"]),
            )

    def _read_log(self, path):
        """Renvoie (changes, succeeded) ; succeeded vaut None si le log est
        antérieur à l'enregistrement des changesets réussis."""
        decoder = json.JSONDecoder()
        text = path.read_text(encoding="utf-8")
        changes, end = decoder.raw_decode(text)
        rest = text[end:].strip()
        succeeded = decoder.raw_decode(rest)[0] if rest else None
        return changes, succeeded

    def _write(self, row, changes, succeeded):
        children, uploaded = reconstruct(
            changes, succeeded, row["status"], row["comment"]
        )
        rows = [
            (
                row["id"],
                c["departement_number"],
                c["items_count"],
                c["osm_changeset_id"],
                c["status"],
                c["comment"],
            )
            for c in children
        ]

        with self.conn.cursor() as cursor:
            cursor.executemany(
                """INSERT INTO import_departements
                       (import_id, departement_number, items_count, osm_changeset_id, status, comment)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                rows,
            )
            # Les colonnes que l'intégration n'avait pas su remplir à l'époque.
            cursor.execute(
                """UPDATE import_history
                   SET items_count = COALESCE(items_count, %s),
                       tags_count  = COALESCE(tags_count, %s)
                   WHERE id = %s""",
                (len(uploaded), json.dumps(get_stats(uploaded)["by_tag"]), row["id"]),
            )


def reconstruct(changes, succeeded, status, comment):
    """Détail par département d'une intégration, depuis son log.

    *succeeded* est la liste des changesets réussis, ou None quand le log est
    antérieur à son enregistrement — on se rabat alors sur les départements
    nommés dans le commentaire d'erreur, qui donne aussi le type de chaque
    échec.

    Renvoie (lignes import_departements, POIs réellement envoyés).
    """
    # À défaut de commentaire nominatif, le suffixe du statut de l'intégration
    # (avant la migration 018, qui le supprime) ; sinon l'erreur reste inconnue.
    default_error = "error_osm_api" if status.endswith("osm_api") else "error_unknown"
    failed_from_comment = {
        _pad(dpt): ERROR_KINDS[kind]
        for kind, dpt in FAILED_DPT_RE.findall(comment or "")
    }

    by_dpt = {}
    for change in changes:
        dpt = _pad(change["departement_number"])
        entry = by_dpt.setdefault(dpt, {"changeset": None, "changes": []})
        entry["changes"].append(change)
        entry["changeset"] = entry["changeset"] or change.get("changeset")

    # Certains logs n'ont pas gardé le changeset sur chaque POI. Les
    # identifiants réussis ayant été notés dans l'ordre des départements, on
    # les redonne dans ce même ordre aux départements que le commentaire
    # n'accuse pas.
    positional = None
    if succeeded is not None and any(e["changeset"] is None for e in by_dpt.values()):
        candidates = [d for d in by_dpt if d not in failed_from_comment]
        if len(candidates) != len(succeeded):
            logger.warning(
                "%d département(s) candidat(s) pour %d changeset(s) réussi(s) : "
                "les derniers sont comptés en échec.", len(candidates), len(succeeded),
            )
        positional = dict(zip(candidates, succeeded))

    children = []
    uploaded = []
    for dpt, entry in by_dpt.items():
        if positional is not None:
            ok = dpt in positional
            changeset = positional.get(dpt)
        elif succeeded is None:
            ok = True
            changeset = entry["changeset"]
        else:
            ok = entry["changeset"] in succeeded
            changeset = entry["changeset"]

        # Le commentaire de l'intégration nomme les départements en échec
        # ("OSM API error for dept 94: ...") : il tranche.
        if dpt in failed_from_comment:
            ok = False

        if ok:
            uploaded.extend(entry["changes"])
        else:
            # Un département en échec garde son changeset quand il a été créé :
            # seul un échec à la création laisse l'identifiant nul. En mode
            # positionnel, on ne sait pas lequel c'était.
            changeset = entry["changeset"]

        children.append({
            "departement_number": dpt,
            "items_count": len(entry["changes"]),
            "osm_changeset_id": changeset,
            "status": "success" if ok else failed_from_comment.get(dpt, default_error),
            "comment": None if ok else comment,
        })

    return children, uploaded


def _pad(departement_number) -> str:
    """Les logs les plus anciens stockaient le département en entier (6, 94)."""
    return str(departement_number).zfill(2)
