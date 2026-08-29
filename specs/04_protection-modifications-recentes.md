# Protection des modifications humaines récentes

## Objectif

Ne jamais écraser une valeur qu'un contributeur humain a posée récemment.

ATP propose des tags issus des sites web des enseignes. Quand un mappeur a
renseigné à la main un `opening_hours` ou un `phone` la semaine dernière, sa
valeur est un choix délibéré, souvent vérifié sur le terrain : elle prime sur
la nôtre. À l'inverse, une valeur posée par un bot n'a pas été vérifiée par
qui que ce soit et peut être remplacée.

La règle, tag par tag :

- **modification ancienne** (au-delà du seuil) → on écrit
- **modification récente par un bot** → on écrit
- **modification récente par un humain** → on n'écrit pas ce tag

Le seuil de fraîcheur est un nombre de semaines, à fixer par configuration.

La protection est **par tag**, pas par objet. Un POI dont le `name` a été
corrigé hier reste éligible à recevoir un `website` d'ATP.

## Données nécessaires

| Donnée | Source | Coût |
|---|---|---|
| Date de dernière modif de l'objet | PBF Geofabrik, colonne `osm_timestamp` | gratuit, déjà en base |
| Versions successives d'un objet, avec tags et auteur | `GET /api/0.6/<type>/<id>/history` | 1 requête par objet |
| Tag `bot` du changeset | `GET /api/0.6/changeset/<id>` | 1 requête par changeset |

Ce que ces sources ne donnent **pas** :

- Aucune source, nulle part, ne date un tag individuellement. OSM ne stocke
  qu'un timestamp par objet. La date d'un tag se reconstruit en comparant les
  versions successives — il n'y a pas d'autre voie.
- Les extraits Geofabrik publics ne portent ni `changeset`, ni `uid`, ni
  `user` (vérifié : ces champs valent 0 / chaîne vide). Seuls `version` et
  `osm_timestamp` en sortent. L'auteur ne peut venir que de l'API.


## Algorithme

Appliqué au moment de la validation, sur le lot de POIs en cours.

```
pour chaque POI du lot :
    si osm_timestamp < NOW - seuil :
        écrire tous les tags du diff        # cas majoritaire, 0 requête
        continuer

    historique = GET /api/0.6/<type>/<id>/history

    pour chaque tag que le diff veut écrire :
        remonter les versions de la plus récente vers la plus ancienne
        jusqu'à trouver celle qui a posé la valeur actuelle
        -> date + changeset de cette version

        si date < NOW - seuil :          écrire
        sinon si changeset est un bot :  écrire
        sinon :                          ne pas écrire ce tag
```

Deux filtres en amont font tout le travail d'économie : le `osm_timestamp`
écarte la grande majorité des POIs sans aucune requête, et la restriction aux
seuls tags que le diff veut écrire évite de dater des tags dont on n'a que
faire.

### Identifier un bot

`bot=yes` sur le changeset est le seul marqueur retenu. Tout le reste est
traité comme humain.

Le nom d'utilisateur n'est pas un signal : OSM n'impose aucune convention de
nommage et beaucoup d'imports tournent sous un compte ordinaire. `created_by`
non plus : il nomme l'outil, pas la nature de l'édition.

Conséquence assumée : un bot qui ne se déclare pas est traité comme un humain,
et sa valeur est préservée. Le doute profite à l'existant.

### Remonter à la version qui a posé la valeur

Une seule règle : en partant de la version courante et en descendant, la
réponse est **la plus ancienne version consécutive portant la valeur
actuelle**. C'est celle qui l'a posée.

L'absence de tag est une valeur comme une autre dans cette comparaison. Une
version qui réécrit la même valeur ne rompt donc pas la suite, et une valeur
recréée à l'identique après suppression la rompt bien — la suppression est une
valeur différente. Un objet à une seule version répond v1.

## Volumétrie et limites externes

- Validation par lots de 100 POIs. Le nombre de requêtes par lot est celui
  des POIs qui survivent au filtre `osm_timestamp`, plus un par changeset
  récent rencontré. Cette proportion n'est pas mesurée : elle dépend du seuil
  retenu et de l'activité réelle sur les POIs concernés. Le plafond absolu
  reste borné par la taille du lot.
- L'API OSM ne publie aucune limite de fréquence en lecture : ni en-tête
  `X-RateLimit-*`, ni `Retry-After`. `/api/capabilities` ne donne que des
  limites de taille. La règle est un usage raisonnable : séquentiel, et un
  `User-Agent` identifiant l'application.
- Les lectures passent par un CDN, elles n'atteignent pas toutes la base.

## Impact sur le pipeline

`osm2pgsql` tourne avec `-x` et `generic.lua` remplit `osm_timestamp`
(epoch, `int8` — le flex output n'a pas de type `timestamp`). `mv_places`
expose la colonne convertie par `to_timestamp()`.

La colonne n'est peuplée qu'après un import osm2pgsql complet. Note au
passage : `version` était déjà déclarée dans le style mais sortait à zéro,
faute de `-x`.

## Interface

Une modification remplace une valeur déjà présente dans OSM ; c'est un geste
plus engageant que l'ajout d'un tag absent. L'interface doit le refléter.

**Lot de taille 1 en bêta.** Les modifications partent par lots d'un seul
POI, indépendamment de `BATCH_MAX_SIZE` qui régit les ajouts. La valeur est
provisoire, le temps d'observer les premiers retours de la communauté ; elle
est relevée ensuite.

**Un exemple par tag modifié dans la liste des validations.** Pour chaque tag
que le lot veut modifier, la liste montre un cas concret — la valeur OSM
actuelle et la valeur ATP proposée — et non le seul nom du tag. On doit
pouvoir juger de la pertinence d'une modification sans ouvrir l'écran de
validation.

**Badge sur les intégrations comportant des modifications.** Visible dans la
liste et dans l'historique, il distingue au premier coup d'œil une intégration
qui ne fait qu'ajouter des tags d'une qui en remplace.

**Avant/après lisible à l'écran de validation.** Les valeurs actuelle et
proposée se lisent côte à côte, alignées, sans avoir à les chercher dans le
reste des tags de l'objet. Les tags ajoutés et les tags remplacés se
distinguent visuellement.

## Hors périmètre

- Reconstituer l'évolution complète d'un tag dans le temps. On ne cherche
  qu'une date, celle de la valeur actuelle.
- Les extraits `osm-internal.download.geofabrik.de`, qui portent `user` et
  `changeset` mais imposent une authentification OSM dans le pipeline. Les
  quelques requêtes API par lot rendent ce coût inutile.
- Le dump planet des changesets (~6 Go), pour la même raison.
