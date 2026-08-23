"""Quick-pick reasons for invalidating an ATP suggestion.

Keys follow a `<subject>_<problem>` structure — subject first so the reasons
group by the field they blame, problem second so the same wording means the
same thing across subjects. They are what gets stored in
`import_history.comment`; labels are display only, so a wording change never
rewrites history. Derived from the free-text comments of the first 131
rejections in production.
"""

ERROR_REASONS = [
    ("website_generic", "Site web générique / de la marque"),
    ("website_broken", "Site web invalide (404, redirection)"),
    ("website_language", "Site web pas en français"),
    ("phone_wrong", "Téléphone incorrect"),
    ("phone_format", "Téléphone au mauvais format"),
    ("opening_hours_wrong", "Horaires incorrects"),
    ("address_wrong", "Adresse incorrecte"),
    ("email_generic", "E-mail générique"),
    ("poi_mismatch", "Ne cible pas le bon POI"),
    ("data_unverifiable", "Donnée invérifiable"),
    ("other", "Autre"),
]

REASON_LABELS = dict(ERROR_REASONS)
