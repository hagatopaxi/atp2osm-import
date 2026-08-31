-- The département gives way to the administrative subdivision: it is no longer
-- derived from a postcode but from the OSM boundary that contains the POI, and
-- "département" is a French word for a French concept.
--
-- Mechanical renaming, no data touched. subdivision_name is filled by 023: the
-- history has to stand on its own, because a code can be split (20 -> 2A/2B)
-- while the line still records what actually happened.

ALTER TABLE import_departements RENAME TO import_subdivisions;
ALTER TABLE import_subdivisions RENAME COLUMN departement_number TO subdivision_code;

ALTER INDEX import_departements_import_id_idx RENAME TO import_subdivisions_import_id_idx;
ALTER INDEX import_departements_dpt_idx RENAME TO import_subdivisions_code_idx;
ALTER SEQUENCE import_departements_id_seq RENAME TO import_subdivisions_id_seq;

ALTER TABLE import_subdivisions ADD COLUMN IF NOT EXISTS subdivision_name TEXT;
