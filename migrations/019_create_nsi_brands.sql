-- NSI (name-suggestion-index) brand index, filled by src/pipeline/nsi.py.
--
-- One row per (brand:wikidata, primary key, primary value). The primary key
-- doubles as the deduplication rule enforced in Python: a brand:wikidata is
-- NOT unique in NSI (E.Leclerc alone carries 15 items), and two items sharing
-- both the QID and the category cannot be told apart, so both are dropped.
-- Should the Python dedup ever break, this constraint fails loudly.
CREATE TABLE IF NOT EXISTS nsi_brands (
    brand_wikidata  text  NOT NULL,
    brand           text  NOT NULL,
    name            text,
    primary_key     text  NOT NULL,   -- shop, amenity, office…
    primary_value   text  NOT NULL,
    tags            jsonb NOT NULL    -- subset of NSI_WRITABLE_TAGS
);

-- No primary key on (brand_wikidata, primary_key, primary_value): several rows
-- may share it on purpose. A brand:wikidata is not unique in NSI (E.Leclerc
-- alone carries 15 items), and two items sharing the category still differ by
-- their labels — which nothing writes to OSM, but which both serve as lookups.
-- What must never differ is the tags they would write; select_items drops the
-- whole group when it does.
CREATE INDEX IF NOT EXISTS nsi_brands_qid_idx ON nsi_brands (brand_wikidata);

-- Reverse index: recovering a QID from a label plus a category.
CREATE INDEX IF NOT EXISTS nsi_brands_brand_idx
    ON nsi_brands (LOWER(brand), primary_key, primary_value);
CREATE INDEX IF NOT EXISTS nsi_brands_name_idx
    ON nsi_brands (LOWER(name), primary_key, primary_value);


-- 'nsi' joins 'osm' and 'atp' as a datasource of its own.
ALTER TABLE data_imports DROP CONSTRAINT IF EXISTS data_imports_type_check;
ALTER TABLE data_imports ADD CONSTRAINT data_imports_type_check
    CHECK (type IN ('osm', 'atp', 'nsi', 'pipeline'));


-- The primary tag of an OSM object, as [key, value], or NULL.
--
-- Used to pick which NSI entry applies when a brand:wikidata carries several
-- (E.Leclerc alone spans supermarket, jewelry, pharmacy, fuel…): the category
-- the object already carries is the only discriminator. NSI items get theirs
-- from their category path; an OSM object has no path, and can carry several
-- candidates at once (amenity=pharmacy + healthcare=pharmacy), hence a fixed
-- priority order. unnest preserves array order, so LIMIT 1 picks the first key
-- present.
--
-- The list holds only keys that can actually reach mv_places: generic.lua drops
-- every object carrying highway, waterway, man_made, advertising, power,
-- railway… long before it gets there.
CREATE OR REPLACE FUNCTION osm_primary_tag(osm_tags jsonb) RETURNS text[] AS $$
    SELECT ARRAY[k, osm_tags->>k]
    FROM unnest(ARRAY['shop', 'amenity', 'tourism', 'office', 'leisure',
                      'healthcare', 'craft', 'landuse']) AS k
    WHERE osm_tags ? k
    LIMIT 1;
$$ LANGUAGE sql IMMUTABLE SET search_path = public, pg_temp;


-- The tags NSI has for an OSM object, or NULL.
--
-- Two ways in, and they never both apply:
--
--   * the object already carries brand:wikidata -> look the QID up. A QID
--     spanning a single category applies unconditionally; one spanning several
--     is narrowed down by the object's primary tag, and yields nothing without
--     one.
--
--   * the object carries no QID -> recover it from (label, primary tag). Both
--     conditions are needed: 'renault' alone is ambiguous, 'renault' plus
--     shop=car_repair is not. Measured over the whole French set, no such pair
--     ever leads to two QIDs — the count check below keeps that a fact rather
--     than an assumption.
--
-- Rows sharing a (QID, category) always carry the same tags by construction,
-- so LIMIT 1 picks an arbitrary one on purpose.
CREATE OR REPLACE FUNCTION nsi_match(osm_tags jsonb) RETURNS jsonb AS $$
DECLARE
    prim text[] := osm_primary_tag(osm_tags);
    qid  text   := osm_tags->>'brand:wikidata';
    hit  jsonb;
    n    integer;
BEGIN
    IF qid IS NOT NULL THEN
        SELECT count(*) INTO n FROM (
            SELECT DISTINCT primary_key, primary_value
            FROM nsi_brands WHERE brand_wikidata = qid
        ) categories;

        IF n = 1 THEN
            SELECT tags INTO hit FROM nsi_brands
             WHERE brand_wikidata = qid LIMIT 1;
            RETURN hit;
        END IF;

        IF n = 0 OR prim IS NULL THEN
            RETURN NULL;
        END IF;

        -- NULL when no category matches: NSI never reclassifies an object.
        SELECT tags INTO hit FROM nsi_brands
         WHERE brand_wikidata = qid
           AND primary_key = prim[1] AND primary_value = prim[2]
         LIMIT 1;
        RETURN hit;
    END IF;

    IF prim IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT count(DISTINCT brand_wikidata) INTO n FROM nsi_brands
     WHERE primary_key = prim[1] AND primary_value = prim[2]
       AND (LOWER(brand) = LOWER(osm_tags->>'brand')
            OR LOWER(name) = LOWER(osm_tags->>'name'));

    IF n <> 1 THEN
        RETURN NULL;
    END IF;

    SELECT tags INTO hit FROM nsi_brands
     WHERE primary_key = prim[1] AND primary_value = prim[2]
       AND (LOWER(brand) = LOWER(osm_tags->>'brand')
            OR LOWER(name) = LOWER(osm_tags->>'name'))
     LIMIT 1;
    RETURN hit;
END;
-- search_path is pinned because CREATE MATERIALIZED VIEW ... WITH DATA runs its
-- query under a restricted one: without this, the body cannot resolve
-- nsi_brands at execution time, while the plain SQL functions around it
-- (resolved at parse time) keep working.
$$ LANGUAGE plpgsql STABLE SET search_path = public, pg_temp;
