-- A single-category brand applied without looking at the object's own primary
-- tag, so the NSI category was written as a brand new key: way/130021335
-- (shop=supermarket, brand:wikidata=Q89029184) came out amenity=fuel too.
--
-- Classification is never NSI's to set. On a disagreement the primary key is
-- dropped and the rest of the entry still applies -- the usual disagreement is
-- a subtype nuance (NSI shop=telecommunication vs OSM shop=mobile_phone) where
-- everything else is right. 2626 French objects, ~250 on a writable key.
--
-- Q89029184 only looked single-category because _UNREACHABLE_KEYS drops its
-- advertising/totem entry on import: the shortcut rested on a count the
-- pipeline reshapes, hence the fix here rather than in the filter.
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
            SELECT CASE
                     WHEN prim IS NULL OR prim = ARRAY[primary_key, primary_value]
                     THEN tags
                     ELSE tags - primary_key
                   END
              INTO hit FROM nsi_brands
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
$$ LANGUAGE plpgsql STABLE SET search_path = public, pg_temp;
