-- Create a reusable phone normalization function.
-- Replaces any international prefix (+XX or 00XX) with 0, then strips separators.
-- Used in indexes (atp_fr, mv_places) and matching WHERE clauses.
-- IMMUTABLE + STRICT: required for use in functional indexes.

CREATE OR REPLACE FUNCTION normalize_phone(phone TEXT) RETURNS TEXT
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE AS $$
  SELECT REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE($1, '^\+\d{1,3}', '0'),  -- +33, +596, +262, +687... → 0
      '^00\d{1,3}', '0'                         -- 0033, 00596...          → 0
    ),
    '[\s\-\.\(\)]', '', 'g'                     -- supprime séparateurs
  );
$$;
