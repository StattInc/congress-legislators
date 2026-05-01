CREATE TABLE IF NOT EXISTS civic.us_federal_legislator_profiles (
    bioguide_id TEXT PRIMARY KEY,
    first_name TEXT,
    middle_name TEXT,
    last_name TEXT,
    suffix TEXT,
    nickname TEXT,
    official_full TEXT,
    display_name TEXT,
    birthday DATE,
    gender TEXT CHECK (gender IN ('M', 'F') OR gender IS NULL),
    other_names JSONB,
    about_page_url TEXT,
    biography TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS civic.us_federal_legislator_ids (
    bioguide_id TEXT NOT NULL REFERENCES civic.us_federal_legislator_profiles (bioguide_id) ON DELETE CASCADE,
    id_type TEXT NOT NULL,
    id_value TEXT NOT NULL,
    is_previous BOOLEAN NOT NULL DEFAULT FALSE,
    sort_order INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bioguide_id, id_type, id_value)
);

CREATE INDEX IF NOT EXISTS idx_us_federal_legislator_ids_lookup
    ON civic.us_federal_legislator_ids (id_type, id_value);

CREATE TABLE IF NOT EXISTS civic.us_federal_legislator_terms (
    term_key TEXT PRIMARY KEY,
    bioguide_id TEXT NOT NULL REFERENCES civic.us_federal_legislator_profiles (bioguide_id) ON DELETE CASCADE,
    term_ordinal INTEGER NOT NULL,
    term_type TEXT NOT NULL CHECK (term_type IN ('sen', 'rep')),
    chamber TEXT NOT NULL CHECK (chamber IN ('Senate', 'House')),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    state_code TEXT NOT NULL,
    district INTEGER,
    senate_class INTEGER,
    state_rank TEXT,
    party TEXT,
    caucus TEXT,
    how TEXT,
    end_type TEXT,
    url TEXT,
    address TEXT,
    phone TEXT,
    fax TEXT,
    contact_form TEXT,
    office TEXT,
    rss_url TEXT,
    seat_key TEXT,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    source_file TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_us_federal_legislator_terms_bioguide_dates
    ON civic.us_federal_legislator_terms (bioguide_id, start_date, end_date);

CREATE INDEX IF NOT EXISTS idx_us_federal_legislator_terms_seat_dates
    ON civic.us_federal_legislator_terms (seat_key, start_date, end_date);

CREATE INDEX IF NOT EXISTS idx_us_federal_legislator_terms_current
    ON civic.us_federal_legislator_terms (is_current);

CREATE TABLE IF NOT EXISTS civic.us_federal_legislator_party_affiliations (
    affiliation_key TEXT PRIMARY KEY,
    term_key TEXT NOT NULL REFERENCES civic.us_federal_legislator_terms (term_key) ON DELETE CASCADE,
    bioguide_id TEXT NOT NULL REFERENCES civic.us_federal_legislator_profiles (bioguide_id) ON DELETE CASCADE,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    party TEXT,
    caucus TEXT,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_us_federal_legislator_party_affiliations_bioguide_dates
    ON civic.us_federal_legislator_party_affiliations (bioguide_id, start_date, end_date);

CREATE TABLE IF NOT EXISTS civic.us_federal_legislator_leadership_roles (
    role_key TEXT PRIMARY KEY,
    bioguide_id TEXT NOT NULL REFERENCES civic.us_federal_legislator_profiles (bioguide_id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    chamber TEXT,
    start_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_us_federal_legislator_leadership_roles_bioguide_dates
    ON civic.us_federal_legislator_leadership_roles (bioguide_id, start_date, end_date);

CREATE OR REPLACE VIEW civic.v_us_federal_legislators_enriched_current AS
SELECT
    t.bioguide_id,
    p.first_name,
    p.middle_name,
    p.last_name,
    p.suffix,
    p.nickname,
    p.official_full,
    p.display_name,
    p.birthday,
    p.gender,
    p.other_names,
    p.about_page_url,
    p.biography,
    t.term_type,
    t.chamber,
    t.start_date,
    t.end_date,
    t.state_code,
    t.district,
    t.senate_class,
    t.state_rank,
    t.party,
    t.caucus,
    t.how,
    t.end_type,
    t.url AS website,
    t.address,
    t.phone,
    t.fax,
    t.contact_form,
    t.office,
    t.rss_url,
    t.seat_key
FROM civic.us_federal_legislator_terms t
JOIN civic.us_federal_legislator_profiles p
    ON p.bioguide_id = t.bioguide_id
WHERE t.is_current = TRUE;

CREATE OR REPLACE VIEW civic.v_us_federal_congressional_seat_history AS
SELECT
    t.seat_key,
    t.term_key,
    t.bioguide_id,
    p.first_name,
    p.middle_name,
    p.last_name,
    p.official_full,
    t.term_type,
    t.chamber,
    t.start_date,
    t.end_date,
    t.state_code,
    t.district,
    t.senate_class,
    t.state_rank,
    t.party,
    t.caucus,
    t.how,
    t.end_type,
    t.is_current
FROM civic.us_federal_legislator_terms t
JOIN civic.us_federal_legislator_profiles p
    ON p.bioguide_id = t.bioguide_id
WHERE t.seat_key IS NOT NULL;

CREATE OR REPLACE VIEW civic.v_us_federal_legislator_career AS
SELECT
    t.bioguide_id,
    'term'::TEXT AS role_kind,
    CASE
        WHEN t.term_type = 'sen' THEN 'Senator'
        ELSE 'Representative'
    END AS role_title,
    t.chamber,
    t.start_date,
    t.end_date,
    t.state_code,
    t.district,
    t.senate_class,
    t.party,
    t.caucus,
    t.seat_key,
    t.term_key AS source_key
FROM civic.us_federal_legislator_terms t
UNION ALL
SELECT
    lr.bioguide_id,
    'leadership_role'::TEXT AS role_kind,
    lr.title AS role_title,
    lr.chamber,
    lr.start_date,
    lr.end_date,
    NULL::TEXT AS state_code,
    NULL::INTEGER AS district,
    NULL::INTEGER AS senate_class,
    NULL::TEXT AS party,
    NULL::TEXT AS caucus,
    NULL::TEXT AS seat_key,
    lr.role_key AS source_key
FROM civic.us_federal_legislator_leadership_roles lr;