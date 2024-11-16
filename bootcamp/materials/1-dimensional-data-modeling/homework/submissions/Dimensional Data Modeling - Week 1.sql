--1. DDL for the actors Table
CREATE TABLE actors (
    actor_id TEXT PRIMARY KEY,  -- Change to TEXT
    actor_name VARCHAR(255) NOT NULL,
    films JSONB NOT NULL,
    is_active BOOLEAN NOT NULL
);


--2. Cumulative Query to Populate the actors Table Year by Year
WITH film_data AS (
    SELECT
        actorid AS actor_id,  -- Treat actorid as TEXT
        actor AS actor_name,
        JSONB_AGG(
            JSONB_BUILD_OBJECT(
                'film', film,
                'votes', votes,
                'rating', rating,
                'film_id', filmid,
                'quality_class', CASE
                    WHEN rating > 8 THEN 'star'
                    WHEN rating > 7 THEN 'good'
                    WHEN rating > 6 THEN 'average'
                    ELSE 'bad'
                END
            )
        ) AS films,
        MAX(year) AS last_year
    FROM actor_films
    GROUP BY actorid, actor
),
actor_status AS (
    SELECT
        actor_id,
        actor_name,
        films,
        (last_year = EXTRACT(YEAR FROM CURRENT_DATE)) AS is_active
    FROM film_data
)
INSERT INTO actors (actor_id, actor_name, films, is_active)
SELECT
    actor_id,  -- Ensure actor_id is treated as TEXT
    actor_name,
    films,
    is_active
FROM actor_status;

--3. DDL for the actors_history_scd Table
CREATE TABLE actors_history_scd (
    id SERIAL PRIMARY KEY,                     -- Unique identifier for history records
    actor_id TEXT NOT NULL,                    -- Actor ID as TEXT
    actor_name VARCHAR(255) NOT NULL,          -- Actor name
    films JSONB NOT NULL,                      -- Films stored as JSONB
    quality_class VARCHAR(20) NOT NULL,        -- Quality classification (star, good, etc.)
    is_active BOOLEAN NOT NULL,                -- Actor's active status
    start_date DATE NOT NULL,                  -- Start date for the record's validity
    end_date DATE,                             -- End date for the record's validity (NULL if current)
    is_current BOOLEAN DEFAULT TRUE            -- Flag to indicate if the record is current
);

--4. Backfill Query for actors_history_scd
WITH actor_data AS (
    SELECT
        actor_id,
        actor_name,
        films,
        CASE
            WHEN AVG((films->>'rating')::FLOAT) > 8 THEN 'star'
            WHEN AVG((films->>'rating')::FLOAT) > 7 THEN 'good'
            WHEN AVG((films->>'rating')::FLOAT) > 6 THEN 'average'
            ELSE 'bad'
        END AS quality_class,
        is_active,
        CURRENT_DATE AS start_date,
        NULL::DATE AS end_date  -- Explicitly cast NULL to DATE
    FROM actors,
    LATERAL JSONB_ARRAY_ELEMENTS(films) AS films
    GROUP BY actor_id, actor_name, films, is_active
)
INSERT INTO actors_history_scd (actor_id, actor_name, films, quality_class, is_active, start_date, end_date, is_current)
SELECT
    actor_id,
    actor_name,
    films,
    quality_class,
    is_active,
    start_date,
    end_date,
    TRUE AS is_current
FROM actor_data;

--5. Incremental Query for actors_history_scd
-- Step 1: Identify changed actors and insert new records
WITH current_actors AS (
    SELECT
        actor_id,
        actor_name,
        films,
        CASE
            WHEN AVG((films->>'rating')::FLOAT) > 8 THEN 'star'
            WHEN AVG((films->>'rating')::FLOAT) > 7 THEN 'good'
            WHEN AVG((films->>'rating')::FLOAT) > 6 THEN 'average'
            ELSE 'bad'
        END AS quality_class,
        is_active
    FROM actors,
    LATERAL JSONB_ARRAY_ELEMENTS(films) AS films
    GROUP BY actor_id, actor_name, films, is_active
),
previous_scd AS (
    SELECT *
    FROM actors_history_scd
    WHERE is_current = TRUE
),
changed_actors AS (
    SELECT
        curr.actor_id,
        curr.actor_name,
        curr.films,
        curr.quality_class,
        curr.is_active,
        CURRENT_DATE AS start_date,
        NULL::DATE AS end_date
    FROM current_actors curr
    LEFT JOIN previous_scd prev
    ON curr.actor_id = prev.actor_id
    WHERE curr.quality_class != prev.quality_class
       OR curr.is_active != prev.is_active
)
-- Insert new records
INSERT INTO actors_history_scd (actor_id, actor_name, films, quality_class, is_active, start_date, end_date, is_current)
SELECT
    actor_id,
    actor_name,
    films,
    quality_class,
    is_active,
    start_date,
    end_date,
    TRUE AS is_current
FROM changed_actors;

-- Step 2: Update previous records to mark them as non-current
WITH current_actors AS (
    SELECT
        actor_id,
        actor_name,
        films,
        CASE
            WHEN AVG((films->>'rating')::FLOAT) > 8 THEN 'star'
            WHEN AVG((films->>'rating')::FLOAT) > 7 THEN 'good'
            WHEN AVG((films->>'rating')::FLOAT) > 6 THEN 'average'
            ELSE 'bad'
        END AS quality_class,
        is_active
    FROM actors,
    LATERAL JSONB_ARRAY_ELEMENTS(films) AS films
    GROUP BY actor_id, actor_name, films, is_active
),
previous_scd AS (
    SELECT *
    FROM actors_history_scd
    WHERE is_current = TRUE
)
UPDATE actors_history_scd
SET is_current = FALSE, end_date = CURRENT_DATE
WHERE id IN (
    SELECT prev.id
    FROM current_actors curr
    JOIN previous_scd prev
    ON curr.actor_id = prev.actor_id
    WHERE curr.quality_class != prev.quality_class
       OR curr.is_active != prev.is_active
);