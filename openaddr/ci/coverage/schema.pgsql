CREATE EXTENSION IF NOT EXISTS postgis;

DROP TABLE IF EXISTS gpwv4_2015;
DROP TABLE IF EXISTS boxes;
DROP TABLE IF EXISTS areas;

CREATE TABLE boxes
(
    id          SERIAL PRIMARY KEY,
    lon         FLOAT NOT NULL,
    lat         FLOAT NOT NULL,
    size        FLOAT NOT NULL,
    geom        GEOMETRY(Polygon, 4326)
);

CREATE TABLE areas
(
    iso_a2      VARCHAR(2) PRIMARY KEY,
    addr_count  INTEGER,
    buffer_km   FLOAT,
    geom        GEOMETRY(MultiPolygon, 4326),

    name        TEXT,
    area_total  INTEGER,
    area_pct    FLOAT,
    pop_total   INTEGER,
    pop_pct     FLOAT,
    
    cpp_min     FLOAT,
    cpp_avg     FLOAT,
    cpp_max     FLOAT,
    cpp_med     FLOAT,
    cpp_stddev  FLOAT
);

CREATE TABLE gpwv4_2015
(
    iso_a2      VARCHAR(2),
    iso_a3      VARCHAR(3),
    box_id      INTEGER REFERENCES boxes(id),
    population  FLOAT,
    area        FLOAT
);

CREATE TEMPORARY TABLE gpwv4_2015_temp
(
    iso_a2      VARCHAR(2),
    iso_a3      VARCHAR(3),
    lon         FLOAT NOT NULL,
    lat         FLOAT NOT NULL,
    size        FLOAT NOT NULL,
    population  FLOAT,
    area        FLOAT
);

-- COPY gpwv4_2015_temp FROM '/tmp/gpwv4-2015-cut.csv' DELIMITER ',' HEADER CSV;
-- 
-- --
-- -- Populate unique boxes from GPWv4 table
-- --
-- INSERT INTO boxes (lon, lat, size, geom)
--     SELECT DISTINCT lon, lat, size,
--     ST_SetSRID(ST_MakeBox2D(ST_MakePoint(lon, lat), ST_MakePoint(lon+size, lat+size)), 4326)
--     FROM gpwv4_2015_temp;
-- 
-- ALTER TABLE boxes ADD UNIQUE (size, lon, lat);
-- CREATE INDEX box_geoms ON boxes USING GIST (geom);
-- 
-- --
-- -- Populate GPWv4 areas
-- --
-- INSERT INTO gpwv4_2015 (iso_a2, iso_a3, population, area, box_id)
--     SELECT g.iso_a2, g.iso_a3, g.population, g.area, b.id
--     FROM gpwv4_2015_temp AS g
--     LEFT JOIN boxes AS b ON b.lat = g.lat AND b.lon = g.lon AND b.size = g.size;
