####################################################
#' Overture Map Foundation Data Download
#' Project Name       - Practice
#'
#' Author             - Pukar Bhandari
#' Team Members       -
####################################################

# ==================================================
# 0. References
# ==================================================

# Overture Maps Extract Data Locally: https://docs.overturemaps.org/getting-data/locally
# DuckDB R API: https://duckdb.org/docs/api/r.html

# Other references:
#   1. https://til.simonwillison.net/overture-maps/overture-maps-parquet
#   2. https://dev.to/savo/spatial-data-analysis-with-duckdb-40j9
#   3. https://cheginit.github.io/til/python/buildings.html#overture-buildings-using-duckdb
#   4. https://walker-data.com/posts/overture-buildings/

# ==================================================
# 1. Setting up the environment
# ==================================================

# -------------------------------------
# Install and Load packages
# -------------------------------------

# Install and load the pacman package
if (!require("pacman")) {
  install.packages("pacman")
}
# library("pacman")

# Install and load multiple desired packages at once
p_load(
  tidyverse, # data manipulation and visualization
  sf, # spatial data
  tmap, # view spatial data

  DBI, # database interface: Connect-Disconnect
  duckdb, # use duckdb connection
  arrow # read parquet data
)

# -------------------------------------
# Global Options
# -------------------------------------

options(scipen = 999) # avoiding scientific notation
tmap_mode(mode = "view")

# ==================================================
# 2. Load and Prepare data
# ==================================================

# -------------------------------------
# Set Working Directory
# -------------------------------------

# Set your main data folder
wd = "M:/MA_Project/GA_Albany_Resilence/GIS"
project = "Albany_Resilience_Task1/Default.gdb"
# setwd(wd)

# -------------------------------------
# Albany Boundary
# -------------------------------------

st_layers(file.path(wd, project))

albany_boundary <- read_sf(file.path(wd, project), layer = "City_of_Albany") %>%
  st_transform(4326)

albany_bbox <- albany_boundary %>%
  st_bbox() %>%
  as.vector()

# ==================================================
# 2. Download Data
# ==================================================

# -------------------------------------
# Setup Function to Download Data
# -------------------------------------

# Define the function to query Overture's buildings data
overture_data <- function(bbox, overture_type, dst_parquet) {

  # Define the theme map
  map_themes <- list(
    "locality" = "admins",
    "locality_area" = "admins",
    "administrative_boundary" = "admins",
    "building" = "buildings",
    "building_part" = "buildings",
    "place" = "places",
    "segment" = "transportation",
    "connector" = "transportation",
    "infrastructure" = "base",
    "land" = "base",
    "land_use" = "base",
    "water" = "base"
  )

  # Validate overture_type
  if (!overture_type %in% names(map_themes)) {
    stop(paste("Valid Overture types are:", paste(names(map_themes), collapse = ", ")))
  }

  s3_region <- "us-west-2"
  base_url <- sprintf("s3://overturemaps-%s/release", s3_region)
  version <- "2024-04-16-beta.0"
  theme <- map_themes[[overture_type]]
  remote_path <- sprintf("%s/%s/theme=%s/type=%s/*", base_url, version, theme, overture_type)

  # Connect to DuckDB
  conn <- dbConnect(duckdb::duckdb())
  dbExecute(conn, "INSTALL httpfs;")
  dbExecute(conn, "INSTALL spatial;")
  dbExecute(conn, "LOAD httpfs;")
  dbExecute(conn, "LOAD spatial;")
  dbExecute(conn, sprintf("SET s3_region='%s';", s3_region))

  # Create SQL query
  read_parquet <- sprintf("read_parquet('%s', filename=TRUE, hive_partitioning=1);", remote_path)
  dbExecute(conn, sprintf("CREATE OR REPLACE VIEW data_view AS SELECT * FROM %s", read_parquet))

  query <- sprintf("
    SELECT
      data.*
      --ST_GeomFromWKB(data.geometry) as geometry,
    FROM data_view AS data
    WHERE data.bbox.xmin <= %f AND data.bbox.xmax >= %f
    AND data.bbox.ymin <= %f AND data.bbox.ymax >= %f
  ", bbox[3], bbox[1], bbox[4], bbox[2])

  # Define output file path
  file <- normalizePath(dst_parquet, mustWork = FALSE)

  # Execute the query and save the results to a Parquet file
  dbExecute(conn, sprintf("COPY (%s) TO '%s' WITH (FORMAT 'parquet');", query, file))

  # Close the connection
  dbDisconnect(conn, shutdown = TRUE)
}

# -------------------------------------
# Download Parquet Data
# -------------------------------------

albany_bbox # Format: xmin, ymin, xmax, ymax

overture_data(albany_bbox, "place", "albany_places_subset.parquet")

# -------------------------------------
# Read Parquet Data
# -------------------------------------

# Load and process the Parquet file
albany_places <- read_parquet("albany_places_subset.parquet")

# -------------------------------------
# Convert to sf objtect
# -------------------------------------

albany_places_sf <- st_as_sf(
  albany_places %>% select(-sources),
  geometry = albany_places$geometry,
  crs = 4326
)

albany_places_sf %>%
  select(names$primary, categories$main, confidence) %>%
  # mapview::mapview()
  tmap::qtm()
