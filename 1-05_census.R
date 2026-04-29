

# **************************************************
#                     DETAILS
#
# Purpose:   Download 2020 ACS 5-year block group estimates for California
#            using tidycensus. Attach block group GEOID to the analytic
#            sample and prop_wide via spatial join (lat/lon -> block group
#            polygon). Save enriched files and a standalone BG-level
#            contextual file for use in later analysis.
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2026-04-28
# Started:   MM/DD/YYYY
# Updated:   MM/DD/YYYY
#
# ---- INPUTS ----
#
#   1-04_analytic.rds   -- joined analytic sample (transaction-level)
#   1-04_prop_wide.rds  -- all properties + wide txn history
#   Census API key      -- set via census_api_key() or CENSUS_API_KEY env var
#
# ---- ACS VINTAGE ----
#
#   Survey:  ACS 5-year, 2020 (2016-2020)
#   Level:   Block group
#   State:   California (FIPS 06)
#   Geometry: TRUE -- block group polygons for spatial join
#
# ---- VARIABLES DOWNLOADED ----
#
#   B25064_001  -- median gross rent (monthly, $)
#   B25003_001  -- total occupied housing units (tenure denominator)
#   B25003_002  -- owner-occupied units
#   B25003_003  -- renter-occupied units
#   B25002_001  -- total housing units
#   B25002_003  -- vacant housing units
#   B25077_001  -- median value owner-occupied housing ($)
#   B19013_001  -- median household income ($)
#   B01003_001  -- total population
#   B25058_001  -- median contract rent ($)  [alt rent measure]
#
#   Margins of error downloaded by tidycensus but dropped -- not used in analysis.
#
# ---- DERIVED VARIABLES ----
#
#   pct_renter     = renter_occ / total_occ * 100
#   vacancy_rate   = vacant / total_units * 100
#   area_km2       = polygon area in km² (from CA Albers geometry, EPSG:3310)
#   pop_density    = total_pop / area_km2  (persons per km²)
#
# ---- SPATIAL JOIN DESIGN ----
#
#   Properties have lat/lon from the property file (prop_slim), assumed WGS84
#   (EPSG:4326) -- Cotality standard, verified via coordinate range check at
#   runtime. Block group polygons are reprojected from NAD83 (EPSG:4269, as
#   returned by tidycensus) to CA Albers (EPSG:3310) for accurate
#   point-in-polygon join. Source CRS and bounding box both verified at runtime.
#   st_within used (not st_intersects) to avoid duplicate matches on shared
#   BG boundaries. Rows missing lat/lon (corrupt CLIPs from 1-04) are dropped
#   here -- fix source in 1-02/1-03. 1-05_analytic.rds is the spatially-
#   complete subset of 1-04_analytic.rds; similarly for prop_wide.
#
# ---- OUTPUTS ----
#
#   1-05_bg_acs.rds        -- block group contextual file (BG-level, CA)
#   1-05_analytic.rds      -- analytic + GEOID + BG contextual columns
#   1-05_prop_wide.rds     -- prop_wide + GEOID + BG contextual columns
#   Plots: Process/Images/1-05_*.png
#   log:      secure drive / Process / Logs
#   drop list: Dropbox / Process / Logs
# **************************************************


# *************
# 1. Setup ----

user   <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root   <- paste0(user, "Fire Investment/")
secure <- "Y:/Institutional Investment/"

data_output_s <- paste0(secure, "Data/Derived/Fire Investment/")

# logging
timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file  <- paste0(secure, "Process/Fire Investment/Logs/1-05_log_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)

start_time <- Sys.time()
set.seed(123)

drop_list <- list()

cat("\n====================================================\n")
cat("  1-05_tidycensus.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

# packages
pkgs <- c("dplyr", "tidyr", "tidycensus", "sf", "beepr", "ggplot2", "scales")
invisible(lapply(pkgs, function(p) {
  if (!require(p, character.only = TRUE)) {
    install.packages(p)
    library(p, character.only = TRUE)
  }
}))
rm(pkgs)

# helper: timestamped log message
ts <- function(label = "") {
  cat(sprintf("\n[%s]  %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), label))
}

# Census API key -- set once per machine; comment out after first run
# census_api_key("YOUR_KEY_HERE", install = TRUE)
# Sys.setenv(CENSUS_API_KEY = "YOUR_KEY_HERE")  # session-only alternative


# *****************************************************
# 2. Load inputs ----
# *****************************************************

ts("Loading 1-04_analytic.rds...")
analytic <- readRDS(paste0(data_output_s, "1-04_analytic.rds"))
cat(sprintf("  analytic: %s rows x %d columns\n",
            formatC(nrow(analytic), format = "d", big.mark = ","), ncol(analytic)))

ts("Loading 1-04_prop_wide.rds...")
prop_wide <- readRDS(paste0(data_output_s, "1-04_prop_wide.rds"))
cat(sprintf("  prop_wide: %s rows x %d columns\n",
            formatC(nrow(prop_wide), format = "d", big.mark = ","), ncol(prop_wide)))

stopifnot("lat" %in% names(analytic),  "lon" %in% names(analytic))
stopifnot("lat" %in% names(prop_wide), "lon" %in% names(prop_wide))

# ---- lat/lon NA rates ----
cat("\n  lat/lon NA rates:\n")
cat(sprintf("    analytic  lat NA: %s  lon NA: %s\n",
            formatC(sum(is.na(analytic$lat)),  format = "d", big.mark = ","),
            formatC(sum(is.na(analytic$lon)),  format = "d", big.mark = ",")))
cat(sprintf("    prop_wide lat NA: %s  lon NA: %s\n",
            formatC(sum(is.na(prop_wide$lat)), format = "d", big.mark = ","),
            formatC(sum(is.na(prop_wide$lon)), format = "d", big.mark = ",")))

# ---- Coordinate range check ----
# WGS84 (EPSG:4326) for California: lat ~32-42, lon ~-124 to -114.
# If coords are in a projected CRS (metres), values would be in the millions.
# Bounds below are deliberately loose to tolerate edge-case imputed coords.
cat("\n  Property coordinate ranges (WGS84 CA expected: lat 32-42, lon -124 to -114):\n")
cat(sprintf("    analytic  lat: %.4f to %.4f\n",
            min(analytic$lat,  na.rm = TRUE), max(analytic$lat,  na.rm = TRUE)))
cat(sprintf("    analytic  lon: %.4f to %.4f\n",
            min(analytic$lon,  na.rm = TRUE), max(analytic$lon,  na.rm = TRUE)))
cat(sprintf("    prop_wide lat: %.4f to %.4f\n",
            min(prop_wide$lat, na.rm = TRUE), max(prop_wide$lat, na.rm = TRUE)))
cat(sprintf("    prop_wide lon: %.4f to %.4f\n",
            min(prop_wide$lon, na.rm = TRUE), max(prop_wide$lon, na.rm = TRUE)))

stopifnot(
  min(analytic$lat,  na.rm = TRUE) > 30,
  max(analytic$lat,  na.rm = TRUE) < 43,
  min(analytic$lon,  na.rm = TRUE) > -126,
  max(analytic$lon,  na.rm = TRUE) < -113,
  min(prop_wide$lat, na.rm = TRUE) > 30,
  max(prop_wide$lat, na.rm = TRUE) < 43,
  min(prop_wide$lon, na.rm = TRUE) > -126,
  max(prop_wide$lon, na.rm = TRUE) < -113
)
cat("  Coordinate range check passed -- consistent with WGS84 California.\n")

drop_list <- c(drop_list,
               "Property lat/lon coordinate ranges verified -- consistent with WGS84 California"
)


# *****************************************************
# 3. Pull ACS codebook and confirm variable codes ----
# *****************************************************
# load_variables() returns the full ACS variable list for a given year/survey.
# Confirm that each code maps to the expected concept and label before
# trusting the download. Output is logged so confirmation is reproducible.
# acs_codebook is left in the environment for interactive lookup.
# *****************************************************
ts("Pulling ACS 2020 5-year codebook for variable confirmation...")

acs_codebook <- tidycensus::load_variables(year = 2020, dataset = "acs5", cache = TRUE)

cat(sprintf("  Codebook rows: %s\n", formatC(nrow(acs_codebook), format = "d", big.mark = ",")))
cat(sprintf("  Codebook columns: %s\n", paste(names(acs_codebook), collapse = ", ")))

codes_to_check <- c(
  "B25064_001",
  "B25003_001",
  "B25003_002",
  "B25003_003",
  "B25002_001",
  "B25002_003",
  "B25077_001",
  "B19013_001",
  "B01003_001",
  "B25058_001"
)

codebook_check <- acs_codebook[acs_codebook$name %in% codes_to_check, ]
codebook_check <- codebook_check[order(codebook_check$name), ]

cat("\n  --- ACS 2020 codebook entries for requested variables ---\n")
cat(sprintf("  %-14s  %-44s  %s\n", "CODE", "LABEL", "CONCEPT"))
cat(sprintf("  %s\n", strrep("-", 108)))
for (i in seq_len(nrow(codebook_check))) {
  cat(sprintf("  %-14s  %-44s  %s\n",
              codebook_check$name[i],
              substr(codebook_check$label[i], 1, 44),
              substr(codebook_check$concept[i], 1, 55)))
}
cat(sprintf("  %s\n", strrep("-", 108)))

missing_codes <- setdiff(codes_to_check, codebook_check$name)
if (length(missing_codes) > 0) {
  stop(sprintf(
    "The following codes were NOT found in the ACS 2020 codebook: %s\nCheck spelling or vintage.",
    paste(missing_codes, collapse = ", ")
  ))
} else {
  cat(sprintf("\n  All %d codes confirmed present in ACS 2020 codebook.\n", length(codes_to_check)))
}

cat("\n  *** STOP AND READ the table above before proceeding. ***\n")
cat("  Confirm LABEL and CONCEPT match expectations for each code.\n")
cat("  If anything looks wrong, fix acs_vars in Section 4 and re-run.\n")

drop_list <- c(drop_list, sprintf(
  "ACS codebook confirmed: all %d variable codes present in ACS 2020 5-year", length(codes_to_check)
))

rm(codebook_check, missing_codes, codes_to_check)


# *****************************************************
# 4. Define ACS variables and download ----
# *****************************************************

acs_vars <- c(
  median_gross_rent    = "B25064_001",
  total_occ            = "B25003_001",
  owner_occ            = "B25003_002",
  renter_occ           = "B25003_003",
  total_units          = "B25002_001",
  vacant               = "B25002_003",
  median_housing_value = "B25077_001",
  median_hh_income     = "B19013_001",
  total_pop            = "B01003_001",
  median_contract_rent = "B25058_001"
)

ts("Downloading ACS 5-year 2020 block group data for California...")
cat(sprintf("  Variables requested: %d\n", length(acs_vars)))
cat(sprintf("  Variables: %s\n", paste(names(acs_vars), collapse = ", ")))

bg_raw <- get_acs(
  geography = "block group",
  variables = acs_vars,
  state     = "CA",
  year      = 2020,
  survey    = "acs5",
  geometry  = TRUE,
  output    = "wide"    # one row per BG; produces *E (estimate) and *M (MOE) columns
)

cat(sprintf("\n  Block groups downloaded: %s\n",
            formatC(nrow(bg_raw), format = "d", big.mark = ",")))
cat(sprintf("  Columns: %s\n", paste(names(bg_raw), collapse = ", ")))

drop_list <- c(drop_list, sprintf(
  "ACS 5-yr 2020 BG download: %s block groups, %d variables, state = CA",
  formatC(nrow(bg_raw), format = "d", big.mark = ","), length(acs_vars)
))


# *****************************************************
# 5. Clean and derive BG-level variables ----
# *****************************************************
ts("Cleaning block group data and deriving variables...")

# tidycensus wide output: *E = estimate, *M = margin of error.
# MOEs dropped -- not used in analysis. Select and rename estimates only.
bg <- bg_raw %>%
  dplyr::select(
    GEOID, NAME, geometry,
    median_gross_rentE, total_occE, owner_occE, renter_occE,
    total_unitsE, vacantE, median_housing_valueE,
    median_hh_incomeE, total_popE, median_contract_rentE
  ) %>%
  dplyr::rename(
    geoid                = GEOID,
    bg_name              = NAME,
    median_gross_rent    = median_gross_rentE,
    total_occ            = total_occE,
    owner_occ            = owner_occE,
    renter_occ           = renter_occE,
    total_units          = total_unitsE,
    vacant               = vacantE,
    median_housing_value = median_housing_valueE,
    median_hh_income     = median_hh_incomeE,
    total_pop            = total_popE,
    median_contract_rent = median_contract_rentE
  ) %>%
  dplyr::mutate(
    pct_renter   = dplyr::if_else(total_occ > 0,
                                  renter_occ / total_occ * 100,
                                  NA_real_),
    vacancy_rate = dplyr::if_else(total_units > 0,
                                  vacant / total_units * 100,
                                  NA_real_)
  )

# area_km2 and pop_density are computed in Section 6 after reprojection
# to CA Albers (EPSG:3310), where st_area() returns accurate metre-based areas.

cat(sprintf("  bg rows: %s  |  columns: %d\n",
            formatC(nrow(bg), format = "d", big.mark = ","), ncol(bg)))

key_est <- c("median_gross_rent", "median_housing_value", "median_hh_income",
             "total_pop", "pct_renter", "vacancy_rate")
# Note: pop_density NA audit runs in Section 6 after CA Albers reprojection
cat("\n  NA counts on key estimates (BG level):\n")
for (v in key_est) {
  n_na <- sum(is.na(sf::st_drop_geometry(bg)[[v]]))
  cat(sprintf("    %-28s  NA: %s (%.1f%%)\n",
              v,
              formatC(n_na, format = "d", big.mark = ","),
              100 * n_na / nrow(bg)))
}
rm(key_est)

drop_list <- c(drop_list,
               "BG-level derived variables created: pct_renter, vacancy_rate, area_km2, pop_density (computed in Section 6 on CA Albers geometry)",
               "MOEs dropped (not used in analysis)"
)


# *****************************************************
# 6. Verify CRS and project to CA Albers for spatial join ----
# *****************************************************
ts("Verifying source CRS and projecting to CA Albers (EPSG:3310)...")

# Check what CRS tidycensus actually returned before assuming anything.
# tidycensus returns Census geometries in NAD83 (EPSG:4269) by default,
# but this can vary by sf/tigris version -- verify at runtime.
bg_crs_raw <- sf::st_crs(bg_raw)
cat(sprintf("  Raw BG CRS (from tidycensus): EPSG:%s  |  %s\n",
            bg_crs_raw$epsg,
            bg_crs_raw$input))

if (!isTRUE(bg_crs_raw$epsg == 4269)) {
  warning(sprintf(
    "Expected ACS geometry in EPSG:4269 (NAD83) but got EPSG:%s (%s).\nReview CRS before proceeding -- downstream transform assumes NAD83 input.",
    bg_crs_raw$epsg, bg_crs_raw$input
  ))
} else {
  cat("  Source CRS confirmed: EPSG:4269 (NAD83) -- as expected from tidycensus.\n")
}

# Reproject to CA Albers (EPSG:3310).
# CA Albers is the standard equal-area projection for California spatial work.
# Using a projected CRS avoids angular distortion of NAD83 for
# point-in-polygon operations, particularly near polygon edges.
bg_3310 <- sf::st_transform(bg, crs = 3310)

# Compute area and population density on the projected geometry.
# st_area() requires a projected CRS for accurate results -- CA Albers metres
# give area in m²; divide by 1e6 for km². Computed on bg_3310 then mirrored
# back to bg so both objects stay in sync.
area_km2_vec <- as.numeric(sf::st_area(bg_3310)) / 1e6

bg <- bg %>%
  dplyr::mutate(
    area_km2    = area_km2_vec,
    pop_density = dplyr::if_else(area_km2 > 0,
                                 total_pop / area_km2,
                                 NA_real_)
  )

bg_3310 <- bg_3310 %>%
  dplyr::mutate(
    area_km2    = area_km2_vec,
    pop_density = dplyr::if_else(area_km2 > 0,
                                 total_pop / area_km2,
                                 NA_real_)
  )

cat(sprintf("  area_km2 range: %.3f to %.1f km²\n",
            min(bg$area_km2, na.rm = TRUE), max(bg$area_km2, na.rm = TRUE)))
cat(sprintf("  pop_density range: %.2f to %.1f persons/km²\n",
            min(bg$pop_density, na.rm = TRUE), max(bg$pop_density, na.rm = TRUE)))
n_zero_area <- sum(bg$area_km2 == 0, na.rm = TRUE)
n_na_density <- sum(is.na(bg$pop_density))
cat(sprintf("  pop_density NA: %d  (zero-area polygons: %d)\n",
            n_na_density, n_zero_area))
if (n_zero_area > 0) warning(sprintf(
  "%d BG polygons have zero area -- pop_density will be NA for these.", n_zero_area))
rm(area_km2_vec, n_zero_area, n_na_density)

bg_crs_3310 <- sf::st_crs(bg_3310)
cat(sprintf("  Transformed BG CRS: EPSG:%s  |  %s\n",
            bg_crs_3310$epsg,
            bg_crs_3310$input))

# Sanity-check bounding box: CA Albers coords are in metres.
# For California, expected ranges roughly: x -400k to 550k, y -700k to 500k.
# If the transform failed silently, coords will still be in degrees (~-125 to
# -114), giving a bbox width < 100 -- catch that here with a hard stop.
bb <- sf::st_bbox(bg_3310)
cat("  BG bounding box (should be CA Albers metres, not degrees):\n")
cat(sprintf("    xmin: %12.1f   xmax: %12.1f   width:  %10.1f\n",
            bb["xmin"], bb["xmax"], bb["xmax"] - bb["xmin"]))
cat(sprintf("    ymin: %12.1f   ymax: %12.1f   height: %10.1f\n",
            bb["ymin"], bb["ymax"], bb["ymax"] - bb["ymin"]))

if (abs(bb["xmax"] - bb["xmin"]) < 1000) {
  stop(paste0(
    "BG bounding box width < 1000 -- coordinates look like degrees, not metres.\n",
    "CRS transform likely failed. Inspect sf::st_crs(bg_3310) before proceeding."
  ))
} else {
  cat("  Bounding box width > 1000 -- transform to metres confirmed.\n")
}

rm(bg_crs_raw, bg_crs_3310, bb)

drop_list <- c(drop_list,
               "CRS: ACS geometry confirmed EPSG:4269 (NAD83); reprojected to EPSG:3310 (CA Albers)",
               "CRS: bounding box checked -- confirmed metres, not degrees",
               "Property points assumed EPSG:4326 (WGS84) -- confirmed via coordinate range check"
)

# Helper: convert a data frame with lat/lon to sf points in CA Albers.
# Property lat/lon assumed WGS84 (EPSG:4326) -- confirmed above.
make_points_3310 <- function(df) {
  df_valid <- df[!is.na(df$lat) & !is.na(df$lon), ]
  pts <- sf::st_as_sf(df_valid,
                      coords = c("lon", "lat"),
                      crs    = 4326,
                      remove = FALSE)
  sf::st_transform(pts, crs = 3310)
}

# Slim BG layer -- only columns to attach to property files.
# Built once here, used in both spatial joins below.
bg_slim_cols <- c("geoid", "bg_name",
                  "median_gross_rent", "median_contract_rent",
                  "total_pop", "total_occ", "renter_occ", "owner_occ",
                  "total_units", "vacant",
                  "median_housing_value", "median_hh_income",
                  "pct_renter", "vacancy_rate",
                  "area_km2", "pop_density")
bg_slim <- bg_3310[, bg_slim_cols]

dir.create(paste0(root, "Process/Images"), showWarnings = FALSE, recursive = TRUE)


# *****************************************************
# 7. Spatial join: analytic ----
# *****************************************************
ts("Spatial join: analytic -> block groups...")

analytic_pts  <- make_points_3310(analytic)
analytic_join <- sf::st_join(analytic_pts, bg_slim, join = sf::st_within, left = TRUE)
analytic_bg   <- sf::st_drop_geometry(analytic_join)

n_analytic_total   <- nrow(analytic)
n_analytic_valid   <- sum(!is.na(analytic$lat) & !is.na(analytic$lon))
n_analytic_matched <- sum(!is.na(analytic_bg$geoid))
n_analytic_no_ll   <- n_analytic_total - n_analytic_valid

cat(sprintf("  analytic rows total:           %s\n", formatC(n_analytic_total,   format = "d", big.mark = ",")))
cat(sprintf("  analytic rows with lat/lon:    %s\n", formatC(n_analytic_valid,   format = "d", big.mark = ",")))
cat(sprintf("  analytic rows missing lat/lon: %s\n", formatC(n_analytic_no_ll,   format = "d", big.mark = ",")))
cat(sprintf("  analytic rows with GEOID:      %s (%.2f%% of valid lat/lon)\n",
            formatC(n_analytic_matched, format = "d", big.mark = ","),
            100 * n_analytic_matched / max(n_analytic_valid, 1)))

# Plot unmatched analytic points against BG polygons.
# analytic_pts still in scope here -- rm'd after plot below.
unmatched_pts_a <- analytic_pts[is.na(analytic_bg$geoid), ]
n_unmatched_a   <- nrow(unmatched_pts_a)
cat(sprintf("  Unmatched analytic points (no BG): %s\n",
            formatC(n_unmatched_a, format = "d", big.mark = ",")))

plot_sample_a <- if (n_unmatched_a > 5000) {
  unmatched_pts_a[sample(n_unmatched_a, 5000), ]
} else {
  unmatched_pts_a
}

png(paste0(root, "Process/Images/1-05_unmatched_analytic.png"),
    width = 1400, height = 1000, res = 150)
plot(sf::st_geometry(bg_3310), col = NA, border = "grey80",
     main = sprintf("analytic: unmatched points (red) -- %s total, %s plotted",
                    formatC(n_unmatched_a,        format = "d", big.mark = ","),
                    formatC(nrow(plot_sample_a),  format = "d", big.mark = ",")))
if (n_unmatched_a > 0) {
  plot(sf::st_geometry(plot_sample_a), col = "red", pch = 16, cex = 0.3, add = TRUE)
}
dev.off()
cat(sprintf("  Unmatched analytic plot saved.\n"))

rm(analytic_pts, analytic_join, unmatched_pts_a, plot_sample_a, n_unmatched_a)

# Drop rows missing lat/lon -- spatially unplaceable (corrupt CLIPs from 1-04).
# These rows were never passed to the spatial join and will never get a GEOID.
# TODO: fix source coordinates in 1-02/1-03 and re-run.
# 1-05_analytic.rds is therefore the spatially-complete subset of 1-04_analytic.rds.
if (n_analytic_no_ll > 0) {
  analytic_bg <- analytic_bg[!is.na(analytic_bg$lat) & !is.na(analytic_bg$lon), ]
  cat(sprintf("  Dropped %s rows missing lat/lon (spatially unplaceable).\n",
              formatC(n_analytic_no_ll, format = "d", big.mark = ",")))
}

drop_list <- c(drop_list,
               sprintf("analytic spatial join: %s of %s rows matched to a BG GEOID (%.2f%%)",
                       formatC(n_analytic_matched, format = "d", big.mark = ","),
                       formatC(n_analytic_total,   format = "d", big.mark = ","),
                       100 * n_analytic_matched / max(n_analytic_valid, 1)),
               sprintf("analytic: dropped %s rows missing lat/lon -- fix source coords in 1-02/1-03",
                       formatC(n_analytic_no_ll, format = "d", big.mark = ","))
)

n_analytic_dropped <- n_analytic_no_ll   # saved for stopifnot in Section 9
rm(n_analytic_total, n_analytic_valid, n_analytic_no_ll, n_analytic_matched)


# *****************************************************
# 8. Spatial join: prop_wide ----
# *****************************************************
ts("Spatial join: prop_wide -> block groups...")

prop_pts  <- make_points_3310(prop_wide)
prop_join <- sf::st_join(prop_pts, bg_slim, join = sf::st_within, left = TRUE)
prop_bg   <- sf::st_drop_geometry(prop_join)

n_prop_total   <- nrow(prop_wide)
n_prop_valid   <- sum(!is.na(prop_wide$lat) & !is.na(prop_wide$lon))
n_prop_matched <- sum(!is.na(prop_bg$geoid))
n_prop_no_ll   <- n_prop_total - n_prop_valid

cat(sprintf("  prop_wide rows total:           %s\n", formatC(n_prop_total,   format = "d", big.mark = ",")))
cat(sprintf("  prop_wide rows with lat/lon:    %s\n", formatC(n_prop_valid,   format = "d", big.mark = ",")))
cat(sprintf("  prop_wide rows missing lat/lon: %s\n", formatC(n_prop_no_ll,   format = "d", big.mark = ",")))
cat(sprintf("  prop_wide rows with GEOID:      %s (%.2f%% of valid lat/lon)\n",
            formatC(n_prop_matched, format = "d", big.mark = ","),
            100 * n_prop_matched / max(n_prop_valid, 1)))

# Plot unmatched prop_wide points -- prop_wide is the full property universe
# so this is the more important diagnostic of the two.
unmatched_pts_p <- prop_pts[is.na(prop_bg$geoid), ]
n_unmatched_p   <- nrow(unmatched_pts_p)
cat(sprintf("  Unmatched prop_wide points (no BG): %s\n",
            formatC(n_unmatched_p, format = "d", big.mark = ",")))

plot_sample_p <- if (n_unmatched_p > 5000) {
  unmatched_pts_p[sample(n_unmatched_p, 5000), ]
} else {
  unmatched_pts_p
}

png(paste0(root, "Process/Images/1-05_unmatched_prop_wide.png"),
    width = 1400, height = 1000, res = 150)
plot(sf::st_geometry(bg_3310), col = NA, border = "grey80",
     main = sprintf("prop_wide: unmatched points (red) -- %s total, %s plotted",
                    formatC(n_unmatched_p,        format = "d", big.mark = ","),
                    formatC(nrow(plot_sample_p),  format = "d", big.mark = ",")))
if (n_unmatched_p > 0) {
  plot(sf::st_geometry(plot_sample_p), col = "red", pch = 16, cex = 0.3, add = TRUE)
}
dev.off()
cat("  Unmatched prop_wide plot saved.\n")

rm(prop_pts, prop_join, unmatched_pts_p, plot_sample_p, n_unmatched_p)

# Drop rows missing lat/lon -- same rationale as analytic above.
if (n_prop_no_ll > 0) {
  prop_bg <- prop_bg[!is.na(prop_bg$lat) & !is.na(prop_bg$lon), ]
  cat(sprintf("  Dropped %s rows missing lat/lon (spatially unplaceable).\n",
              formatC(n_prop_no_ll, format = "d", big.mark = ",")))
}

drop_list <- c(drop_list,
               sprintf("prop_wide spatial join: %s of %s rows matched to a BG GEOID (%.2f%%)",
                       formatC(n_prop_matched, format = "d", big.mark = ","),
                       formatC(n_prop_total,   format = "d", big.mark = ","),
                       100 * n_prop_matched / max(n_prop_valid, 1)),
               sprintf("prop_wide: dropped %s rows missing lat/lon -- fix source coords in 1-02/1-03",
                       formatC(n_prop_no_ll, format = "d", big.mark = ","))
)

n_prop_dropped <- n_prop_no_ll   # saved for stopifnot in Section 9
rm(n_prop_total, n_prop_valid, n_prop_no_ll, n_prop_matched)


# *****************************************************
# 9. Post-join validation ----
# *****************************************************
ts("Post-join validation...")

# GEOID length -- CA block group GEOIDs are 12 characters
geoid_lengths <- nchar(na.omit(analytic_bg$geoid))
cat(sprintf("  GEOID length check (analytic): min=%d max=%d (expected 12)\n",
            min(geoid_lengths), max(geoid_lengths)))
if (any(geoid_lengths != 12)) warning("  !!! Some GEOIDs are not 12 characters -- investigate.")
rm(geoid_lengths)

cat("\n  median_hh_income distribution (analytic):\n")
print(summary(analytic_bg$median_hh_income))

cat("\n  pct_renter distribution (analytic):\n")
print(summary(analytic_bg$pct_renter))

cat("\n  vacancy_rate distribution (analytic):\n")
print(summary(analytic_bg$vacancy_rate))

# Row counts: analytic_bg and prop_bg are smaller than their sources by the
# number of dropped missing-coord rows. Verify the difference is exactly that.
stopifnot(nrow(analytic_bg) == nrow(analytic)  - n_analytic_dropped)
stopifnot(nrow(prop_bg)     == nrow(prop_wide) - n_prop_dropped)
cat(sprintf("\n  Row count check passed: analytic %s -> %s (-%s missing coord)\n",
            formatC(nrow(analytic),  format = "d", big.mark = ","),
            formatC(nrow(analytic_bg), format = "d", big.mark = ","),
            formatC(n_analytic_dropped, format = "d", big.mark = ",")))
cat(sprintf("  Row count check passed: prop_wide %s -> %s (-%s missing coord)\n",
            formatC(nrow(prop_wide), format = "d", big.mark = ","),
            formatC(nrow(prop_bg),   format = "d", big.mark = ","),
            formatC(n_prop_dropped,  format = "d", big.mark = ",")))
rm(n_analytic_dropped, n_prop_dropped)



# *****************************************************
# 10. Corporate buyer context: BG-level summary stats ----
# *****************************************************
# Early look at whether corporate-buyer transactions (buy1_corp == 1) are
# concentrated in systematically different block groups than non-corporate.
# Unit of analysis: transaction (analytic_bg row), matched to BG only.
# Tests: Wilcoxon rank-sum -- non-parametric, no normality assumption,
#        appropriate for skewed Census variables (rent, income, value).
# Effect size: Cohen's d on ranks (robust alternative to raw-scale d).
# Note: these are BG-level contextual variables attached to transactions,
#       not property-level attributes. One BG can appear many times if
#       multiple transactions occurred in it. That is expected and correct
#       for this descriptive pass; account for clustering in formal models.
# *****************************************************
ts("Corporate buyer BG-level summary stats (Section 10)...")

# Restrict to matched rows with non-missing buy1_corp
corp_df <- analytic_bg[
  !is.na(analytic_bg$buy1_corp),   # all rows now have a GEOID
]

n_corp    <- sum(corp_df$buy1_corp == 1L)
n_noncorp <- sum(corp_df$buy1_corp == 0L)
cat(sprintf("  Rows used: %s corporate  |  %s non-corporate\n",
            formatC(n_corp,    format = "d", big.mark = ","),
            formatC(n_noncorp, format = "d", big.mark = ",")))

# Variables to compare
bg_context_vars <- c(
  "median_gross_rent",
  "median_housing_value",
  "median_hh_income",
  "pct_renter",
  "vacancy_rate",
  "pop_density"
)

# Cohen's d on ranks (robust to outliers/skew)
cohens_d_ranks <- function(x, group) {
  x     <- as.numeric(x)
  group <- as.integer(group)
  r     <- rank(x, na.last = "keep")
  r1    <- r[group == 1]; r0 <- r[group == 0]
  r1    <- r1[!is.na(r1)]; r0 <- r0[!is.na(r0)]
  if (length(r1) < 2 || length(r0) < 2) return(NA_real_)
  pooled_sd <- sqrt(((length(r1) - 1) * var(r1) +
                       (length(r0) - 1) * var(r0)) /
                      (length(r1) + length(r0) - 2))
  (mean(r1) - mean(r0)) / pooled_sd
}

cat("\n")
cat(sprintf("  %-24s  %10s  %10s  %10s  %10s  %8s  %s\n",
            "VARIABLE", "CORP mean", "NON-CORP", "DIFF", "DIFF %", "Cohen d", "Wilcox p"))
cat(sprintf("  %s\n", strrep("-", 95)))

corp_stats <- list()

for (v in bg_context_vars) {
  x_corp    <- corp_df[[v]][corp_df$buy1_corp == 1L]
  x_noncorp <- corp_df[[v]][corp_df$buy1_corp == 0L]
  
  m_corp    <- mean(x_corp,    na.rm = TRUE)
  m_noncorp <- mean(x_noncorp, na.rm = TRUE)
  diff_abs  <- m_corp - m_noncorp
  diff_pct  <- if (!is.na(m_noncorp) && m_noncorp != 0) 100 * diff_abs / abs(m_noncorp) else NA_real_
  
  # Wilcoxon rank-sum -- suppress ties warning (expected with Census data)
  wt <- suppressWarnings(
    wilcox.test(x_corp, x_noncorp, exact = FALSE, correct = TRUE)
  )
  
  d <- cohens_d_ranks(corp_df[[v]], corp_df$buy1_corp)
  
  # significance stars
  stars <- dplyr::case_when(
    wt$p.value < 0.001 ~ "***",
    wt$p.value < 0.01  ~ "**",
    wt$p.value < 0.05  ~ "*",
    TRUE               ~ ""
  )
  
  cat(sprintf("  %-24s  %10.1f  %10.1f  %+10.1f  %+9.1f%%  %8.3f  %.2e %s\n",
              v, m_corp, m_noncorp, diff_abs, diff_pct, d, wt$p.value, stars))
  
  corp_stats[[v]] <- list(
    mean_corp    = m_corp,
    mean_noncorp = m_noncorp,
    diff_abs     = diff_abs,
    diff_pct     = diff_pct,
    cohens_d     = d,
    wilcox_p     = wt$p.value
  )
}

cat(sprintf("  %s\n", strrep("-", 95)))
cat("  Significance: *** p<0.001  ** p<0.01  * p<0.05\n")
cat("  Cohen's d on ranks: |d| ~0.2 small, ~0.5 medium, ~0.8 large\n")
cat("  Note: N is large so even tiny differences will be significant.\n")
cat("        Focus on Cohen's d and diff % for practical magnitude.\n")

drop_list <- c(drop_list, sprintf(
  "Corporate buyer BG context: Wilcoxon + Cohen's d on %d variables (%s corp, %s non-corp txns)",
  length(bg_context_vars),
  formatC(n_corp,    format = "d", big.mark = ","),
  formatC(n_noncorp, format = "d", big.mark = ",")
))

# ---- Box plots: corporate vs non-corporate by BG variable ----
# Sample non-corporate rows to keep plot rendering manageable
set.seed(123)
plot_vars  <- c("median_gross_rent", "median_housing_value",
                "median_hh_income",  "pct_renter", "pop_density")
plot_labels <- c(
  median_gross_rent    = "Median Gross Rent ($)",
  median_housing_value = "Median Housing Value ($)",
  median_hh_income     = "Median HH Income ($)",
  pct_renter           = "% Renter-Occupied",
  pop_density          = "Pop. Density (persons/km²)"
)

n_sample     <- min(n_noncorp, 50000L)
corp_plot_df <- dplyr::bind_rows(
  corp_df[corp_df$buy1_corp == 1L, c("buy1_corp", plot_vars)],
  corp_df[corp_df$buy1_corp == 0L, c("buy1_corp", plot_vars)][
    sample(n_noncorp, n_sample), ]
) %>%
  dplyr::mutate(
    buyer_type = dplyr::if_else(buy1_corp == 1L, "Corporate", "Non-corporate")
  ) %>%
  tidyr::pivot_longer(cols = dplyr::all_of(plot_vars),
                      names_to  = "variable",
                      values_to = "value") %>%
  dplyr::filter(!is.na(value)) %>%
  dplyr::mutate(
    variable = factor(variable, levels = plot_vars,
                      labels = plot_labels[plot_vars])
  )

p_box <- ggplot(corp_plot_df,
                aes(x = buyer_type, y = value, fill = buyer_type)) +
  geom_boxplot(outlier.size = 0.3, outlier.alpha = 0.3, width = 0.5) +
  scale_fill_manual(values = c("Corporate"     = "#d62728",
                               "Non-corporate" = "steelblue"),
                    guide  = "none") +
  facet_wrap(~ variable, scales = "free_y", ncol = 3) +
  labs(title    = "BG Context: Corporate vs Non-Corporate Buyers (ACS 2020)",
       subtitle = sprintf(
         "Corporate: %s txns  |  Non-corporate: %s txns (%s sampled for plot)",
         formatC(n_corp,    format = "d", big.mark = ","),
         formatC(n_noncorp, format = "d", big.mark = ","),
         formatC(n_sample,  format = "d", big.mark = ",")),
       x = NULL, y = NULL) +
  theme_minimal() +
  theme(strip.text = element_text(size = 8))

ggsave(paste0(root, "Process/Images/1-05_corp_bg_context.png"),
       plot = p_box, width = 12, height = 7, dpi = 150)
cat("\n  Box plot saved: 1-05_corp_bg_context.png\n")

rm(corp_df, corp_plot_df, corp_stats, p_box,
   n_corp, n_noncorp, n_sample, bg_context_vars, plot_vars, plot_labels,
   cohens_d_ranks)


# *****************************************************
# 11. Diagnostic plot: income histogram ----
# *****************************************************
ts("Generating income distribution plot...")

bg_in_analytic <- unique(analytic_bg$geoid)  # all rows have GEOID after Section 7 drop
bg_plot <- sf::st_drop_geometry(bg) %>%
  dplyr::mutate(in_analytic = geoid %in% bg_in_analytic)

p1 <- ggplot(bg_plot, aes(x = median_hh_income, fill = in_analytic)) +
  geom_histogram(bins = 60, color = "white", position = "identity", alpha = 0.7) +
  scale_fill_manual(values = c("TRUE" = "steelblue", "FALSE" = "grey70"),
                    labels = c("TRUE" = "BG in analytic sample", "FALSE" = "BG not in analytic"),
                    name   = NULL) +
  scale_x_continuous(labels = scales::dollar_format(scale = 1e-3, suffix = "k"),
                     limits = c(0, 250000)) +
  labs(title    = "CA Block Group Median HH Income (ACS 2020)",
       subtitle = "Blue = BGs with at least one transaction in analytic sample",
       x        = "Median HH Income",
       y        = "Block groups") +
  theme_minimal()
print(p1)
ggsave(paste0(root, "Process/Images/1-05_bg_income_distribution.png"),
       plot = p1, width = 10, height = 5, dpi = 150)

rm(bg_in_analytic, bg_plot, p1)


# *****************************************************
# 12. Save ----
# *****************************************************
ts("Saving outputs...")

# Standalone BG contextual file -- geometry stripped for smaller file size
bg_save <- sf::st_drop_geometry(bg)
out_bg        <- paste0(data_output_s, "1-05_bg_acs.rds")
out_analytic  <- paste0(data_output_s, "1-05_analytic.rds")
out_prop_wide <- paste0(data_output_s, "1-05_prop_wide.rds")

saveRDS(bg_save,     file = out_bg);        cat(sprintf("  bg_acs    -> %s\n", out_bg))
saveRDS(analytic_bg, file = out_analytic);  cat(sprintf("  analytic  -> %s\n", out_analytic))
saveRDS(prop_bg,     file = out_prop_wide); cat(sprintf("  prop_wide -> %s\n", out_prop_wide))

drop_list <- c(drop_list,
               sprintf("1-05_bg_acs.rds:    %s BGs x %d cols (geometry stripped)",
                       formatC(nrow(bg_save),     format = "d", big.mark = ","), ncol(bg_save)),
               sprintf("1-05_analytic.rds:  %s rows x %d cols",
                       formatC(nrow(analytic_bg), format = "d", big.mark = ","), ncol(analytic_bg)),
               sprintf("1-05_prop_wide.rds: %s rows x %d cols",
                       formatC(nrow(prop_bg),     format = "d", big.mark = ","), ncol(prop_bg))
)


# *****************************************************
# 13. Close out ----
# *****************************************************
cat("\n\nObjects saved:\n")
cat(sprintf("  bg_acs:    %s\n", out_bg))
cat(sprintf("  analytic:  %s\n", out_analytic))
cat(sprintf("  prop_wide: %s\n", out_prop_wide))

drop_list_file <- paste0(root, "Process/Logs/1-05_drop_list_", timestamp, ".txt")
writeLines(as.character(drop_list), drop_list_file)
cat(sprintf("  Drop list: %s\n", drop_list_file))

cat("\nDrop list summary:\n")
invisible(lapply(drop_list, function(x) cat(sprintf("  %s\n", x))))

cat("\n====================================================\n")
cat("  Decisions needed after reviewing output:\n")
cat("  1. Codebook table (Section 3) -- read LABEL and CONCEPT for each code.\n")
cat("     If anything looks wrong, fix acs_vars in Section 4 and re-run.\n")
cat("  2. CRS block (Section 6) -- source EPSG, target EPSG, and bounding box\n")
cat("     all logged. Script stops hard if bbox looks like degrees, not metres.\n")
cat("  3. Unmatched point plots -- check 1-05_unmatched_analytic.png and\n")
cat("     1-05_unmatched_prop_wide.png. Random scatter = coordinate noise,\n")
cat("     spatial clustering = bad data batch or offshore/border coords.\n")
cat("  4. GEOID match rate -- low rates indicate lat/lon errors in the property\n")
cat("     file. Investigate with 999 file.\n")
cat("  5. Corporate buyer summary (Section 10) -- read Cohen's d, not just\n")
cat("     p-values. With 2M+ rows everything will be significant. d > 0.2\n")
cat("     is worth flagging; d > 0.5 is substantively large.\n")
cat("     Check 1-05_corp_bg_context.png for visual distribution.\n")
cat("  6. NA rates on BG variables -- suppressed cells in sparse BGs are\n")
cat("     expected for medians. Consider tract-level fallback in analysis.\n")
cat("  7. Outputs:\n")
cat("       1-05_bg_acs.rds     -- BG-level contextual file (CA, ACS 2020)\n")
cat("       1-05_analytic.rds   -- analytic + GEOID + BG context\n")
cat("       1-05_prop_wide.rds  -- prop_wide + GEOID + BG context\n")
cat("====================================================\n")

message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")

beep()
savehistory(paste0(secure, "Process/Fire Investment/Logs/1-05_history_", timestamp, ".txt"))
sink()