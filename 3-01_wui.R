

# **************************************************
#                     DETAILS
#
# Purpose:   Load California WUI block-level change shapefile
#            (1990-2020), verify and reproject CRS to match
#            project standard (CA Albers EPSG:3310), and
#            explore structure for downstream join to property
#            and insurance panel data.
#
#            Source: Radeloff et al. WUI change dataset,
#            CA_wui_block_1990_2020_change_v4
#            Unit: Census block
#            Years: 1990, 2000, 2010, 2020 (change classifications)
#
#            CRS note: property spatial joins in 1-05 used CA Albers
#            (EPSG:3310). WUI shapefile CRS verified at runtime and
#            reprojected to EPSG:3310 if needed before any joins.
#
# Inputs:    Data/Source/WUI/CA_wui_block_1990_2020_change_v4/
#              CA_wui_block_1990_2020_change_v4.shp
#
# Outputs:   [TBD -- to be determined once structure is understood]
#
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2026-04-29
# Started:   MM/DD/YYYY
# Updated:   MM/DD/YYYY
# **************************************************


# *************
# 1. Setup ----

user   <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root   <- paste0(user, "Fire Investment/")
secure <- "Y:/Institutional Investment/"

data_wui     <- paste0(root, "Data/Source/WUI/CA_wui_block_1990_2020_change_v4/")
data_derived <- paste0(root, "Data/Derived/WUI/")
data_output_s <- paste0(secure, "Data/Derived/Fire Investment/")

dir.create(data_derived, showWarnings = FALSE, recursive = TRUE)

timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file  <- paste0(root, "Process/Logs/3-01_log_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)

start_time <- Sys.time()
set.seed(123)

cat("\n====================================================\n")
cat("  3-01_wui.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

pkgs <- c("sf", "dplyr")
invisible(lapply(pkgs, function(p) {
  if (!require(p, character.only = TRUE)) {
    install.packages(p); library(p, character.only = TRUE)
  }
}))
rm(pkgs)

ts <- function(label = "") {
  cat(sprintf("\n[%s]  %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), label))
}


# *****************************************************
# 2. Load WUI shapefile ----
# *****************************************************

ts("Loading WUI shapefile...")

wui_raw <- sf::st_read(paste0(data_wui, "CA_wui_block_1990_2020_change_v4.shp"))

cat(sprintf("\n  Rows: %s\n",    formatC(nrow(wui_raw), format = "d", big.mark = ",")))
cat(sprintf("  Columns: %d\n",  ncol(wui_raw)))
cat(sprintf("  Names: %s\n",    paste(names(wui_raw), collapse = ", ")))
cat(sprintf("  Geometry type: %s\n", as.character(sf::st_geometry_type(wui_raw)[1])))

cat("\n--- First few rows ---\n")
print(head(wui_raw))

cat("\n--- Column types ---\n")
print(str(sf::st_drop_geometry(wui_raw)))


# *****************************************************
# 3. CRS check and reproject ----
# *****************************************************
# Project standard: CA Albers EPSG:3310, used in 1-05 for all
# spatial joins. Reproject WUI to match before any joins.

ts("Checking and reprojecting CRS...")

wui_crs <- sf::st_crs(wui_raw)
cat(sprintf("  Source CRS: EPSG:%s  |  %s\n", wui_crs$epsg, wui_crs$input))

if (isTRUE(wui_crs$epsg == 3310)) {
  cat("  Already in CA Albers (EPSG:3310) -- no reprojection needed.\n")
  wui <- wui_raw
} else {
  cat(sprintf("  Reprojecting from EPSG:%s to EPSG:3310 (CA Albers)...\n", wui_crs$epsg))
  wui <- sf::st_transform(wui_raw, crs = 3310)
  cat(sprintf("  Reprojected CRS: EPSG:%s\n", sf::st_crs(wui)$epsg))
}

# Bounding box sanity check -- CA Albers metres expected
bb <- sf::st_bbox(wui)
cat("\n  Bounding box after reproject (should be metres, not degrees):\n")
cat(sprintf("    xmin: %12.1f   xmax: %12.1f   width:  %10.1f\n",
            bb["xmin"], bb["xmax"], bb["xmax"] - bb["xmin"]))
cat(sprintf("    ymin: %12.1f   ymax: %12.1f   height: %10.1f\n",
            bb["ymin"], bb["ymax"], bb["ymax"] - bb["ymin"]))

if (abs(bb["xmax"] - bb["xmin"]) < 1000) {
  stop("Bounding box width < 1000 -- CRS transform likely failed. Inspect st_crs(wui).")
} else {
  cat("  Bounding box width > 1000 -- confirmed metres.\n")
}

rm(wui_raw, wui_crs, bb)


# *****************************************************
# 4. [PLACEHOLDER] -- explore structure, decide join key
# *****************************************************
# Fill in once column names and WUI classification scheme are understood.
# Expected:
#   - Census block GEOID or FIPS as join key to property data
#   - WUI class columns for each decade (1990, 2000, 2010, 2020)
#   - Change classification column(s)
#
# Questions to answer from head() / str() output above:
#   1. What is the join key column name?
#   2. What do the WUI class codes mean? (interface / intermix / non-WUI)
#   3. Is there a single "change" column or separate columns per decade?
#   4. What is the geographic unit -- block or block group?


# *****************************************************
# 5. Save ----
# *****************************************************

ts("Saving outputs...")

# Save reprojected WUI as RDS for faster loading in downstream scripts
out_wui <- paste0(data_derived, "3-01_wui_3310.rds")
saveRDS(wui, out_wui)
cat(sprintf("  wui_3310 -> %s  (%s rows x %d cols)\n",
            out_wui,
            formatC(nrow(wui), format = "d", big.mark = ","),
            ncol(wui)))


# *****************************************************
# 6. Close out ----
# *****************************************************

cat("\n\n")
cat(strrep("=", 70), "\n")
cat("  Outputs:\n")
cat(sprintf("    3-01_wui_3310.rds  -- WUI shapefile reprojected to EPSG:3310\n"))
cat("\n  Next steps:\n")
cat("    1. Review column names and WUI class scheme from Section 2 output\n")
cat("    2. Identify join key (block GEOID?) for property data join\n")
cat("    3. Decide which WUI columns to carry forward (2020 only? change?)\n")
cat("    4. Build join to 2-04_analytic.rds or 2-04_prop_wide.rds\n")
cat(strrep("=", 70), "\n")

message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")

savehistory(paste0(root, "Process/Logs/3-01_history_", timestamp, ".txt"))
sink()