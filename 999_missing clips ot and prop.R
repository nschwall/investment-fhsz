

# **************************************************
#                     DETAILS
#
# Purpose:   Investigate OT CLIPs that did not match the cleaned SFR
#            property file (1-03_property_sfr.rds). Check whether those
#            CLIPs exist in the *original* raw property3 file before the
#            SFR filter — i.e., were they dropped by our cleaning steps,
#            or were they never in the property file at all?
#
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2026-04-28
# Started:   MM/DD/YYYY
# Updated:   MM/DD/YYYY
#
# ---- INPUTS ----
#
#   1-04_unmatched_clips.rds  — OT rows with no match in prop_slim (from 1-04)
#   raw property3 CSV         — same source file loaded in 1-03, loaded here
#                               with a targeted column select (not all 220 cols)
#
# ---- QUESTION ----
#
#   For each unmatched clip_id:
#     A) In raw property file (pre SFR filter)?  -> dropped by our cleaning
#     B) Not in raw property file at all?        -> genuinely missing from Cotality
#
# ---- OUTPUTS ----
#
#   unmatched_in_raw   — in-memory df: unmatched clips found in raw property file
#                        with key raw property columns for inspection
#   (no file saved — review in session)
# **************************************************


# *************
# 1. Setup ----

user   <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root   <- paste0(user, "Fire Investment/")
secure <- "Y:/Institutional Investment/"

data_output_s <- paste0(secure, "Data/Derived/Fire Investment/")
data_input_s  <- paste0(secure, "Data/Source/")
transfer_root <- paste0(data_input_s, "Cotality/September 2025 Transfer")

start_time <- Sys.time()
set.seed(123)

cat("\n====================================================\n")
cat("  999_unmatched_clip_investigation.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

pkgs <- c("dplyr", "data.table")
invisible(lapply(pkgs, function(p) {
  if (!require(p, character.only = TRUE)) {
    install.packages(p)
    library(p, character.only = TRUE)
  }
}))
rm(pkgs)

ts <- function(label = "") {
  cat(sprintf("\n[%s]  %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), label))
}

# helper: normalize column names to lowercase + underscores
normalize_names <- function(df) {
  new_names <- tolower(names(df))
  new_names <- gsub("[^a-z0-9]+", "_", new_names)
  new_names <- gsub("_+", "_", new_names)
  new_names <- sub("^_", "", new_names)
  new_names <- sub("_$", "", new_names)
  if (any(duplicated(new_names))) stop("Name collision after normalization.")
  names(df) <- new_names
  df
}

# helper: convert YYYYMMDD columns to Date
convert_date_cols <- function(df) {
  date_cols <- names(df)[grepl("date", names(df), ignore.case = TRUE)]
  for (col in date_cols) {
    sample_vals <- na.omit(as.character(df[[col]]))
    looks_yyyymmdd <- length(sample_vals) > 0 &&
      all(nchar(sample_vals[1:min(10, length(sample_vals))]) == 8)
    if (looks_yyyymmdd) {
      df[[col]] <- as.Date(as.character(df[[col]]), format = "%Y%m%d")
      cat(sprintf("  Converted %s -> Date\n", col))
    }
  }
  df
}


# *****************************************************
# 2. Load unmatched clips (from 1-04) ----
# *****************************************************
ts("Loading unmatched clips...")

unmatched_df <- readRDS(paste0(data_output_s, "1-04_unmatched_clips.rds"))

cat(sprintf("  Unmatched OT rows:          %s\n", formatC(nrow(unmatched_df),                    format = "d", big.mark = ",")))
cat(sprintf("  Unique unmatched clip_ids:  %s\n", formatC(length(unique(unmatched_df$clip_id)),  format = "d", big.mark = ",")))

unmatched_clips <- unique(unmatched_df$clip_id)

cat("\n  Preview of unmatched_df:\n")
print(head(unmatched_df, 20))


# *****************************************************
# 3. Locate raw property3 CSV ----
# *****************************************************
ts("Locating raw property3 CSV...")

skip_dirs <- c("Zip Folders", "Meta Data")
subdirs   <- list.dirs(transfer_root, full.names = TRUE, recursive = FALSE)
subdirs   <- subdirs[!basename(subdirs) %in% skip_dirs]

csv_all      <- unlist(lapply(subdirs, function(d) {
  list.files(d, pattern = "^UNIVERSITY_OF_CALIFORNIA.*\\.csv$",
             full.names = TRUE, recursive = TRUE, ignore.case = TRUE)
}))
csv_property <- csv_all[grepl("property3", csv_all, ignore.case = TRUE)]
csv_property <- csv_property[!grepl("hist_", csv_property, ignore.case = TRUE)]

if (length(csv_property) == 0) stop("No property3 CSV found — check transfer_root path.")
if (length(csv_property) > 1)  stop("Multiple non-hist property3 files found — check transfer_root path.")
f_property <- csv_property[1]
cat(sprintf("  Using: %s\n", basename(f_property)))


# *****************************************************
# 4. Peek — identify column names ----
# *****************************************************
ts("Peeking at property file to confirm column names...")

prop_peek <- as.data.frame(data.table::fread(
  f_property,
  nrows        = 5,
  na.strings   = c("", "NA", "N/A", "null"),
  showProgress = FALSE
))
prop_peek <- normalize_names(prop_peek)
cat(sprintf("  Column count: %d\n", ncol(prop_peek)))

# Columns to load — everything we used in 1-03 for cleaning/filtering/keeping,
# plus the raw indicator columns we dropped after recoding.
# Do NOT load all 220 columns.
target_cols <- c(
  # join key
  "clip",
  # used for SFR filter (we want to know if unmatched clips were filtered out here)
  "property_indicator_code",
  # geography — used in 1-03 Section 6 CA check and kept in prop_slim
  "fips_code",
  "apn_parcel_number_unformatted",
  "situs_state",
  "situs_county",
  "situs_city",
  "situs_zip_code",
  "situs_street_address",
  "municipality_name",
  # spatial — kept in prop_slim (parcel backfilled from block in 1-03)
  "parcel_level_latitude",
  "parcel_level_longitude",
  "block_level_latitude",
  "block_level_longitude",
  # structure — kept in prop_slim
  "total_living_area_square_feet_all_buildings",
  "building_quality_code",
  "construction_type_code",
  "total_number_of_bedrooms_all_buildings",
  "total_number_of_bathrooms_all_buildings",
  "total_number_of_stories",
  "year_built",
  "effective_year_built",       # used in 1-03 backfill, then dropped
  "total_number_of_acres",
  # value — kept in prop_slim
  "calculated_total_value",
  "calculated_total_value_source_code",
  "assessed_total_value",       # used in 1-03 Section 12 comparison, then dropped
  "market_total_value",         # used in 1-03 Section 12 comparison, then dropped
  # owner — kept in prop_slim (after recode)
  "owner_occupancy_code",
  "homestead_exempt_indicator", # raw; recoded to homestead_exempt in 1-03
  # manufactured home — kept in prop_slim (after recode)
  "manufactured_home_indicator" # raw; recoded to manufactured_home in 1-03
)

# Confirm which target cols actually exist in this file
actual_cols <- names(prop_peek)
load_cols   <- target_cols[target_cols %in% actual_cols]
missing_cols <- target_cols[!target_cols %in% actual_cols]

if (length(missing_cols) > 0) {
  cat(sprintf("  Columns not found in raw file (%d): %s\n",
              length(missing_cols), paste(missing_cols, collapse = ", ")))
}
cat(sprintf("  Loading %d of %d target columns.\n", length(load_cols), length(target_cols)))


# *****************************************************
# 5. Load raw property file — targeted columns only ----
# *****************************************************
ts("Loading raw property file (targeted columns only — not all 220)...")

prop_raw <- as.data.frame(data.table::fread(
  f_property,
  select       = load_cols,
  na.strings   = c("", "NA", "N/A", "null"),
  showProgress = TRUE
))
prop_raw <- normalize_names(prop_raw)
prop_raw <- convert_date_cols(prop_raw)

cat(sprintf("  Loaded: %s rows x %d columns\n",
            formatC(nrow(prop_raw), format = "d", big.mark = ","), ncol(prop_raw)))

gc()
ts("  gc() done")


# *****************************************************
# 6. Check: are unmatched CLIPs in the raw file? ----
# *****************************************************
ts("Checking unmatched CLIPs against raw property file...")

n_unmatched_clips <- length(unmatched_clips)

in_raw     <- unmatched_clips[unmatched_clips %in% prop_raw$clip]
not_in_raw <- unmatched_clips[!unmatched_clips %in% prop_raw$clip]

cat(sprintf("  Unmatched unique CLIPs:               %s\n", formatC(n_unmatched_clips, format = "d", big.mark = ",")))
cat(sprintf("  Found in raw property file:           %s (%.1f%%)\n",
            formatC(length(in_raw),     format = "d", big.mark = ","),
            100 * length(in_raw)     / n_unmatched_clips))
cat(sprintf("  NOT in raw property file at all:      %s (%.1f%%)\n",
            formatC(length(not_in_raw), format = "d", big.mark = ","),
            100 * length(not_in_raw) / n_unmatched_clips))


# *****************************************************
# 7. For CLIPs found in raw: why were they excluded? ----
# *****************************************************
ts("Diagnosing CLIPs found in raw file...")

unmatched_in_raw <- prop_raw[prop_raw$clip %in% in_raw, ]

cat(sprintf("  Rows in raw for these CLIPs: %s\n",
            formatC(nrow(unmatched_in_raw), format = "d", big.mark = ",")))

# Key question: what property_indicator_code are they?
# If not "10" -> they were filtered out by the SFR filter in 1-03
cat("\n  property_indicator_code distribution (unmatched CLIPs found in raw):\n")
print(table(unmatched_in_raw$property_indicator_code, useNA = "always"))

n_sfr     <- sum(unmatched_in_raw$property_indicator_code == "10", na.rm = TRUE)
n_non_sfr <- sum(unmatched_in_raw$property_indicator_code != "10", na.rm = TRUE)
n_pic_na  <- sum(is.na(unmatched_in_raw$property_indicator_code))

cat(sprintf("\n  In raw AND coded SFR (pic == '10'):     %s — these SHOULD have matched; investigate further\n",
            formatC(n_sfr, format = "d", big.mark = ",")))
cat(sprintf("  In raw AND non-SFR (filtered in 1-03): %s — expected, filtered by design\n",
            formatC(n_non_sfr, format = "d", big.mark = ",")))
cat(sprintf("  In raw AND pic missing:                 %s\n",
            formatC(n_pic_na, format = "d", big.mark = ",")))

# State distribution — were any non-CA slipping through?
if ("situs_state" %in% names(unmatched_in_raw)) {
  cat("\n  situs_state distribution (unmatched CLIPs found in raw):\n")
  print(sort(table(unmatched_in_raw$situs_state, useNA = "always"), decreasing = TRUE)[1:10])
}

# If any are SFR in raw, pull them separately for closer inspection
if (n_sfr > 0) {
  unmatched_sfr_in_raw <- unmatched_in_raw[
    !is.na(unmatched_in_raw$property_indicator_code) &
      unmatched_in_raw$property_indicator_code == "10", ]
  cat(sprintf("\n  'unmatched_sfr_in_raw' available in environment: %s rows\n",
              formatC(nrow(unmatched_sfr_in_raw), format = "d", big.mark = ",")))
  cat("  These were SFR in the raw file but didn't make it into prop_slim — investigate.\n")
  cat("\n  Head of unmatched_sfr_in_raw:\n")
  print(head(unmatched_sfr_in_raw, 20))
}

cat(sprintf("\n  'unmatched_in_raw' available in environment: %s rows x %d columns\n",
            formatC(nrow(unmatched_in_raw), format = "d", big.mark = ","),
            ncol(unmatched_in_raw)))
cat("  Join unmatched_df to unmatched_in_raw on clip_id == clip for full picture.\n")


# *****************************************************
# 8. CLIPs not in raw at all ----
# *****************************************************
if (length(not_in_raw) > 0) {
  ts("Reviewing CLIPs not found in raw property file at all...")
  
  not_in_raw_df <- unmatched_df[unmatched_df$clip_id %in% not_in_raw, ]
  
  cat(sprintf("  %s unique CLIPs not in raw property file.\n",
              formatC(length(not_in_raw), format = "d", big.mark = ",")))
  cat("  These transactions refer to parcels Cotality has no property record for.\n")
  cat("\n  Sale year distribution:\n")
  print(table(format(not_in_raw_df$sale_date, "%Y"), useNA = "always"))
  cat("\n  buy1_corp distribution:\n")
  print(table(not_in_raw_df$buy1_corp, useNA = "always"))
  cat("\n  Head:\n")
  print(head(not_in_raw_df, 20))
  cat(sprintf("\n  'not_in_raw_df' available in environment: %s rows\n",
              formatC(nrow(not_in_raw_df), format = "d", big.mark = ",")))
}




# *****************************************************
# 9. ADDED ----
# *****************************************************

# Quick check: are unmatched CLIPs in the March 2026 property file?

library(data.table)

secure        <- "Y:/Institutional Investment/"
data_output_s <- paste0(secure, "Data/Derived/Fire Investment/")

f_march <- "Y:/Institutional Investment/Data/Source/Cotality/March 2026 Transfer/UNIVERSITY_OF_CALIFORNIA_SAN_DIEGO_property3_dpc_01930334_20260309_014507_data/UNIVERSITY_OF_CALIFORNIA_SAN_DIEGO_property3_dpc_01930334_20260309_014507_data.csv"

# load unmatched clips
unmatched_df    <- readRDS(paste0(data_output_s, "1-04_unmatched_clips.rds"))
unmatched_clips <- unique(unmatched_df$clip_id)
cat(sprintf("Unmatched CLIPs to check: %s\n", formatC(length(unmatched_clips), format = "d", big.mark = ",")))

# load only clip column from March 2026 file
cat("Loading March 2026 clip column...\n")
march_clips <- fread(f_march, select = "CLIP", na.strings = c("", "NA"), showProgress = TRUE)[[1]]
cat(sprintf("March 2026 property rows: %s\n", formatC(length(march_clips), format = "d", big.mark = ",")))

# check
n_found <- sum(unmatched_clips %in% march_clips)
n_still_missing <- sum(!unmatched_clips %in% march_clips)

cat(sprintf("\nUnmatched CLIPs found in March 2026 file:     %s (%.1f%%)\n",
            formatC(n_found,         format = "d", big.mark = ","), 100 * n_found         / length(unmatched_clips)))
cat(sprintf("Still not in March 2026 file:                  %s (%.1f%%)\n",
            formatC(n_still_missing, format = "d", big.mark = ","), 100 * n_still_missing / length(unmatched_clips)))


# *****************************************************
# 9. Summary ----
# *****************************************************
cat("\n====================================================\n")
cat("  Summary\n")
cat("====================================================\n")
cat(sprintf("  Total unmatched unique CLIPs:           %s\n", formatC(n_unmatched_clips, format = "d", big.mark = ",")))
cat(sprintf("  Found in raw file (any property type):  %s (%.1f%%)\n",
            formatC(length(in_raw),     format = "d", big.mark = ","), 100 * length(in_raw)     / n_unmatched_clips))
if (exists("n_sfr"))     cat(sprintf("    of which SFR in raw (pic == 10):    %s — unexpected, investigate\n", formatC(n_sfr,     format = "d", big.mark = ",")))
if (exists("n_non_sfr")) cat(sprintf("    of which non-SFR (filtered ok):    %s\n",                           formatC(n_non_sfr, format = "d", big.mark = ",")))
cat(sprintf("  Not in raw file at all:                 %s (%.1f%%)\n",
            formatC(length(not_in_raw), format = "d", big.mark = ","), 100 * length(not_in_raw) / n_unmatched_clips))
cat("\n  Objects in environment for exploration:\n")
cat("    unmatched_df         — all unmatched OT rows (from 1-04)\n")
cat("    unmatched_in_raw     — subset of raw property file for matched CLIPs\n")
if (exists("unmatched_sfr_in_raw")) cat("    unmatched_sfr_in_raw — the SFR-coded ones that still didn't match\n")
if (exists("not_in_raw_df"))        cat("    not_in_raw_df        — OT rows whose CLIPs aren't in raw property at all\n")
cat("====================================================\n")

message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")



