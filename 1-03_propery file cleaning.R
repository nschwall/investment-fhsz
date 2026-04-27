

# **************************************************
#                     DETAILS
#
# Purpose:   Explore Cotality property3 file
#            Understand structure, missingness, key vars,
#            CLIP uniqueness, and join to OwnerTransfer
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2026-04-25
# Started:   MM/DD/YYYY
# Updated:   MM/DD/YYYY
# **************************************************


# *************
# 1. Setup ----

user   <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root   <- paste0(user, "Fire Investment/")
secure <- "Y:/Institutional Investment/"

data_input_s  <- paste0(secure, "Data/Source/")
transfer_root <- paste0(data_input_s, "Cotality/September 2025 Transfer")

start_time <- Sys.time()

cat("\n====================================================\n")
cat("  1-03_property_explore.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

# load packages
pkgs <- c("dplyr", "data.table", "R.utils")
invisible(lapply(pkgs, function(p) {
  if (!require(p, character.only = TRUE)) {
    install.packages(p)
    library(p, character.only = TRUE)
  }
}))
rm(pkgs)

# timestamp defined AFTER packages load to avoid R.utils masking
timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file  <- paste0(secure, "Process/Fire Investment/Logs/1-03_property_explore_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)

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
# 2. Discover property CSV ----
# *****************************************************
ts("Scanning for property CSV file...")

skip_dirs <- c("Zip Folders", "Meta Data")
subdirs   <- list.dirs(transfer_root, full.names = TRUE, recursive = FALSE)
subdirs   <- subdirs[!basename(subdirs) %in% skip_dirs]

# OwnerTransfer file (for reference / join validation later)
csv_transfer <- unlist(lapply(subdirs, function(d) {
  list.files(d,
             pattern = "^UNIVERSITY_OF_CALIFORNIA.*OwnerTransfer.*\\.csv$",
             full.names = TRUE, recursive = TRUE, ignore.case = TRUE)
}))
csv_transfer <- csv_transfer[!grepl("hist_", csv_transfer, ignore.case = TRUE)]
f_014500 <- csv_transfer[grepl("014500", csv_transfer)]
if (length(f_014500) == 0) stop("OwnerTransfer 014500 not found — check transfer_root path.")
cat(sprintf("OwnerTransfer 014500 found: %s\n", basename(f_014500)))

# Property file — search for property3 filename directly (same broad pattern as 1-01)
csv_all <- unlist(lapply(subdirs, function(d) {
  list.files(d, pattern = "^UNIVERSITY_OF_CALIFORNIA.*\\.csv$",
             full.names = TRUE, recursive = TRUE, ignore.case = TRUE)
}))
csv_property <- csv_all[grepl("property3", csv_all, ignore.case = TRUE)]

cat(sprintf("\nProperty3 CSV(s) found (%d):\n", length(csv_property)))
cat(paste0("  ", basename(csv_property), "\n"))

if (length(csv_property) == 0) stop("No property3 CSV found — check transfer_root path.")
if (length(csv_property) > 1) warning("Multiple property3 files found — using first. Confirm this is correct.")
f_property <- csv_property[1]
cat(sprintf("\nUsing: %s\n", basename(f_property)))


# *****************************************************
# 3. Peek — raw column names ----
# *****************************************************
ts("Peeking at property file (100 rows)...")

prop_peek <- as.data.frame(data.table::fread(
  f_property,
  nrows        = 100,
  na.strings   = c("", "NA", "N/A", "null"),
  showProgress = FALSE
))

cat(sprintf("  Peek: %d rows x %d columns\n", nrow(prop_peek), ncol(prop_peek)))
cat("\n  Raw column names:\n")
print(names(prop_peek))


# *****************************************************
# 4. Full load ----
# *****************************************************
ts("Loading full property file...")

prop <- as.data.frame(data.table::fread(
  f_property,
  na.strings   = c("", "NA", "N/A", "null"),
  showProgress = TRUE
))

prop <- normalize_names(prop)
prop <- convert_date_cols(prop)

cat(sprintf("  %s rows x %d columns\n",
            formatC(nrow(prop), format = "d", big.mark = ","),
            ncol(prop)))
cat(sprintf("  Columns: %s\n", paste(names(prop), collapse = ", ")))

gc()
ts("  gc() done")

# Filter to SFR before NA sweep so missingness reflects analytic universe
ts("Filtering to SFR (property_indicator_code == '10')...")

cat(sprintf("  Rows before SFR filter: %s\n", formatC(nrow(prop), format = "d", big.mark = ",")))
prop <- prop[!is.na(prop$property_indicator_code) & prop$property_indicator_code == "10", ]
cat(sprintf("  Rows after SFR filter:  %s\n", formatC(nrow(prop), format = "d", big.mark = ",")))

gc()
ts("  gc() done")

# NA count per column — informational only, no columns dropped here
ts("NA sweep...")

na_counts <- sapply(prop, function(x) sum(is.na(x) | x == "" | (is.numeric(x) & x == 0)))
na_pcts   <- 100 * na_counts / nrow(prop)

na_summary <- data.frame(
  column   = names(na_pcts),
  n_miss   = na_counts,
  pct_miss = round(na_pcts, 1),
  row.names = NULL
)

cat("\n  NA% per column (sorted):\n")
print(na_summary[order(na_summary$pct_miss, decreasing = TRUE), ], row.names = FALSE)

# No columns dropped here — review output and decide manually which to exclude


# *****************************************************
# 5. CLIP — uniqueness check ----
# *****************************************************
ts("Checking CLIP uniqueness...")

cat(sprintf("  clip present:    %s\n", "clip" %in% names(prop)))
cat(sprintf("  Total rows:      %s\n", formatC(nrow(prop), format = "d", big.mark = ",")))
cat(sprintf("  Unique CLIPs:    %s\n", formatC(data.table::uniqueN(prop$clip), format = "d", big.mark = ",")))
cat(sprintf("  CLIP is unique:  %s\n", data.table::uniqueN(prop$clip) == nrow(prop)))
cat(sprintf("  CLIP NA:         %s\n", formatC(sum(is.na(prop$clip)), format = "d", big.mark = ",")))
cat(sprintf("  CLIP empty str:  %s\n", formatC(sum(prop$clip == "", na.rm = TRUE), format = "d", big.mark = ",")))

if (data.table::uniqueN(prop$clip) < nrow(prop)) {
  cat("\n*** CLIP NOT UNIQUE — diagnosing ***\n")
  
  dt_prop   <- data.table::as.data.table(prop)
  dup_clips <- dt_prop[, .N, by = clip][N > 1]
  cat(sprintf("  CLIPs with >1 row:    %s\n", formatC(nrow(dup_clips), format = "d", big.mark = ",")))
  cat(sprintf("  Extra duplicate rows: %s\n", formatC(sum(dup_clips$N) - nrow(dup_clips), format = "d", big.mark = ",")))
  
  exact_dupes <- prop[duplicated(prop) | duplicated(prop, fromLast = TRUE), ]
  cat(sprintf("  Exact duplicate rows: %s\n", formatC(nrow(exact_dupes), format = "d", big.mark = ",")))
  
  # Record action indicator among dupes — delta file A/U/D logic?
  if ("record_action_indicator" %in% names(prop)) {
    cat("\n  Record action indicator among duplicate CLIPs:\n")
    print(table(prop$record_action_indicator[prop$clip %in% dup_clips$clip], useNA = "always"))
  }
  
  cat("\n  Sample duplicate CLIP rows:\n")
  sample_clips <- head(dup_clips$clip, 5)
  sub_dup <- prop[prop$clip %in% sample_clips, ]
  print(sub_dup[order(sub_dup$clip), ])
  
  # Resolution options — decide after inspecting output:
  # A: drop exact dupes (keep first)
  # B: keep row with non-missing parcel lat/lon
  # C: use record_action_indicator to resolve (U supersedes A; D = delete)
}

cat("\n  Sample CLIP values:\n")
print(head(prop$clip, 20))


# *****************************************************
# 6. California filter ----
# *****************************************************
ts("Checking California coverage...")

if ("situs_state" %in% names(prop)) {
  cat("  State distribution (top 10):\n")
  print(sort(table(prop$situs_state, useNA = "always"), decreasing = TRUE)[1:10])
  n_ca <- sum(prop$situs_state == "CA", na.rm = TRUE)
  cat(sprintf("\n  CA rows (situs_state == 'CA'): %s (%.1f%%)\n",
              formatC(n_ca, format = "d", big.mark = ","),
              100 * n_ca / nrow(prop)))
}

if ("fips_code" %in% names(prop)) {
  n_ca_fips <- sum(startsWith(as.character(prop$fips_code), "06"), na.rm = TRUE)
  cat(sprintf("  CA rows (FIPS starts '06'):    %s (%.1f%%)\n",
              formatC(n_ca_fips, format = "d", big.mark = ","),
              100 * n_ca_fips / nrow(prop)))
}


# *****************************************************
# 7. Property indicator code — SFR check ----
# *****************************************************
ts("Checking property indicator code...")

if ("property_indicator_code" %in% names(prop)) {
  cat("  Distribution:\n")
  print(sort(table(prop$property_indicator_code, useNA = "always"), decreasing = TRUE))
  cat(sprintf("\n  SFR ('10') count: %s\n",
              formatC(sum(prop$property_indicator_code == "10", na.rm = TRUE), format = "d", big.mark = ",")))
  cat(sprintf("  SFR share:        %.1f%%\n",
              100 * mean(prop$property_indicator_code == "10", na.rm = TRUE)))
}


# *****************************************************
# 8. Lat/lon coverage ----
# *****************************************************
ts("Checking lat/lon coverage...")

latlon_cols <- grep("latitude|longitude", names(prop), value = TRUE, ignore.case = TRUE)
cat(sprintf("  Lat/lon columns: %s\n\n", paste(latlon_cols, collapse = ", ")))

for (col in latlon_cols) {
  n_miss <- sum(is.na(prop[[col]]) | prop[[col]] == 0)
  vals   <- prop[[col]][!is.na(prop[[col]]) & prop[[col]] != 0]
  cat(sprintf("  %-40s  missing/zero: %s (%.1f%%)  range: [%.4f, %.4f]\n",
              col,
              formatC(n_miss, format = "d", big.mark = ","),
              100 * n_miss / nrow(prop),
              ifelse(length(vals) > 0, min(vals), NA),
              ifelse(length(vals) > 0, max(vals), NA)))
}

cat("\n  CA bounds: lat [32, 42], lon [-124, -114]\n")
cat("  Prefer parcel-level for FHSZ spatial join; fall back to block-level\n")
cat("  CRS: WGS84 (EPSG:4326) — no reprojection needed on property side\n")

# Parcel vs block coverage overlap
p_lat <- grep("parcel.*lat", names(prop), value = TRUE)[1]
b_lat <- grep("block.*lat",  names(prop), value = TRUE)[1]

if (!is.na(p_lat) & !is.na(b_lat)) {
  has_parcel <- !is.na(prop[[p_lat]]) & prop[[p_lat]] != 0
  has_block  <- !is.na(prop[[b_lat]]) & prop[[b_lat]] != 0
  cat(sprintf("\n  Parcel only:  %s\n", formatC(sum( has_parcel & !has_block), format = "d", big.mark = ",")))
  cat(sprintf("  Block only:   %s\n", formatC(sum(!has_parcel &  has_block), format = "d", big.mark = ",")))
  cat(sprintf("  Both:         %s\n", formatC(sum( has_parcel &  has_block), format = "d", big.mark = ",")))
  cat(sprintf("  Neither:      %s\n", formatC(sum(!has_parcel & !has_block), format = "d", big.mark = ",")))
}


# *****************************************************
# 9. Key analytic variables — missingness ----
# *****************************************************
ts("Missingness sweep: key analytic variables...")

# Columns we expect to keep — review against Section 5 full NA sweep before finalizing
key_vars <- c(
  "clip",
  "fips_code",
  "apn_parcel_number_unformatted",
  "property_indicator_code",
  "situs_state",
  "situs_county",
  "situs_city",
  "situs_zip_code",
  "parcel_level_latitude",
  "parcel_level_longitude",
  "building_gross_area_square_feet",
  "building_quality_code",
  "building_improvement_condition_code",
  "total_number_of_bedrooms_all_buildings",
  "total_number_of_bathrooms",
  "year_built",
  "effective_year_built",
  "total_number_of_acres",
  "calculated_total_value",
  "calculated_total_value_source_code",
  "assessed_total_value",
  "market_total_value",
  "owner_occupancy_code",
  "owner_1_corporate_indicator"
)

actual_cols <- names(prop)

for (v in key_vars) {
  col <- actual_cols[grepl(v, actual_cols, fixed = TRUE)]
  if (length(col) == 0) col <- actual_cols[grepl(v, actual_cols, ignore.case = TRUE)]
  if (length(col) == 0) {
    cat(sprintf("  %-50s  NOT FOUND\n", v))
    next
  }
  col   <- col[1]
  vals  <- prop[[col]]
  is_numeric <- is.numeric(vals)
  n_miss <- if (is_numeric) {
    sum(is.na(vals) | vals == 0)
  } else {
    sum(is.na(vals) | vals == "")
  }
  cat(sprintf("  %-50s  %s missing (%.1f%%)\n",
              col,
              formatC(n_miss, format = "d", big.mark = ","),
              100 * n_miss / nrow(prop)))
}

# *** STOP HERE — review Section 5 (all columns) and Section 10 (key columns)
# *** before proceeding. Decide which columns to drop, update keep_cols_slim below.


# *****************************************************
# 10. Subset to analytic columns ----
# *****************************************************
ts("Subsetting to analytic columns...")

# TODO: remove any columns from this list based on missingness review above
keep_cols_slim <- c(
  "clip",
  "fips_code",
  "apn_parcel_number_unformatted",
  "property_indicator_code",
  "situs_state",
  "situs_county",
  "situs_city",
  "situs_zip_code",
  "parcel_level_latitude",
  "parcel_level_longitude",
  "building_gross_area_square_feet",
  "building_quality_code",
  "building_improvement_condition_code",
  "total_number_of_bedrooms_all_buildings",
  "total_number_of_bathrooms",
  "year_built",
  "effective_year_built",
  "total_number_of_acres",
  "calculated_total_value",
  "calculated_total_value_source_code",
  "assessed_total_value",
  "market_total_value",
  "owner_occupancy_code",
  "owner_1_corporate_indicator"
)

# check all requested cols exist
missing_slim <- keep_cols_slim[!keep_cols_slim %in% names(prop)]
if (length(missing_slim) > 0) {
  cat("  WARNING — columns not found, dropping from keep list:\n")
  cat(paste0("    ", missing_slim, "\n"))
  keep_cols_slim <- keep_cols_slim[keep_cols_slim %in% names(prop)]
}

prop_slim <- prop[, keep_cols_slim, drop = FALSE]

cat(sprintf("  prop_slim: %s rows x %d columns\n",
            formatC(nrow(prop_slim), format = "d", big.mark = ","),
            ncol(prop_slim)))
cat(sprintf("  Columns: %s\n", paste(names(prop_slim), collapse = ", ")))


# *****************************************************
# 11. Value field comparison ----
# *****************************************************
ts("Comparing value fields...")

tv_col <- actual_cols[grepl("calculated_total_value$", actual_cols)][1]
av_col <- actual_cols[grepl("assessed_total_value",    actual_cols)][1]
mv_col <- actual_cols[grepl("market_total_value",      actual_cols)][1]

if (!is.na(tv_col) & !is.na(av_col) & !is.na(mv_col)) {
  has_tv <- !is.na(prop[[tv_col]]) & prop[[tv_col]] > 0
  has_av <- !is.na(prop[[av_col]]) & prop[[av_col]] > 0
  has_mv <- !is.na(prop[[mv_col]]) & prop[[mv_col]] > 0
  
  cat(sprintf("  total_value_calculated non-zero: %s (%.1f%%)\n",
              formatC(sum(has_tv), format = "d", big.mark = ","), 100 * mean(has_tv)))
  cat(sprintf("  assessed_total_value   non-zero: %s (%.1f%%)\n",
              formatC(sum(has_av), format = "d", big.mark = ","), 100 * mean(has_av)))
  cat(sprintf("  market_total_value     non-zero: %s (%.1f%%)\n",
              formatC(sum(has_mv), format = "d", big.mark = ","), 100 * mean(has_mv)))
  
  cat(sprintf("\n  All three populated:   %s\n", formatC(sum( has_tv &  has_av &  has_mv), format = "d", big.mark = ",")))
  cat(sprintf("  Only total_value_calc: %s\n",  formatC(sum( has_tv & !has_av & !has_mv), format = "d", big.mark = ",")))
  cat(sprintf("  Only assessed:         %s\n",  formatC(sum(!has_tv &  has_av & !has_mv), format = "d", big.mark = ",")))
  cat(sprintf("  Only market:           %s\n",  formatC(sum(!has_tv & !has_av &  has_mv), format = "d", big.mark = ",")))
  cat(sprintf("  None populated:        %s\n",  formatC(sum(!has_tv & !has_av & !has_mv), format = "d", big.mark = ",")))
  
  src_col <- actual_cols[grepl("calculated_total_value_source_code", actual_cols)][1]
  if (!is.na(src_col)) {
    cat("\n  Calculated value source (A=Assessed, M=Market, P=Appraised, T=Transitional):\n")
    print(table(prop[[src_col]], useNA = "always"))
  }
}


# *****************************************************
# 12. Join validation to OwnerTransfer 014500 ----
# *****************************************************

# TODO: uncomment once ot_clean is available from 1-02
# (or load a slim version of 014500 here just for CLIP matching)
#
# ts("Join validation: property CLIP vs OwnerTransfer 014500 CLIP...")
#
# ot_clips   <- unique(ot_clean$clip)
# prop_clips <- unique(prop$clip[!is.na(prop$clip)])
#
# cat(sprintf("  OwnerTransfer unique CLIPs: %s\n", formatC(length(ot_clips),   format = "d", big.mark = ",")))
# cat(sprintf("  Property unique CLIPs:      %s\n", formatC(length(prop_clips), format = "d", big.mark = ",")))
# cat(sprintf("  OT clips in property:       %s\n", formatC(sum(ot_clips %in% prop_clips), format = "d", big.mark = ",")))
# cat(sprintf("  Match rate:                 %.2f%%\n", 100 * mean(ot_clips %in% prop_clips)))
# cat(sprintf("  Unmatched OT clips:         %s\n", formatC(sum(!ot_clips %in% prop_clips), format = "d", big.mark = ",")))
#
# # Are unmatched clips systematically older?
# ot_clean <- ot_clean %>% mutate(prop_matched = clip %in% prop_clips)
# cat("\n  Sale year x prop_matched:\n")
# print(table(lubridate::year(ot_clean$sale_derived_date), ot_clean$prop_matched))


# *****************************************************
# 13. Save ----
# *****************************************************
ts("Saving SFR property file (slim columns)...")

data_derived <- "Y:/Institutional Investment/Data/Derived/"
out_path <- paste0(data_derived, "1-03_property_sfr.rds")
saveRDS(prop_slim, out_path)
cat(sprintf("  Saved -> %s
", out_path))
cat(sprintf("  Rows: %s  |  Columns: %d\n",
            formatC(nrow(prop_slim), format = "d", big.mark = ","), ncol(prop_slim)))


# ******************************
# 14. Close out ----

cat("\n====================================================\n")
cat("  Decisions needed after reviewing output:\n")
cat("  1. CLIP unique? If not, resolve (Section 6)\n")
cat("  2. Parcel-level lat/lon coverage — see Section 9\n")
cat("  3. Value field to use — see source code dist (Section 11)\n")
cat("  4. Run Section 12 join validation once ot_clean available\n")
cat("  5. Output saved: 1-03_property_sfr.rds\n")
cat("====================================================\n")

message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")

savehistory(paste0(secure, "Process/Fire Investment/Logs/1-03_property_explore_history_", timestamp, ".txt"))

