


# **************************************************
#                     DETAILS
#
# Purpose:   Load and clean Cotality OwnerTransfer file (014500, March 2026 pull).
#            Apply all primary sample restrictions.
#            Output: cleaned analytic file, ready for join with property file in 1-04.
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2026-04-23
# Started:   MM/DD/YYYY
# Updated:   MM/DD/YYYY
#
# ---- FILE DECISION ----
#
# Single analytic file: 014500 (March 2026 pull, ~10.27M rows)
# 014500 adds ~129K newer transactions not in 144502 (99.2% overlap).
# 144502 (Sept 2025 pull) is NOT loaded — redundant, 99.2% overlap confirmed
# in 999_dedup_check.R. Use 014500 only.
#
# ---- PRIMARY SAMPLE RESTRICTIONS (implemented below) ----
#
#   Applied sequentially to owner_transfer:
#   1. Non-CA records:        situs_state != "CA"
#   2. No buyer name at position 1: is.na(buy1_name)
#   3. Pre-2019:              sale_date < 2019-01-01
#   4. SFR filter:            prop_indicator != "10" | residential != "Y"
#   5. Mobile homes:          mobile_home == "Y"
#   6. NA clip:               is.na(clip_id)  [cannot link to property file]
#   7. NA sale amount:        is.na(sale_amt)
#        Justification: computed below at drop #7 against 014500 and
#        reported to drop_list. Prior estimate from 144502: ~97.8% of
#        dropped rows were interfamily==1 or buyer/seller name match;
#        <1% of analytic sample unexplained. Verify against 014500.
#   8. Trust / public entity exclusions from buy1_corp == 1:
#        TRUST, LIVING TRUST, STATE OF CA, STATE OF CALIFORNIA,
#        COUNTY OF, CITY OF, VETERANS AFFAIRS, UNIFIED SCHOOL,
#        WATER STORAGE, FLOOD CONTROL, REGENTS OF
#        [full exclusion list — expand as needed]
#   9. Inconsistent buyer corp flags: corp_consistent == 0
#        corp_consistent = 1 when buy1_corp is the same value across
#        all buyer positions that are present. Rows where positions
#        disagree (e.g. buy1_corp=1, buy2_corp=0) are ambiguous and
#        dropped. Count reported at runtime.
#
# ---- SENSITIVITY ANALYSES ONLY (not primary drops) ----
#
#   10. interfamily == 1
#   11. buy_occ == "T" & buy1_corp == 0  (absentee non-corporate)
#
# ---- FLAGS CREATED ----
#
#   gd_flag:         sale_doc_type == "GD" (Grant Deed — standard arms-length in CA)
#   trust_flag:      any of buy1–buy4 name contains "TRUST"
#   name_match_flag: buyer last name in seller name fields or vice versa (sensitivity only)
#   corp_consistent: buy1_corp consistent across all present buyer positions
#
# ---- NOTE ON GD FILTER ----
#
# Non-GD transactions are NOT dropped as a primary restriction. The NA sale
# amount drop (restriction #7) catches most non-market transfers regardless
# of deed type. gd_flag is created for sensitivity analysis and the doc type
# distribution is printed before and after the NA sale amount drop.
#
# ---- SAMPLE RESTRICTION JUSTIFICATION ----
#
# NA sale amount: missing sale amount is the cleanest signal for non-market
#   transfers (gifts, intra-family, trust transfers). Drop computed and
#   reported against 014500 at runtime — see drop_list output.
#
# Interfamily flag: in 144502, 75% of interfamily==1 rows had a buyer/seller
#   last name match; 25% did not (different surnames in families). Flag is
#   meaningful but not exhaustive — use as sensitivity, not primary drop.
#   Verify against 014500 if needed.
# **************************************************


# *************
# 1. Setup ----

user   <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root   <- paste0(user, "Fire Investment/")
secure <- "Y:/Institutional Investment/"

data_input_s  <- paste0(secure, "Data/Source/")
data_output_s <- paste0(secure, "Data/Derived/Fire Investment/")

transfer_root <- paste0(data_input_s, "Cotality/September 2025 Transfer")

# logging
timestamp  <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file   <- paste0(secure, "Process/Fire Investment/Logs/1-02_log_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)

start_time <- Sys.time()
set.seed(123)

# drop list — accumulates all sample restriction counts; written to txt at close
drop_list <- list()

cat("\n====================================================\n")
cat("  1-02_load_clean.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

# packages
pkgs <- c("dplyr", "tidyverse", "data.table", "beepr", "R.utils", "ggplot2", "lubridate")
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

# helper: find date columns and convert YYYYMMDD -> Date
convert_date_cols <- function(df) {
  date_cols <- names(df)[grepl("date", names(df), ignore.case = TRUE)]
  if (length(date_cols) == 0) {
    cat("  No columns with 'date' in name found.\n")
    return(df)
  }
  cat(sprintf("  Found %d date column(s): %s\n", length(date_cols),
              paste(date_cols, collapse = ", ")))
  cat("\n  Head of date columns:\n")
  print(as.data.frame(head(df[, date_cols, drop = FALSE])))
  for (col in date_cols) {
    sample_vals <- na.omit(as.character(df[[col]]))
    looks_yyyymmdd <- length(sample_vals) > 0 &&
      all(nchar(sample_vals[1:min(10, length(sample_vals))]) == 8)
    if (looks_yyyymmdd) {
      df[[col]] <- as.Date(as.character(df[[col]]), format = "%Y%m%d")
      cat(sprintf("  Converted %s -> Date\n", col))
    } else {
      cat(sprintf("  Skipped %s (does not look like YYYYMMDD)\n", col))
    }
  }
  df
}

# helper: normalize column names (lower, underscores, no leading/trailing)
normalize_names <- function(df) {
  nms <- tolower(names(df))
  nms <- gsub("[^a-z0-9]+", "_", nms)
  nms <- gsub("_+", "_", nms)
  nms <- sub("^_", "", nms)
  nms <- sub("_$", "", nms)
  if (any(duplicated(nms))) stop("Name collision after normalization — check columns.")
  names(df) <- nms
  df
}

# helper: recode buyX_corp
#   1  = corporate (Y in original)
#   0  = buyer present but not flagged corporate
#   NA = no buyer at that position
recode_corp <- function(corp_col, name_col) {
  dplyr::case_when(
    corp_col == "Y"                    ~ 1L,
    is.na(corp_col) & !is.na(name_col) ~ 0L,
    TRUE                               ~ NA_integer_
  )
}

# helper: safe grepl for name matching (guards against NA / very short strings)
safe_grepl <- function(pattern, x) {
  ifelse(is.na(pattern) | is.na(x) | nchar(trimws(pattern)) <= 2,
         FALSE,
         mapply(grepl, trimws(pattern), x, MoreArgs = list(ignore.case = TRUE)))
}

# helper: log row drop with count and percent, and append to drop_list
log_drop <- function(before, after, label) {
  n_drop <- before - after
  pct    <- round(100 * n_drop / before, 2)
  msg    <- sprintf("DROP [%s]: -%s rows (%.2f%%)  |  remaining: %s",
                    label,
                    formatC(n_drop, format = "d", big.mark = ","),
                    pct,
                    formatC(after,  format = "d", big.mark = ","))
  cat(sprintf("  %s\n", msg))
  drop_list[[length(drop_list) + 1]] <<- msg
}

# rename map for owner_transfer columns
ot_rename <- c(
  clip_id         = "clip",
  prev_clip       = "previous_clip",
  fips            = "fips_code",
  apn_unformatted = "apn_parcel_number_unformatted",
  landuse_code    = "land_use_code_static",
  prop_indicator  = "property_indicator_code_static",
  mobile_home     = "mobile_home_indicator",
  bldg_count      = "total_number_of_buildings",
  situs_state     = "deed_situs_state_static",   # kept for CA filter drop; removed after
  txn_id          = "owner_transfer_composite_transaction_id",
  batch_date      = "transaction_batch_date",
  sale_date       = "sale_derived_date",
  sale_rec_date   = "sale_derived_recording_date",
  sale_amt        = "sale_amount",
  sale_type       = "sale_type_code",
  sale_doc_type   = "sale_document_type_code",
  deed_cat        = "deed_category_type_code",
  primary_cat     = "primary_category_code",
  pending         = "pending_record_indicator",
  investor        = "investor_purchase_indicator",
  residential     = "residential_indicator",
  cash            = "cash_purchase_indicator",
  mortgage        = "mortgage_purchase_indicator",
  interfamily     = "interfamily_related_indicator",
  new_constr      = "new_construction_indicator",
  resale          = "resale_indicator",
  short_sale      = "short_sale_indicator",
  reo             = "foreclosure_reo_indicator",
  reo_sale        = "foreclosure_reo_sale_indicator",
  buy1_name       = "buyer_1_full_name",
  buy1_last       = "buyer_1_last_name",
  buy1_first      = "buyer_1_first_name_and_middle_initial",
  buy1_corp       = "buyer_1_corporate_indicator",
  buy2_name       = "buyer_2_full_name",
  buy2_last       = "buyer_2_last_name",
  buy2_first      = "buyer_2_first_name_and_middle_initial",
  buy2_corp       = "buyer_2_corporate_indicator",
  buy3_name       = "buyer_3_full_name",
  buy3_last       = "buyer_3_last_name",
  buy3_first      = "buyer_3_first_name_and_middle_initial",
  buy3_corp       = "buyer_3_corporate_indicator",
  buy4_name       = "buyer_4_full_name",
  buy4_last       = "buyer_4_last_name",
  buy4_first      = "buyer_4_first_name_and_middle_initial",
  buy4_corp       = "buyer_4_corporate_indicator",
  buy_occ         = "buyer_occupancy_code",
  buy_rights      = "buyer_ownership_rights_code",
  partial_int     = "partial_interest_indicator",
  sell1_name      = "seller_1_full_name",
  sell1_last      = "seller_1_last_name",
  sell1_first     = "seller_1_first_name",
  sell2_name      = "seller_2_full_name",
  record_action   = "record_action_indicator"
)

ot_keep <- c(
  "clip_id", "prev_clip", "fips", "apn_unformatted",
  "landuse_code", "prop_indicator", "mobile_home", "bldg_count",
  "situs_state",
  "txn_id", "batch_date", "sale_date", "sale_rec_date",
  "sale_amt", "sale_type", "sale_doc_type", "deed_cat", "primary_cat", "pending",
  "investor", "residential", "cash", "mortgage", "interfamily",
  "new_constr", "resale", "short_sale", "reo", "reo_sale",
  "buy1_name", "buy1_last", "buy1_first", "buy1_corp",
  "buy2_name", "buy2_last", "buy2_first", "buy2_corp",
  "buy3_name", "buy3_last", "buy3_first", "buy3_corp",
  "buy4_name", "buy4_last", "buy4_first", "buy4_corp",
  "buy_occ", "buy_rights", "partial_int",
  "sell1_name", "sell1_last", "sell1_first",
  "sell2_name", "record_action"
)

# trust / public entity exclusion patterns (buy1_corp == 1 only)
# applied to buy1_name; expand list as needed based on review
trust_exclusion_patterns <- paste(c(
  "\\bTRUST\\b",
  "\\bLIVING TRUST\\b",
  "\\bSTATE OF CA\\b",
  "STATE OF CALIFORNIA",
  "\\bCOUNTY OF\\b",
  "\\bCITY OF\\b",
  "VETERANS AFFAIRS",
  "UNIFIED SCHOOL",
  "WATER STORAGE",
  "FLOOD CONTROL",
  "\\bREGENTS OF\\b"
), collapse = "|")


# *****************************************************
# 2. Load owner_transfer  [014500, March 2026 pull] ----
# *****************************************************
# 144502 (Sept 2025) not loaded — 99.2% overlap confirmed in 999_dedup_check.R.
# 014500 adds ~129K newer transactions; use as single analytic file.
# Update pattern if Cotality redelivers under a different filename.

f_ot <- list.files(
  transfer_root,
  pattern    = "^UNIVERSITY_OF_CALIFORNIA.*OwnerTransfer.*014500.*\\.csv$",
  full.names = TRUE, recursive = TRUE, ignore.case = TRUE
)
f_ot <- f_ot[!grepl("hist_", f_ot, ignore.case = TRUE)]
if (length(f_ot) == 0) stop("014500 OwnerTransfer CSV not found — check transfer_root path.")
if (length(f_ot) > 1)  stop(sprintf("Multiple matches for 014500: %s", paste(basename(f_ot), collapse = ", ")))

ts(sprintf("Loading: %s", basename(f_ot)))

owner_transfer <- as.data.frame(data.table::fread(
  f_ot,
  na.strings = c("", "NA", "N/A", "null"),
  showProgress = TRUE
))

cat(sprintf("  Loaded: %s rows x %d columns\n",
            formatC(nrow(owner_transfer), format = "d", big.mark = ","),
            ncol(owner_transfer)))
drop_list <- c(drop_list, sprintf("owner_transfer loaded (014500): %s rows x %d columns",
                                  formatC(nrow(owner_transfer), format = "d", big.mark = ","),
                                  ncol(owner_transfer)))

# raw backup — restart from here without reloading if you need to re-run cleaning
owner_transfer_raw <- owner_transfer
cat("  Backup saved as owner_transfer_raw\n")


# *****************************************************
# 3. Early cleaning ----
# *****************************************************

# --- 3a. Normalize names ----
ts("  Normalizing column names...")
owner_transfer <- normalize_names(owner_transfer)

# --- 3b. Convert dates ----
ts("  Converting date columns...")
owner_transfer <- convert_date_cols(owner_transfer)

# --- 3c. Rename and select ----
ts("  Renaming and selecting columns...")
owner_transfer <- owner_transfer %>%
  dplyr::rename(any_of(ot_rename)) %>%
  dplyr::select(any_of(ot_keep))

# --- 3d. Recode buyX_corp ----
ts("  Recoding buyX_corp...")
owner_transfer$buy1_corp <- recode_corp(owner_transfer$buy1_corp, owner_transfer$buy1_name)
owner_transfer$buy2_corp <- recode_corp(owner_transfer$buy2_corp, owner_transfer$buy2_name)
owner_transfer$buy3_corp <- recode_corp(owner_transfer$buy3_corp, owner_transfer$buy3_name)
owner_transfer$buy4_corp <- recode_corp(owner_transfer$buy4_corp, owner_transfer$buy4_name)

# corp recode summary: 1 = corporate, 0 = present/non-corporate, NA = no buyer at position
for (pos in 1:4) {
  col <- paste0("buy", pos, "_corp")
  n1  <- sum(owner_transfer[[col]] == 1L, na.rm = TRUE)
  n0  <- sum(owner_transfer[[col]] == 0L, na.rm = TRUE)
  nna <- sum(is.na(owner_transfer[[col]]))
  cat(sprintf("  %s:  corporate=%s  non-corp=%s  no-buyer(NA)=%s\n",
              col,
              formatC(n1,  format = "d", big.mark = ","),
              formatC(n0,  format = "d", big.mark = ","),
              formatC(nna, format = "d", big.mark = ",")))
  drop_list <- c(drop_list, sprintf(
    "recode %s: corporate(1)=%s | non-corp(0)=%s | no-buyer(NA)=%s",
    col,
    formatC(n1,  format = "d", big.mark = ","),
    formatC(n0,  format = "d", big.mark = ","),
    formatC(nna, format = "d", big.mark = ",")
  ))
}
rm(pos, col, n1, n0, nna)

cat(sprintf("\n  After rename/recode: %s rows x %d columns\n",
            formatC(nrow(owner_transfer), format = "d", big.mark = ","),
            ncol(owner_transfer)))

# --- 3e. Create flags (before drops) ----
ts("  Creating flags...")

# gd_flag: Grant Deed indicator (standard arms-length deed type in CA)
# Not a hard drop — used for sensitivity analysis and printed before/after NA sale amt drop
owner_transfer$gd_flag <- as.integer(owner_transfer$sale_doc_type == "GD")

# trust flag: any buyer position (1–4) name contains "TRUST"
owner_transfer$trust_flag <-
  grepl("TRUST", owner_transfer$buy1_name, ignore.case = TRUE) |
  grepl("TRUST", owner_transfer$buy2_name, ignore.case = TRUE) |
  grepl("TRUST", owner_transfer$buy3_name, ignore.case = TRUE) |
  grepl("TRUST", owner_transfer$buy4_name, ignore.case = TRUE)

# name match flag: buyer last name found in seller name fields, or vice versa
# uses last name columns (cleaner than full name matching)
# seller_1_last_name checked against buyer full names as seller last name field
# may not always be populated — full name used as fallback on seller side
owner_transfer$name_match_flag <-
  safe_grepl(owner_transfer$buy1_last, owner_transfer$sell1_name) |
  safe_grepl(owner_transfer$buy1_last, owner_transfer$sell2_name) |
  safe_grepl(owner_transfer$buy2_last, owner_transfer$sell1_name) |
  safe_grepl(owner_transfer$buy2_last, owner_transfer$sell2_name) |
  safe_grepl(owner_transfer$sell1_last, owner_transfer$buy1_name) |
  safe_grepl(owner_transfer$sell1_last, owner_transfer$buy2_name)

# corp_consistent: flags rows where buyer positions disagree on corporate status.
# buy1_corp is treated as the reference; positions 2–4 are compared only if present
# (NA = no buyer at that position, so NA positions are ignored).
# corp_consistent == 0 means at least one present position contradicts buy1_corp —
# these are ambiguous and dropped in restriction #9.
owner_transfer$corp_consistent <- as.integer(
  (is.na(owner_transfer$buy2_corp) | owner_transfer$buy2_corp == owner_transfer$buy1_corp) &
    (is.na(owner_transfer$buy3_corp) | owner_transfer$buy3_corp == owner_transfer$buy1_corp) &
    (is.na(owner_transfer$buy4_corp) | owner_transfer$buy4_corp == owner_transfer$buy1_corp)
)

cat(sprintf("  trust_flag:         %s TRUE\n",
            formatC(sum(owner_transfer$trust_flag, na.rm = TRUE), format = "d", big.mark = ",")))
cat(sprintf("  name_match_flag:    %s TRUE\n",
            formatC(sum(owner_transfer$name_match_flag, na.rm = TRUE), format = "d", big.mark = ",")))
cat(sprintf("  corp_consistent==0: %s rows\n",
            formatC(sum(owner_transfer$corp_consistent == 0, na.rm = TRUE), format = "d", big.mark = ",")))


# *****************************************************
# 4. Primary sample restrictions ----
# *****************************************************
ts("Applying primary sample restrictions...")

n0 <- nrow(owner_transfer)
drop_list <- c(drop_list, sprintf("starting rows before sample restrictions: %s",
                                  formatC(n0, format = "d", big.mark = ",")))
cat(sprintf("\n  Starting rows: %s\n", formatC(n0, format = "d", big.mark = ",")))

# 1. Non-CA
owner_transfer <- dplyr::filter(owner_transfer, situs_state == "CA")
log_drop(n0, nrow(owner_transfer), "non-CA"); n0 <- nrow(owner_transfer)
owner_transfer$situs_state <- NULL   # only needed for CA filter; property file has address fields

# 2. No buyer name at position 1
# Rows with no buy1_name have no usable buyer information and cannot be classified.
# buy1_corp will also be NA for these rows after recode, but name is the cleaner signal.
n_no_buy1 <- sum(is.na(owner_transfer$buy1_name))
cat(sprintf("\n  Rows with no buy1_name: %s\n",
            formatC(n_no_buy1, format = "d", big.mark = ",")))
owner_transfer <- dplyr::filter(owner_transfer, !is.na(buy1_name))
log_drop(n0, nrow(owner_transfer), "no buy1_name"); n0 <- nrow(owner_transfer)
rm(n_no_buy1)

# 3. Pre-2019
owner_transfer <- dplyr::filter(owner_transfer, sale_date >= as.Date("2019-01-01"))
log_drop(n0, nrow(owner_transfer), "pre-2019"); n0 <- nrow(owner_transfer)

# 4. SFR filter
owner_transfer <- dplyr::filter(owner_transfer, prop_indicator == "10" & residential == "Y")
log_drop(n0, nrow(owner_transfer), "non-SFR"); n0 <- nrow(owner_transfer)

# 5. Mobile homes
owner_transfer <- dplyr::filter(owner_transfer, mobile_home != "Y" | is.na(mobile_home))
log_drop(n0, nrow(owner_transfer), "mobile home"); n0 <- nrow(owner_transfer)

# 6. NA clip
owner_transfer <- dplyr::filter(owner_transfer, !is.na(clip_id))
log_drop(n0, nrow(owner_transfer), "NA clip_id"); n0 <- nrow(owner_transfer)

# 7. NA sale amount
# Print doc type table before drop — shows GD vs non-GD composition of NA-sale-amt rows
cat("\n  sale_doc_type distribution BEFORE NA sale_amt drop:\n")
print(table(owner_transfer$sale_doc_type, useNA = "always"))

# Compute justification: what share of NA-sale-amt rows are explained?
na_amt_rows   <- dplyr::filter(owner_transfer, is.na(sale_amt))
n_na_amt      <- nrow(na_amt_rows)
n_interfamily <- sum(na_amt_rows$interfamily == 1, na.rm = TRUE)
n_namematch   <- sum(na_amt_rows$name_match_flag, na.rm = TRUE)
n_trust       <- sum(na_amt_rows$trust_flag, na.rm = TRUE)
n_explained   <- sum(
  na_amt_rows$interfamily == 1 |
    na_amt_rows$name_match_flag    |
    na_amt_rows$trust_flag,
  na.rm = TRUE
)
n_unexplained <- n_na_amt - n_explained
pct_explained <- round(100 * n_explained / n_na_amt, 1)
pct_unexplained_of_sample <- round(100 * n_unexplained / nrow(owner_transfer), 2)

cat(sprintf("\n  NA sale_amt rows: %s\n", formatC(n_na_amt, format = "d", big.mark = ",")))
cat(sprintf("    interfamily==Y:          %s\n", formatC(n_interfamily, format = "d", big.mark = ",")))
cat(sprintf("    name_match_flag==TRUE:   %s\n", formatC(n_namematch,   format = "d", big.mark = ",")))
cat(sprintf("    trust_flag==TRUE:        %s\n", formatC(n_trust,       format = "d", big.mark = ",")))
cat(sprintf("    any (explained):         %s (%.1f%% of NA-amt rows)\n",
            formatC(n_explained, format = "d", big.mark = ","), pct_explained))
cat(sprintf("    unexplained:             %s (%.2f%% of pre-drop sample)\n",
            formatC(n_unexplained, format = "d", big.mark = ","), pct_unexplained_of_sample))

drop_list <- c(drop_list, sprintf(
  "NA sale_amt drop: %s rows | explained (interfamily|name match|trust): %s (%.1f%%) | unexplained: %s (%.2f%% of sample)",
  formatC(n_na_amt,      format = "d", big.mark = ","),
  formatC(n_explained,   format = "d", big.mark = ","), pct_explained,
  formatC(n_unexplained, format = "d", big.mark = ","), pct_unexplained_of_sample
))
rm(na_amt_rows, n_na_amt, n_interfamily, n_namematch, n_trust,
   n_explained, n_unexplained, pct_explained, pct_unexplained_of_sample)

owner_transfer <- dplyr::filter(owner_transfer, !is.na(sale_amt))
log_drop(n0, nrow(owner_transfer), "NA sale_amt"); n0 <- nrow(owner_transfer)

cat("\n  sale_doc_type distribution AFTER NA sale_amt drop:\n")
print(table(owner_transfer$sale_doc_type, useNA = "always"))

# 8. Trust / public entity exclusions (buy1_corp == 1 rows only)
is_trust_public <- owner_transfer$buy1_corp == 1 &
  grepl(trust_exclusion_patterns, owner_transfer$buy1_name, ignore.case = TRUE, perl = TRUE)
cat(sprintf("\n  Trust/public entity rows identified: %s\n",
            formatC(sum(is_trust_public, na.rm = TRUE), format = "d", big.mark = ",")))
owner_transfer <- dplyr::filter(owner_transfer, !is_trust_public | is.na(is_trust_public))
log_drop(n0, nrow(owner_transfer), "trust/public entity"); n0 <- nrow(owner_transfer)

# 9. Inconsistent buyer corp flags
owner_transfer <- dplyr::filter(owner_transfer, corp_consistent == 1 | is.na(corp_consistent))
log_drop(n0, nrow(owner_transfer), "corp_consistent==0"); n0 <- nrow(owner_transfer)

drop_list <- c(drop_list, sprintf("analytic sample after all restrictions: %s rows",
                                  formatC(nrow(owner_transfer), format = "d", big.mark = ",")))
cat(sprintf("\n  ANALYTIC SAMPLE: %s rows\n", formatC(nrow(owner_transfer), format = "d", big.mark = ",")))


# *****************************************************
# 5. Drop constant columns ----
# *****************************************************
# Remove any column where EVERY value (including NAs) is identical.
# A mix of a value and NA is NOT constant and will not be dropped.
# Targets columns made constant by sample restrictions (e.g. prop_indicator,
# residential, sale_doc_type, mobile_home after filtering to one value each).

ts("Dropping constant columns...")
cat("\nColumns before constant-drop:\n")
print(names(owner_transfer))

const_cols <- names(owner_transfer)[sapply(owner_transfer, function(x) {
  length(unique(x)) == 1
})]

if (length(const_cols) == 0) {
  cat("  No constant columns found.\n")
} else {
  cat(sprintf("  Constant columns dropped (%d): %s\n",
              length(const_cols), paste(const_cols, collapse = ", ")))
  drop_list <- c(drop_list, sprintf(
    "constant columns dropped: %s", paste(const_cols, collapse = ", ")
  ))
  owner_transfer <- owner_transfer[, !names(owner_transfer) %in% const_cols]
}

cat("\nColumns after constant-drop:\n")
print(names(owner_transfer))


# *****************************************************
# 6. Quick checks — analytic sample ----
# *****************************************************
ts("Quick checks on analytic sample...")

cat("\nSale date range:\n")
print(summary(owner_transfer$sale_date))

cat("\nbuy1_corp distribution:\n")
print(table(owner_transfer$buy1_corp, useNA = "always"))

cat("\nCorporate buyers (buy1_corp == 1):", formatC(sum(owner_transfer$buy1_corp == 1, na.rm = TRUE),
                                                    format = "d", big.mark = ","), "\n")

cat("\ntrust_flag among corporate buyers:\n")
print(table(owner_transfer$trust_flag[owner_transfer$buy1_corp == 1], useNA = "always"))

cat("\ninvestor flag:\n")
print(table(owner_transfer$investor, useNA = "always"))

# sale date histogram — analytic sample
dir.create(paste0(root, "Process/Images"), showWarnings = FALSE, recursive = TRUE)

p1 <- ggplot(owner_transfer, aes(x = sale_date)) +
  geom_histogram(binwidth = 91, fill = "steelblue", color = "white") +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  labs(title = "Sale Date — analytic sample (2019+, 3-month bins)",
       x = "Sale Date", y = "Count") +
  theme_minimal()
print(p1)
ggsave(paste0(root, "Process/Images/1-02_sale_date_analytic.png"),
       plot = p1, width = 10, height = 5, dpi = 150)

message("Elapsed so far: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")


# *****************************************************
# 7. Save ----
# *****************************************************
# Saves cleaned owner_transfer (analytic sample, no property join).
# Property join happens in 1-04, which combines this file with 1-03_property_sfr.rds.
ts("Saving...")

out_analytic <- paste0(data_output_s, "1-02_owner_transfer.rds")
saveRDS(owner_transfer, file = out_analytic)
cat(sprintf("\n  Saved -> %s\n", out_analytic))

cat("\nFinal analytic sample:\n")
cat(sprintf("  Rows:    %s\n", formatC(nrow(owner_transfer), format = "d", big.mark = ",")))
cat(sprintf("  Columns: %d\n", ncol(owner_transfer)))


# ******************************
# 8. Close out ----
# Paths are intentionally split:
#   root (Dropbox)  — drop list, images: no PII, used when writing up
#   secure (Y:/)    — log, history: may contain PII, for debugging only

cat("\n\nObject saved:\n")
cat(sprintf("  Analytic:  %s\n", out_analytic))

# drop list -> Dropbox (no PII, used for write-up)
drop_list_file <- paste0(root, "Process/Logs/1-02_drop_list_", timestamp, ".txt")
writeLines(as.character(drop_list), drop_list_file)
cat(sprintf("  Drop list: %s\n", drop_list_file))
cat("\nDrop list summary:\n")
invisible(lapply(drop_list, function(x) cat(sprintf("  %s\n", x))))

message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")

beep()
# log and history -> secure drive (may contain PII)
savehistory(paste0(secure, "Process/Fire Investment/Logs/1-02_history_", timestamp, ".txt"))
sink()

