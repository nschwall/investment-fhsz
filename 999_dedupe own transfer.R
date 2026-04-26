
# **************************************************
#                     DETAILS
#
# Purpose:   Scratch — understand the relationship between
#            owner_transfer_144502 and owner_transfer_014500
#            Start by understanding each file individually,
#            then compare
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
cat("  999_dedup_check.R  |  Started:", format(start_time), "\n")
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
log_file  <- paste0(secure, "Process/Fire Investment/Logs/999_dedup_check_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)

ts <- function(label = "") {
  cat(sprintf("\n[%s]  %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), label))
}

# columns to keep — exact raw names from CSV
keep_cols <- c(
  "CLIP",
  "APN (PARCEL NUMBER UNFORMATTED)",
  "COMPOSITE PROPERTY LINKAGE KEY",
  "SALE AMOUNT",
  "SALE DERIVED DATE",
  "SALE DERIVED RECORDING DATE",
  "BUYER 1 FULL NAME",
  "BUYER 1 LAST NAME",
  "BUYER 2 FULL NAME",
  "BUYER 2 LAST NAME",
  "SELLER 1 FULL NAME",
  "SELLER 1 LAST NAME",
  "SELLER 2 FULL NAME",
  "SALE DOCUMENT TYPE CODE",
  "INTERFAMILY RELATED INDICATOR",
  "PENDING RECORD INDICATOR",
  "MULTI OR SPLIT PARCEL CODE",
  "OWNERSHIP TRANSFER PERCENTAGE",
  "PROPERTY INDICATOR CODE - STATIC",
  "NEW CONSTRUCTION INDICATOR",
  "PRIMARY CATEGORY CODE",
  "ACTUAL YEAR BUILT - STATIC",
  "TOTAL NUMBER OF BUILDINGS"
)

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
# 2. Discover CSV files ----
# *****************************************************
ts("Scanning for CSV files...")

skip_dirs <- c("Zip Folders", "Meta Data")
subdirs   <- list.dirs(transfer_root, full.names = TRUE, recursive = FALSE)
subdirs   <- subdirs[!basename(subdirs) %in% skip_dirs]

csv_files <- unlist(lapply(subdirs, function(d) {
  list.files(d,
             pattern = "^UNIVERSITY_OF_CALIFORNIA.*OwnerTransfer.*\\.csv$",
             full.names = TRUE, recursive = TRUE, ignore.case = TRUE)
}))
csv_files <- csv_files[!grepl("hist_", csv_files, ignore.case = TRUE)]

cat(sprintf("Found %d OwnerTransfer file(s):\n", length(csv_files)))
cat(paste0("  ", basename(csv_files), "\n"))

f_144502 <- csv_files[grepl("144502", csv_files)]
f_014500 <- csv_files[grepl("014500", csv_files)]


# *****************************************************
# 3. Load and slim owner_transfer_144502 ----
# *****************************************************
ts(sprintf("Loading 144502: %s", basename(f_144502)))

ot_144502 <- as.data.frame(data.table::fread(
  f_144502,
  select       = keep_cols,
  na.strings   = c("", "NA", "N/A", "null"),
  showProgress = TRUE
))

ot_144502 <- normalize_names(ot_144502)
ot_144502 <- convert_date_cols(ot_144502)

cat(sprintf("  %s rows x %d columns\n",
            formatC(nrow(ot_144502), format = "d", big.mark = ","),
            ncol(ot_144502)))
cat(sprintf("  Columns: %s\n", paste(names(ot_144502), collapse = ", ")))

cat(sprintf("\n  Before SFR filter (prop_indicator == 10): %s rows\n",
            formatC(nrow(ot_144502), format = "d", big.mark = ",")))
ot_144502 <- ot_144502[!is.na(ot_144502$property_indicator_code_static) &
                         ot_144502$property_indicator_code_static == "10", ]
cat(sprintf("  After SFR filter: %s rows\n",
            formatC(nrow(ot_144502), format = "d", big.mark = ",")))

cat("\n  clip vs APN cross-tab:\n")
print(with(ot_144502, table(is.na(clip), is.na(apn_parcel_number_unformatted))))

cat("\n  clip vs composite_property_linkage_key cross-tab:\n")
print(with(ot_144502, table(is.na(clip), is.na(composite_property_linkage_key))))

# NOTE: APN fallback swap commented out — would need to be consistent across both
# files to be useful for cross-file matching, and that's complex. Revisit in 1-02.
# ot_144502$clip[is.na(ot_144502$clip) & !is.na(ot_144502$apn_parcel_number_unformatted)] <-
#   paste0("APN_", ot_144502$apn_parcel_number_unformatted[
#     is.na(ot_144502$clip) & !is.na(ot_144502$apn_parcel_number_unformatted)])

gc()
ts("  gc() done")


# *****************************************************
# 4. Load and slim owner_transfer_014500 ----
# *****************************************************
ts(sprintf("Loading 014500: %s", basename(f_014500)))

ot_014500 <- as.data.frame(data.table::fread(
  f_014500,
  select       = keep_cols,
  na.strings   = c("", "NA", "N/A", "null"),
  showProgress = TRUE
))

ot_014500 <- normalize_names(ot_014500)
ot_014500 <- convert_date_cols(ot_014500)

cat(sprintf("  %s rows x %d columns\n",
            formatC(nrow(ot_014500), format = "d", big.mark = ","),
            ncol(ot_014500)))

cat(sprintf("\n  Before SFR filter (prop_indicator == 10): %s rows\n",
            formatC(nrow(ot_014500), format = "d", big.mark = ",")))
ot_014500 <- ot_014500[!is.na(ot_014500$property_indicator_code_static) &
                         ot_014500$property_indicator_code_static == "10", ]
cat(sprintf("  After SFR filter: %s rows\n",
            formatC(nrow(ot_014500), format = "d", big.mark = ",")))

cat("\n  clip vs APN cross-tab:\n")
print(with(ot_014500, table(is.na(clip), is.na(apn_parcel_number_unformatted))))

cat("\n  clip vs composite_property_linkage_key cross-tab:\n")
print(with(ot_014500, table(is.na(clip), is.na(composite_property_linkage_key))))

# NOTE: APN fallback swap commented out — would need to be consistent across both
# files to be useful for cross-file matching, and that's complex. Revisit in 1-02.
# ot_014500$clip[is.na(ot_014500$clip) & !is.na(ot_014500$apn_parcel_number_unformatted)] <-
#   paste0("APN_", ot_014500$apn_parcel_number_unformatted[
#     is.na(ot_014500$clip) & !is.na(ot_014500$apn_parcel_number_unformatted)])

gc()
ts("  gc() done")


# *****************************************************
# 5. Understand each file individually ----
# *****************************************************
ts("Examining each file individually...")

# create transaction keys
for (obj in c("ot_144502", "ot_014500")) {
  df <- get(obj)
  df$txn_date   <- paste(df$clip, df$sale_derived_date,  sep = "_")
  df$txn_buyer  <- paste(df$clip, df$buyer_1_full_name,  sep = "_")
  df$txn_seller <- paste(df$clip, df$seller_1_full_name, sep = "_")
  assign(obj, df)
}

for (obj in c("ot_144502", "ot_014500")) {
  df <- get(obj)
  cat(sprintf("\n--- %s ---\n", obj))
  cat(sprintf("Rows: %s\n", formatC(nrow(df), format = "d", big.mark = ",")))
  
  cat("Sale date range:\n")
  print(summary(df$sale_derived_date))
  
  cat("\nWithin-file duplicate txn_date (clip + sale date):\n")
  print(table(duplicated(df$txn_date), useNA = "always"))
  
  cat("\nWithin-file duplicate txn_buyer (clip + buyer):\n")
  print(table(duplicated(df$txn_buyer), useNA = "always"))
  
  cat("\nWithin-file duplicate txn_seller (clip + seller):\n")
  print(table(duplicated(df$txn_seller), useNA = "always"))
  
  cat("\nSale doc type for within-file txn_date dupes:\n")
  dup_keys <- df$txn_date[duplicated(df$txn_date)]
  print(table(df$sale_document_type_code[df$txn_date %in% dup_keys], useNA = "always"))
  
  cat("\nInterfamily for within-file txn_date dupes:\n")
  print(table(df$interfamily_related_indicator[df$txn_date %in% dup_keys], useNA = "always"))
  
  cat("\nMulti/split parcel for within-file txn_date dupes:\n")
  print(table(df$multi_or_split_parcel_code[df$txn_date %in% dup_keys], useNA = "always"))
  
  cat("\nPending for within-file txn_date dupes:\n")
  print(table(df$pending_record_indicator[df$txn_date %in% dup_keys], useNA = "always"))
  
  cat("\nNew construction for within-file txn_date dupes:\n")
  print(table(df$new_construction_indicator[df$txn_date %in% dup_keys], useNA = "always"))
  
  cat("\nPrimary category for within-file txn_date dupes:\n")
  print(table(df$primary_category_code[df$txn_date %in% dup_keys], useNA = "always"))
  
  cat("\nSample of within-file txn_date dupes (sorted by clip, NA clips excluded):\n")
  sub <- df[df$txn_date %in% dup_keys & !is.na(df$clip), ]
  sub <- sub[order(sub$clip, sub$sale_derived_date), ]
  print(head(sub, 30))
}


# *****************************************************
# 6. Dupe check on cleaned subset (drop NA clip + NA sale amount) ----
# *****************************************************
ts("Dupe check on cleaned subset...")

for (obj in c("ot_144502", "ot_014500")) {
  df <- get(obj)
  cat(sprintf("\n--- %s (cleaned subset) ---\n", obj))
  
  cat(sprintf("Rows before drop: %s\n", formatC(nrow(df), format = "d", big.mark = ",")))
  df_clean <- df[!is.na(df$clip) & !is.na(df$sale_amount), ]
  cat(sprintf("Rows after drop NA clip + NA sale amount: %s\n",
              formatC(nrow(df_clean), format = "d", big.mark = ",")))
  
  cat("\nclip vs composite_property_linkage_key cross-tab:\n")
  print(with(df_clean, table(is.na(clip), is.na(composite_property_linkage_key))))
  
  cat("\nWithin-subset duplicate txn_date:\n")
  print(table(duplicated(df_clean$txn_date), useNA = "always"))
  
  cat("\nWithin-subset duplicate txn_buyer:\n")
  print(table(duplicated(df_clean$txn_buyer), useNA = "always"))
  
  cat("\nWithin-subset duplicate txn_seller:\n")
  print(table(duplicated(df_clean$txn_seller), useNA = "always"))
  
  dup_keys_clean <- df_clean$txn_date[duplicated(df_clean$txn_date)]
  
  cat("\nSale doc type for subset txn_date dupes:\n")
  print(table(df_clean$sale_document_type_code[df_clean$txn_date %in% dup_keys_clean], useNA = "always"))
  
  cat("\nSample of subset txn_date dupes (sorted by clip):\n")
  sub_clean <- df_clean[df_clean$txn_date %in% dup_keys_clean, ]
  sub_clean <- sub_clean[order(sub_clean$clip, sub_clean$sale_derived_date), ]
  print(head(sub_clean, 30))
}




# *****************************************************
# 7. Interfamily / no-sale-amount note ----
# *****************************************************
ts("Creating trust and name match flags...")

safe_grepl <- function(pattern, x) {
  ifelse(is.na(pattern) | is.na(x) | nchar(trimws(pattern)) <= 2,
         FALSE,
         mapply(grepl, trimws(pattern), x, MoreArgs = list(ignore.case = TRUE)))
}

# Loop 1: create flags and assign back
for (obj in c("ot_144502", "ot_014500")) {
  df <- get(obj)
  
  df$trust_flag <- grepl("TRUST", df$buyer_1_full_name, ignore.case = TRUE) |
    grepl("TRUST", df$buyer_2_full_name, ignore.case = TRUE)
  
  df$name_match_flag <- safe_grepl(df$buyer_1_last_name,  df$seller_1_full_name) |
    safe_grepl(df$buyer_1_last_name,  df$seller_2_full_name) |
    safe_grepl(df$buyer_2_last_name,  df$seller_1_full_name) |
    safe_grepl(df$buyer_2_last_name,  df$seller_2_full_name) |
    safe_grepl(df$seller_1_last_name, df$buyer_1_full_name)  |
    safe_grepl(df$seller_1_last_name, df$buyer_2_full_name)
  
  assign(obj, df)
}

# Loop 2: cross-tabs against NA sale amount
notes_path <- "C:/Users/Nora Schwaller/Dropbox (Personal)/Fire Investment/Process/Script Output/999_interfamily_note.txt"

for (obj in c("ot_144502", "ot_014500")) {
  df <- get(obj)
  
  lines_out <- c(
    sprintf("=== %s ===", obj),
    sprintf("Total rows: %s", formatC(nrow(df), format = "d", big.mark = ",")),
    ""
  )
  
  cat(paste(lines_out, collapse = "\n"))
  
  cat(sprintf("\n--- %s ---\n", obj))
  
  cat("\nNA sale amount x trust_flag:\n")
  print(table(is.na(df$sale_amount), df$trust_flag, dnn = c("sale_amount_NA", "trust_flag")))
  
  cat("\nNA sale amount x name_match_flag:\n")
  print(table(is.na(df$sale_amount), df$name_match_flag, dnn = c("sale_amount_NA", "name_match_flag")))
  
  cat("\nNA sale amount x trust OR name match:\n")
  print(table(is.na(df$sale_amount), df$trust_flag | df$name_match_flag,
              dnn = c("sale_amount_NA", "trust_or_name_match")))
  
  cat("\nNA sale amount x trust AND name match:\n")
  print(table(is.na(df$sale_amount), df$trust_flag & df$name_match_flag,
              dnn = c("sale_amount_NA", "trust_and_name_match")))
  
  write(lines_out, file = notes_path, append = (obj == "ot_014500"))
}

cat(sprintf("\nNote saved -> %s\n", notes_path))


# *****************************************************
# 8. Compare across files ----
# *****************************************************
ts("Comparing across files...")

# --- 8a. Create cleaned subsets ---
cat("\nCreating cleaned subsets (drop NA clip, pre-2019, non-GD, NA sale amount)...\n")

make_clean <- function(df, label) {
  cat(sprintf("\n%s\n", label))
  cat(sprintf("  Start:                %s\n", formatC(nrow(df), format = "d", big.mark = ",")))
  df <- df[!is.na(df$property_indicator_code_static) & df$property_indicator_code_static == "10", ]
  cat(sprintf("  After SFR filter:     %s\n", formatC(nrow(df), format = "d", big.mark = ",")))
  df <- df[!is.na(df$clip), ]
  cat(sprintf("  After drop NA clip:   %s\n", formatC(nrow(df), format = "d", big.mark = ",")))
  df <- df[!is.na(df$sale_derived_date) & df$sale_derived_date >= as.Date("2019-01-01"), ]
  cat(sprintf("  After drop pre-2019:  %s\n", formatC(nrow(df), format = "d", big.mark = ",")))
  df <- df[!is.na(df$sale_document_type_code) & df$sale_document_type_code == "GD", ]
  cat(sprintf("  After drop non-GD:    %s\n", formatC(nrow(df), format = "d", big.mark = ",")))
  df <- df[!is.na(df$sale_amount), ]
  cat(sprintf("  After drop NA sale:   %s\n", formatC(nrow(df), format = "d", big.mark = ",")))
  df
}

clean_144502 <- make_clean(ot_144502, "144502")
clean_014500 <- make_clean(ot_014500, "014500")

# --- 8b. Cross-file comparison on cleaned subsets ---
cat("\ntxn_date keys:\n")
cat(sprintf("  Shared:          %s\n", formatC(length(intersect(clean_144502$txn_date, clean_014500$txn_date)), format = "d", big.mark = ",")))
cat(sprintf("  Only in 144502:  %s\n", formatC(length(setdiff(clean_144502$txn_date, clean_014500$txn_date)), format = "d", big.mark = ",")))
cat(sprintf("  Only in 014500:  %s\n", formatC(length(setdiff(clean_014500$txn_date, clean_144502$txn_date)), format = "d", big.mark = ",")))

# date range of unique transactions in each file
only_144502 <- setdiff(clean_144502$txn_date, clean_014500$txn_date)
only_014500 <- setdiff(clean_014500$txn_date, clean_144502$txn_date)

cat("\nSale date range for txn_date only in 144502:\n")
print(summary(clean_144502$sale_derived_date[clean_144502$txn_date %in% only_144502]))

cat("\nSale date range for txn_date only in 014500:\n")
print(summary(clean_014500$sale_derived_date[clean_014500$txn_date %in% only_014500]))

cat("\nSample of txn_date only in 144502:\n")
sub_144502 <- clean_144502[clean_144502$txn_date %in% only_144502, ]
sub_144502 <- sub_144502[order(sub_144502$clip, sub_144502$sale_derived_date), ]
print(head(sub_144502, 20))

cat("\nSample of txn_date only in 014500:\n")
sub_014500 <- clean_014500[clean_014500$txn_date %in% only_014500, ]
sub_014500 <- sub_014500[order(sub_014500$clip, sub_014500$sale_derived_date), ]
print(head(sub_014500, 20))


# ******************************
# 9. Close out ----

message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")

savehistory(paste0(secure, "Process/Fire Investment/Logs/999_dedup_check_history_", timestamp, ".txt"))
sink()




