



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
  "APN SEQUENCE NUMBER",
  "COMPOSITE PROPERTY LINKAGE KEY",
  "SALE AMOUNT",
  "SALE DERIVED DATE",
  "SALE DERIVED RECORDING DATE",
  "BUYER 1 FULL NAME",
  "BUYER 2 FULL NAME",
  "SELLER 1 FULL NAME",
  "SELLER 2 FULL NAME",
  "SALE DOCUMENT TYPE CODE",
  "INTERFAMILY RELATED INDICATOR",
  "PENDING RECORD INDICATOR",
  "MULTI OR SPLIT PARCEL CODE",
  "OWNERSHIP TRANSFER PERCENTAGE",
  "PROPERTY INDICATOR CODE - STATIC"
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
  
  cat("\nSample of within-file txn_date dupes (sorted by clip, NA clips excluded):\n")
  sub <- df[df$txn_date %in% dup_keys & !is.na(df$clip), ]
  sub <- sub[order(sub$clip, sub$sale_derived_date), ]
  print(head(sub, 30))
}


# *****************************************************
# 6. Compare across files ----
# *****************************************************
ts("Comparing across files...")

cat("\ntxn_date keys:\n")
cat(sprintf("  Shared:          %s\n", formatC(length(intersect(ot_144502$txn_date, ot_014500$txn_date)), format = "d", big.mark = ",")))
cat(sprintf("  Only in 144502:  %s\n", formatC(length(setdiff(ot_144502$txn_date, ot_014500$txn_date)), format = "d", big.mark = ",")))
cat(sprintf("  Only in 014500:  %s\n", formatC(length(setdiff(ot_014500$txn_date, ot_144502$txn_date)), format = "d", big.mark = ",")))

# date range of unique transactions in each file
only_144502 <- setdiff(ot_144502$txn_date, ot_014500$txn_date)
only_014500 <- setdiff(ot_014500$txn_date, ot_144502$txn_date)

cat("\nSale date range for txn_date only in 144502:\n")
print(summary(ot_144502$sale_derived_date[ot_144502$txn_date %in% only_144502]))

cat("\nSale date range for txn_date only in 014500:\n")
print(summary(ot_014500$sale_derived_date[ot_014500$txn_date %in% only_014500]))

cat("\nSample of txn_date only in 144502:\n")
sub_144502 <- ot_144502[ot_144502$txn_date %in% only_144502, ]
sub_144502 <- sub_144502[order(sub_144502$clip, sub_144502$sale_derived_date), ]
print(head(sub_144502, 20))

cat("\nSample of txn_date only in 014500:\n")
sub_014500 <- ot_014500[ot_014500$txn_date %in% only_014500, ]
sub_014500 <- sub_014500[order(sub_014500$clip, sub_014500$sale_derived_date), ]
print(head(sub_014500, 20))


# ******************************
# 7. Close out ----

message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")

savehistory(paste0(secure, "Process/Fire Investment/Logs/999_dedup_check_history_", timestamp, ".txt"))
sink()








