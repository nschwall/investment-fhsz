

# **************************************************
#                     DETAILS
#
# Purpose:   Scratch — identify duplicate/overlapping transactions
#            between owner_transfer_144502 and owner_transfer_014500
#            Load each file slimmed to key columns, rbind, then
#            identify non-unique CLIPs and examine overlap
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

# columns to keep — must match exact raw column names in the CSV
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
  "RECORD ACTION INDICATOR"
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

# helper: convert YYYYMMDD integer/character columns to Date
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

cat(sprintf("  %s rows x %d columns\n",
            formatC(nrow(ot_144502), format = "d", big.mark = ","),
            ncol(ot_144502)))

ts("  Normalizing column names...")
ot_144502 <- normalize_names(ot_144502)
ot_144502 <- convert_date_cols(ot_144502)
ot_144502$source <- "144502"

cat(sprintf("  Columns: %s\n", paste(names(ot_144502), collapse = ", ")))

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

cat(sprintf("  %s rows x %d columns\n",
            formatC(nrow(ot_014500), format = "d", big.mark = ","),
            ncol(ot_014500)))

ts("  Normalizing column names...")
ot_014500 <- normalize_names(ot_014500)
ot_014500 <- convert_date_cols(ot_014500)
ot_014500$source <- "014500"

gc()
ts("  gc() done")


# *****************************************************
# 5. Rbind ----
# *****************************************************
ts("Binding rows...")

ot_combined <- rbind(ot_144502, ot_014500)

cat(sprintf("  Combined: %s rows x %d columns\n",
            formatC(nrow(ot_combined), format = "d", big.mark = ","),
            ncol(ot_combined)))

rm(ot_144502, ot_014500)
gc()
ts("  Removed source objects, gc() done")

cat("\nSource breakdown:\n")
print(table(ot_combined$source, useNA = "always"))


# *****************************************************
# 6. Identify CLIPs appearing in both files ----
# *****************************************************
ts("Identifying CLIPs appearing in both files...")

clip_sources <- aggregate(
  source ~ clip,
  data    = unique(ot_combined[, c("clip", "source")]),
  FUN     = function(x) paste(sort(unique(x)), collapse = "+")
)
names(clip_sources)[2] <- "sources"

cat("\nCLIP source breakdown:\n")
print(table(clip_sources$sources, useNA = "always"))

clips_in_both <- clip_sources$clip[clip_sources$sources == "014500+144502"]

cat(sprintf("\n  %s CLIPs appear in both files\n",
            formatC(length(clips_in_both), format = "d", big.mark = ",")))
cat(sprintf("  %.1f%% of all unique CLIPs\n",
            100 * length(clips_in_both) / nrow(clip_sources)))


# *****************************************************
# 7. Subset to CLIPs in both files ----
# *****************************************************
ts("Subsetting to CLIPs appearing in both files...")

ot_overlap <- ot_combined[ot_combined$clip %in% clips_in_both, ]
ot_overlap  <- ot_overlap[order(ot_overlap$clip, ot_overlap$sale_derived_date), ]

cat(sprintf("  %s rows for overlapping CLIPs\n",
            formatC(nrow(ot_overlap), format = "d", big.mark = ",")))

cat("\nSource breakdown within overlap subset:\n")
print(table(ot_overlap$source, useNA = "always"))


# *****************************************************
# 8. Examine overlap ----
# *****************************************************
ts("Examining overlap...")

# are any rows completely identical ignoring source column?
ot_overlap_nosrc <- ot_overlap[, !names(ot_overlap) %in% "source"]
n_exact_dupes <- sum(duplicated(ot_overlap_nosrc))
cat(sprintf("\n  Exact duplicate rows (ignoring source): %s\n",
            formatC(n_exact_dupes, format = "d", big.mark = ",")))

# for overlapping CLIPs, do sale dates differ between files?
cat("\nFor overlapping CLIPs — do sale dates differ between files?\n")
date_check <- aggregate(
  sale_derived_date ~ clip + source,
  data = ot_overlap,
  FUN  = function(x) paste(sort(unique(as.character(x))), collapse = ", ")
)

date_wide <- reshape(date_check,
                     idvar     = "clip",
                     timevar   = "source",
                     direction = "wide")
names(date_wide) <- gsub("sale_derived_date\\.", "dates_", names(date_wide))

if (all(c("dates_144502", "dates_014500") %in% names(date_wide))) {
  same_dates <- sum(date_wide$dates_144502 == date_wide$dates_014500, na.rm = TRUE)
  diff_dates <- sum(date_wide$dates_144502 != date_wide$dates_014500, na.rm = TRUE)
  cat(sprintf("  CLIPs with same sale date set in both files:  %s\n",
              formatC(same_dates, format = "d", big.mark = ",")))
  cat(sprintf("  CLIPs with different sale date sets:          %s\n",
              formatC(diff_dates, format = "d", big.mark = ",")))
}

# sample of overlapping rows
cat("\nSample of overlapping CLIP rows (first 10 CLIPs, all transactions):\n")
sample_clips <- head(unique(ot_overlap$clip), 10)
print(ot_overlap[ot_overlap$clip %in% sample_clips,
                 c("clip", "source", "sale_derived_date", "sale_amount",
                   "buyer_1_full_name", "seller_1_full_name", "record_action_indicator")])

# record_action_indicator distribution within overlap
cat("\nrecord_action_indicator in overlap subset:\n")
print(table(ot_overlap$record_action_indicator, ot_overlap$source, useNA = "always"))


# ******************************
# 9. Close out ----

message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")

savehistory(paste0(secure, "Process/Fire Investment/Logs/999_dedup_check_history_", timestamp, ".txt"))
sink()
