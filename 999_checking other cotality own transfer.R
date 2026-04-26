
# **************************************************
#                     DETAILS
#
# Purpose:   Scratch — quick exploration of owner_transfer_014500
#            to understand date range, column structure, and
#            overlap with owner_transfer_144502
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2026-04-23
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
cat("  999_owner_transfer_014500_explore.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

# load packages
pkgs <- c("dplyr", "data.table", "lubridate", "R.utils")
invisible(lapply(pkgs, function(p) {
  if (!require(p, character.only = TRUE)) {
    install.packages(p)
    library(p, character.only = TRUE)
  }
}))
rm(pkgs)

# timestamp defined AFTER packages load to avoid R.utils masking
timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file  <- paste0(secure, "Process/Fire Investment/Logs/999_owner_transfer_014500_explore_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)

ts <- function(label = "") {
  cat(sprintf("\n[%s]  %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), label))
}

convert_date_cols <- function(df) {
  date_cols <- names(df)[grepl("date", names(df), ignore.case = TRUE)]
  if (length(date_cols) == 0) {
    cat("  No columns with 'date' in name found.\n")
    return(df)
  }
  cat(sprintf("  Found %d date column(s): %s\n", length(date_cols),
              paste(date_cols, collapse = ", ")))
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

recode_corp <- function(corp_col, name_col) {
  dplyr::case_when(
    corp_col == "Y"                     ~ 1L,
    is.na(corp_col) & !is.na(name_col) ~ 0L,
    TRUE                                ~ NA_integer_
  )
}


# *****************************************************
# 2. Discover and load owner_transfer_014500 ----
# *****************************************************
ts("Scanning for CSV files...")

skip_dirs <- c("Zip Folders", "Meta Data")
subdirs   <- list.dirs(transfer_root, full.names = TRUE, recursive = FALSE)
subdirs   <- subdirs[!basename(subdirs) %in% skip_dirs]

csv_files <- unlist(lapply(subdirs, function(d) {
  list.files(d,
             pattern = "^UNIVERSITY_OF_CALIFORNIA.*(OwnerTransfer|property3).*\\.csv$",
             full.names = TRUE, recursive = TRUE, ignore.case = TRUE)
}))
csv_files <- csv_files[!grepl("hist_", csv_files, ignore.case = TRUE)]

f_ot2 <- csv_files[grepl("014500", csv_files)]
ts(sprintf("Loading %s", basename(f_ot2)))

ot2 <- as.data.frame(data.table::fread(
  f_ot2,
  na.strings = c("", "NA", "N/A", "null"),
  showProgress = TRUE
))

cat(sprintf("  %s rows x %d columns\n",
            formatC(nrow(ot2), format = "d", big.mark = ","),
            ncol(ot2)))


# *****************************************************
# 3. Normalize column names ----
# *****************************************************
old_names <- names(ot2)
new_names <- tolower(old_names)
new_names <- gsub("[^a-z0-9]+", "_", new_names)
new_names <- gsub("_+", "_", new_names)
new_names <- sub("^_", "", new_names)
new_names <- sub("_$", "", new_names)
if (any(duplicated(new_names))) stop("Name collision after normalization.")
names(ot2) <- new_names


# *****************************************************
# 4. Convert date columns ----
# *****************************************************
ts("Converting date columns...")
ot2 <- convert_date_cols(ot2)


# *****************************************************
# 5. Column comparison with 144502 ----
# *****************************************************
# check if columns match 144502 — if owner_transfer_144502 is in
# environment from a prior session, compare directly; otherwise
# just print colnames for manual review
cat("\nColumn names in 014500:\n")
print(colnames(ot2))

if (exists("ot")) {
  cat("\nColumn names identical to 144502 (ot)?\n")
  print(identical(names(ot2), names(ot)))
} else {
  cat("\n(owner_transfer_144502 / ot not in environment — load 999_owner_transfer_144502_explore.R first to compare)\n")
}


# *****************************************************
# 6. Date range ----
# *****************************************************
# key question: does 014500 extend the time series, overlap with
# 144502, or cover a different geography?

cat("\nSale date range (sale_derived_date):\n")
print(summary(ot2$sale_derived_date))

cat("\nTransaction batch date range (transaction_batch_date):\n")
print(summary(ot2$transaction_batch_date))

cat("\nSale date histogram by year:\n")
print(table(format(ot2$sale_derived_date, "%Y"), useNA = "always"))


# quick visual of sale date distribution — not saved
library(ggplot2)

p1 <- ggplot(ot2, aes(x = sale_derived_date)) +
  geom_histogram(binwidth = 365, fill = "steelblue", color = "white") +
  scale_x_date(date_breaks = "5 years", date_labels = "%Y") +
  labs(title = "Sale Date Distribution — owner_transfer_014500 (all dates)",
       x = "Sale Date", y = "Count") +
  theme_minimal()

print(p1)

p2 <- ggplot(
  dplyr::filter(ot2, sale_derived_date >= as.Date("2015-01-01")),
  aes(x = sale_derived_date)) +
  geom_histogram(binwidth = 91, fill = "steelblue", color = "white") +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  labs(title = "Sale Date Distribution — owner_transfer_014500 (2015+, 3-month bins)",
       x = "Sale Date", y = "Count") +
  theme_minimal()

print(p2)


# *****************************************************
# 7. CLIP overlap with 144502 ----
# *****************************************************
cat("\nTotal CLIPs in 014500:\n")
print(length(unique(ot2$clip)))

cat("\nUnique CLIPs in 014500:\n")
print(length(unique(ot2$clip)))

if (exists("ot")) {
  cat("\nCLIP overlap between 014500 and 144502:\n")
  overlap <- length(intersect(ot2$clip, ot$clip_id))
  cat(sprintf("  %s CLIPs appear in both files\n",
              formatC(overlap, format = "d", big.mark = ",")))
  cat(sprintf("  %.1f%% of 014500 CLIPs also in 144502\n",
              100 * overlap / length(unique(ot2$clip))))
}






# *****************************************************
# 8. Quick distribution checks ----
# *****************************************************

# recode corp for comparison
ot2$buy1_corp_r <- recode_corp(ot2$buyer_1_corporate_indicator, ot2$buyer_1_full_name)

cat("\nbuy1_corp (recoded):\n")
print(table(ot2$buy1_corp_r, useNA = "always"))

cat("\nresidential x prop_indicator:\n")
print(table(ot2$residential_indicator, ot2$property_indicator_code_static, useNA = "always"))

cat("\nsale_doc_type frequency:\n")
print(sort(table(ot2$sale_document_type_code, useNA = "always"), decreasing = TRUE)[1:20])

cat("\ninterfamily flag:\n")
print(table(ot2$interfamily_related_indicator, useNA = "always"))

cat("\nsitus_state — confirming California only:\n")
print(sort(table(ot2$deed_situs_state_static, useNA = "always"), decreasing = TRUE)[1:10])


# ******************************
# 9. Close out ----

message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")

savehistory(paste0(secure, "Process/Fire Investment/Logs/999_owner_transfer_014500_explore_history_", timestamp, ".txt"))
sink()


