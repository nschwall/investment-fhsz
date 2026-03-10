

# **************************************************
#                     DETAILS
#
# Purpose:   Explore Owner Transfer Source Files (September 2025 Transfer)
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2025-03-10
# Started:   MM/DD/YYYY
# Updated:   MM/DD/YYYY
# **************************************************

# *************
# 1. Setup ----
user   <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root   <- paste0(user, "Institutional Investment/")
secure <- "Y:/Institutional Investment/"

data_input_s  <- paste0(secure, "Data/Source/")
data_output_s <- paste0(secure, "Data/Derived/")

transfer_root  <- paste0(data_input_s, "Cotality/September 2025 Transfer")
sample_out_dir <- paste0(data_output_s, "Test Samples")

# logging
timestamp  <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file   <- paste0(root, "Process/Logs/1-00_log_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)
start_time <- Sys.time()

cat("\n====================================================\n")
cat("  1-00_exploration.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

set.seed(123)

# load packages
pkgs <- c("dplyr", "readxl", "lubridate")
invisible(lapply(pkgs, function(p) {
  if (!require(p, character.only = TRUE)) {
    install.packages(p)
    library(p, character.only = TRUE)
  }
}))
rm(pkgs)

# helper: print current timestamp
ts <- function(label = "") {
  cat(sprintf("\n[%s]  %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), label))
}

# ensure output directory exists
if (!dir.exists(sample_out_dir)) dir.create(sample_out_dir, recursive = TRUE)

# *****************************************************
# 2. Discover Excel files ----
# *****************************************************
ts("Scanning for Excel files...")

skip_dirs  <- c("Zip Folders", "Meta Data")
subdirs    <- list.dirs(transfer_root, full.names = TRUE, recursive = FALSE)
subdirs    <- subdirs[!basename(subdirs) %in% skip_dirs]

cat("Subfolders to scan:\n")
cat(paste0("  ", basename(subdirs), "\n"))

excel_files <- unlist(lapply(subdirs, function(d) {
  list.files(d, pattern = "\\.(xlsx|xls)$", full.names = TRUE,
             recursive = TRUE, ignore.case = TRUE)
}))

cat(sprintf("\nFound %d Excel file(s):\n", length(excel_files)))
cat(paste0("  ", basename(excel_files), "\n"))

if (length(excel_files) == 0) stop("No Excel files found — check path.")

# *****************************************************
# 3. Loop over each file ----
# *****************************************************

for (f in excel_files) {
  
  file_base <- tools::file_path_sans_ext(basename(f))
  
  cat("\n\n")
  cat(strrep("=", 70), "\n")
  ts(paste("FILE:", basename(f)))
  cat(strrep("=", 70), "\n")
  
  # --- 3a. Load ----
  ts("  Loading...")
  sheets <- readxl::excel_sheets(f)
  
  if (length(sheets) > 1) {
    warning("Multiple sheets found in ", basename(f), ": ",
            paste(sheets, collapse = ", "), " — reading sheet 1 only.")
  }
  
  cat(sprintf("  Sheet(s) found: %s\n", paste(sheets, collapse = ", ")))
  
  df <- as.data.frame(readxl::read_excel(f, sheet = 1, guess_max = 10000))
  
  cat(sprintf("  Dimensions: %d rows x %d columns\n", nrow(df), ncol(df)))
  
  # --- 3b. Column names ----
  cat("\n--- COLUMN NAMES ---\n")
  print(colnames(df))
  
  # --- 3c. Head (untruncated) ----
  cat("\n--- HEAD (first 6 rows, all columns) ---\n")
  old_width <- getOption("width")
  options(width = 300)
  print(head(df), row.names = FALSE)
  options(width = old_width)
  
  # --- 3d. Summary stats by type ----
  ts("  Running summary stats...")
  
  num_cols  <- names(df)[sapply(df, function(x) is.numeric(x) || is.integer(x))]
  date_cols <- names(df)[sapply(df, function(x) inherits(x, c("Date", "POSIXct", "POSIXlt")))]
  chr_cols  <- names(df)[sapply(df, function(x) is.character(x) || is.factor(x) || is.logical(x))]
  
  # Numeric / integer
  if (length(num_cols) > 0) {
    cat("\n--- NUMERIC / INTEGER FIELDS ---\n")
    for (col in num_cols) {
      cat(sprintf("\n  >> %s\n", col))
      vals <- df[[col]]
      cat(sprintf("     N = %d  |  NAs = %d  (%.1f%%)\n",
                  length(vals), sum(is.na(vals)), 100 * mean(is.na(vals))))
      if (sum(!is.na(vals)) > 0) print(summary(vals))
    }
  }
  
  # Date
  if (length(date_cols) > 0) {
    cat("\n--- DATE FIELDS ---\n")
    for (col in date_cols) {
      cat(sprintf("\n  >> %s\n", col))
      vals <- df[[col]]
      cat(sprintf("     N = %d  |  NAs = %d  (%.1f%%)\n",
                  length(vals), sum(is.na(vals)), 100 * mean(is.na(vals))))
      if (sum(!is.na(vals)) > 0) print(summary(vals))
    }
  }
  
  # Character / factor / logical -> table (cap at 50 unique values)
  if (length(chr_cols) > 0) {
    cat("\n--- CHARACTER / FACTOR / LOGICAL FIELDS ---\n")
    for (col in chr_cols) {
      vals   <- df[[col]]
      n_uniq <- length(unique(na.omit(vals)))
      cat(sprintf("\n  >> %s  (%d unique non-NA values, %d NAs)\n",
                  col, n_uniq, sum(is.na(vals))))
      if (n_uniq <= 50) {
        print(table(vals, useNA = "always"))
      } else {
        cat("     (> 50 unique values — showing top 20 by frequency)\n")
        tbl <- sort(table(vals, useNA = "always"), decreasing = TRUE)
        print(head(tbl, 20))
      }
    }
  }
  
  # --- 3e. Random 1k subsample & save ----
  ts("  Saving 1k subsample...")
  
  n_sample  <- min(1000L, nrow(df))
  df_sample <- df[sample(nrow(df), n_sample), ]
  
  out_path <- file.path(sample_out_dir, paste0("1-00_", file_base, ".rds"))
  saveRDS(df_sample, file = out_path)
  cat(sprintf("  Saved %d-row sample -> %s\n", n_sample, out_path))
  
  # --- 3f. Clean up ----
  ts("  Cleaning up memory...")
  rm(df, df_sample)
  gc()
  
  cat(sprintf("\n  Time since start: %.1f min\n",
              as.numeric(difftime(Sys.time(), start_time, units = "mins"))))
}

# ******************************
# 4. Close out ----
cat("\n\n")
cat(strrep("=", 70), "\n")
ts("ALL FILES DONE")
message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2),
        " minutes")
cat(strrep("=", 70), "\n")

savehistory(paste0(root, "Process/Logs/1-00_history_", timestamp, ".txt"))
sink()