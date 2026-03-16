

# **************************************************
#                     DETAILS
#
# Purpose:   Explore Owner Transfer Source Files (September 2025 Transfer)
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2025-03-12
# Started:   MM/DD/YYYY
# Updated:   MM/DD/YYYY
# **************************************************

# *************
# 1. Setup ----
user   <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root   <- paste0(user, "Institutional Investment/")
secure <- "Y:/Institutional Investment/"

data_input_s  <- paste0(secure, "Data/Source/")

transfer_root <- paste0(data_input_s, "Cotality/September 2025 Transfer")

# logging — secure path because output will contain PII (names etc.)
timestamp  <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file   <- paste0(secure, "Process/Logs/1-00_log_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)
start_time <- Sys.time()

cat("\n====================================================\n")
cat("  1-00_exploration.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

set.seed(123)

# load packages
pkgs <- c("dplyr", "data.table", "lubridate")
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

# *****************************************************
# 2. Discover CSV files ----
# *****************************************************
ts("Scanning for CSV files...")

skip_dirs <- c("Zip Folders", "Meta Data")
subdirs   <- list.dirs(transfer_root, full.names = TRUE, recursive = FALSE)
subdirs   <- subdirs[!basename(subdirs) %in% skip_dirs]

cat("Subfolders to scan:\n")
cat(paste0("  ", basename(subdirs), "\n"))

csv_files <- unlist(lapply(subdirs, function(d) {
  list.files(d, pattern = "^UNIVERSITY_OF_CALIFORNIA.*\\.csv$",
             full.names = TRUE, recursive = TRUE, ignore.case = TRUE)
}))

cat(sprintf("\nFound %d CSV file(s):\n", length(csv_files)))
cat(paste0("  ", basename(csv_files), "\n"))

if (length(csv_files) == 0) stop("No matching CSV files found — check path.")

# *****************************************************
# 3. Loop over each file ----
# *****************************************************

n_rows <- 15000L  # rows to load from top of each file

for (f in csv_files) {
  
  file_base <- tools::file_path_sans_ext(basename(f))
  subfolder <- basename(dirname(f))
  
  cat("\n\n")
  cat(strrep("=", 70), "\n")
  ts(paste("FILE:", basename(f)))
  cat(sprintf("  Subfolder : %s\n", subfolder))
  cat(strrep("=", 70), "\n")
  
  # --- 3a. Load ----
  ts(sprintf("  Loading first %s rows...",
             formatC(n_rows, format = "d", big.mark = ",")))
  
  df <- as.data.frame(data.table::fread(
    f, nrows = n_rows,
    na.strings = c("", "NA", "N/A", "null"),
    showProgress = FALSE
  ))
  
  cat(sprintf("  Loaded: %s rows x %d columns\n",
              formatC(nrow(df), format = "d", big.mark = ","), ncol(df)))
  
  # --- 3b. Duplicate column names ----
  dup_names <- names(df)[duplicated(names(df))]
  if (length(dup_names) > 0) {
    warning("Duplicate column names detected: ", paste(dup_names, collapse = ", "))
  } else {
    cat("  No duplicate column names.\n")
  }
  
  # --- 3c. Fully empty columns ----
  empty_cols <- names(df)[sapply(df, function(x) all(is.na(x)))]
  if (length(empty_cols) > 0) {
    cat(sprintf("  WARNING: %d fully empty column(s): %s\n",
                length(empty_cols), paste(empty_cols, collapse = ", ")))
  } else {
    cat("  No fully empty columns.\n")
  }
  
  # --- 3d. Column names ----
  cat("\n--- COLUMN NAMES ---\n")
  print(colnames(df))
  
  # --- 3e. Head and tail (untruncated) ----
  old_width <- getOption("width")
  options(width = 300)
  
  cat("\n--- HEAD (first 6 rows, all columns) ---\n")
  print(head(df), row.names = FALSE)
  
  cat("\n--- TAIL (last 6 rows of loaded sample, all columns) ---\n")
  print(tail(df), row.names = FALSE)
  
  options(width = old_width)
  
  # --- 3f. Classify columns ----
  num_cols  <- names(df)[sapply(df, function(x) is.numeric(x) || is.integer(x))]
  date_cols <- names(df)[sapply(df, function(x) inherits(x, c("Date", "POSIXct", "POSIXlt")))]
  chr_cols  <- names(df)[sapply(df, function(x) is.character(x) || is.factor(x) || is.logical(x))]
  
  # --- 3g. Summary stats by type ----
  ts("  Running summary stats...")
  
  # Numeric / integer
  if (length(num_cols) > 0) {
    cat("\n--- NUMERIC / INTEGER FIELDS ---\n")
    for (col in num_cols) {
      cat(sprintf("\n  >> %s\n", col))
      vals   <- df[[col]]
      n_na   <- sum(is.na(vals))
      n_zero <- sum(vals == 0, na.rm = TRUE)
      cat(sprintf("     N = %d  |  NAs = %d (%.1f%%)  |  Zeros = %d (%.1f%% of non-NA)\n",
                  length(vals),
                  n_na,   100 * n_na   / length(vals),
                  n_zero, 100 * n_zero / max(1, sum(!is.na(vals)))))
      if (sum(!is.na(vals)) > 0) print(summary(vals))
    }
  }
  
  # Date
  if (length(date_cols) > 0) {
    cat("\n--- DATE FIELDS ---\n")
    for (col in date_cols) {
      cat(sprintf("\n  >> %s\n", col))
      vals <- df[[col]]
      n_na <- sum(is.na(vals))
      cat(sprintf("     N = %d  |  NAs = %d (%.1f%%)\n",
                  length(vals), n_na, 100 * n_na / length(vals)))
      if (sum(!is.na(vals)) > 0) print(summary(vals))
    }
  }
  
  # Character / factor / logical
  if (length(chr_cols) > 0) {
    cat("\n--- CHARACTER / FACTOR / LOGICAL FIELDS ---\n")
    for (col in chr_cols) {
      vals     <- df[[col]]
      n_total  <- length(vals)
      n_na     <- sum(is.na(vals))
      n_uniq   <- length(unique(na.omit(vals)))
      pct_uniq <- round(100 * n_uniq / max(1, n_total - n_na), 1)
      cat(sprintf("\n  >> %s\n", col))
      cat(sprintf("     N = %d  |  NAs = %d (%.1f%%)  |  Unique (non-NA) = %d (%.1f%% of non-NA)\n",
                  n_total, n_na, 100 * n_na / n_total, n_uniq, pct_uniq))
      tbl <- sort(table(vals, useNA = "always"), decreasing = TRUE)
      if (n_uniq <= 15) {
        print(tbl)
      } else {
        cat(sprintf("     (showing top 15 of %d unique values)\n", n_uniq))
        print(head(tbl, 15))
      }
    }
  }
  
  # --- 3h. Clean up ----
  ts("  Cleaning up...")
  
  # out_path <- file.path(paste0(secure, "Data/Derived/Test Samples"), paste0("1-00_", file_base, ".rds"))
  # saveRDS(df, file = out_path)
  # cat(sprintf("  Saved %s-row sample -> %s\n",
  #             formatC(nrow(df), format = "d", big.mark = ","), out_path))
  
  rm(df)
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

savehistory(paste0(secure, "Process/Logs/1-00_history_", timestamp, ".txt"))
sink()