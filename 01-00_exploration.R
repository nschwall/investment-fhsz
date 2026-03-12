

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
pkgs <- c("dplyr", "data.table", "lubridate", "R.utils")
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

n_edge <- 5000L  # rows to pull from each end

for (f in csv_files) {
  
  file_base <- tools::file_path_sans_ext(basename(f))
  
  cat("\n\n")
  cat(strrep("=", 70), "\n")
  ts(paste("FILE:", basename(f)))
  cat(strrep("=", 70), "\n")
  
  # --- 3a. Row count (cheap) ----
  ts("  Counting rows...")
  n_total <- R.utils::countLines(f) - 1L  # subtract header
  cat(sprintf("  Total rows (excl. header): %s\n",
              formatC(n_total, format = "d", big.mark = ",")))
  
  # --- 3b. Load first + last N rows ----
  ts(sprintf("  Loading first and last %s rows...",
             formatC(n_edge, format = "d", big.mark = ",")))
  
  df_head <- as.data.frame(data.table::fread(
    f, nrows = n_edge,
    na.strings = c("", "NA", "N/A", "null"),
    showProgress = FALSE
  ))
  
  if (n_total > n_edge) {
    df_tail <- as.data.frame(data.table::fread(
      f, skip = n_total - n_edge,
      na.strings = c("", "NA", "N/A", "null"),
      showProgress = FALSE,
      col.names = names(df_head)
    ))
    df <- rbind(df_head, df_tail)
    rm(df_head, df_tail)
    cat(sprintf("  Loaded %s rows (%s head + %s tail) of %s total\n",
                formatC(nrow(df), format = "d", big.mark = ","),
                formatC(n_edge,   format = "d", big.mark = ","),
                formatC(n_edge,   format = "d", big.mark = ","),
                formatC(n_total,  format = "d", big.mark = ",")))
  } else {
    df <- df_head
    rm(df_head)
    cat(sprintf("  File has <= %s rows — loaded all %s\n",
                formatC(n_edge,   format = "d", big.mark = ","),
                formatC(nrow(df), format = "d", big.mark = ",")))
  }
  
  cat(sprintf("  Columns: %d\n", ncol(df)))
  
  # --- 3c. Column names ----
  cat("\n--- COLUMN NAMES ---\n")
  print(colnames(df))
  
  # --- 3d. Head (untruncated) ----
  cat("\n--- HEAD (first 6 rows, all columns) ---\n")
  old_width <- getOption("width")
  options(width = 300)
  print(head(df), row.names = FALSE)
  options(width = old_width)
  
  # --- 3e. Summary stats by type ----
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
  
  # --- 3f. Save sample & clean up ----
  ts("  Cleaning up...")
  
  # out_path <- file.path(sample_out_dir, paste0("1-00_", file_base, ".rds"))
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

savehistory(paste0(root, "Process/Logs/1-00_history_", timestamp, ".txt"))
sink()