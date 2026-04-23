

# **************************************************
#                     DETAILS
#
# Purpose:   Load Owner Transfer Files
#            (September 2025 Transfer)
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2025-03-25
# Started:   09/30/2025
# Updated:   MM/DD/YYYY
# **************************************************


# *************
# 1. Setup ----

user   <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root   <- paste0(user, "Institutional Investment/")
secure <- "Y:/Institutional Investment/"

data_input_s <- paste0(secure, "Data/Source/")

transfer_root <- paste0(data_input_s, "Cotality/September 2025 Transfer")

# Set to a number to load just that file, or NULL to load all
load_index <- 1L

# logging
timestamp  <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file   <- paste0(root, "Process/Logs/1-02_log_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)

start_time <- Sys.time()
set.seed(123)

cat("\n====================================================\n")
cat("  1-02_load_owner_transfer.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

# load packages
pkgs <- c("dplyr", "tidyverse", "data.table", "beepr", "R.utils", "ggplot2", "lubridate")
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

csv_files <- unlist(lapply(subdirs, function(d) {
  list.files(d,
             pattern = "^UNIVERSITY_OF_CALIFORNIA.*(OwnerTransfer|property3).*\\.csv$",
             full.names = TRUE, recursive = TRUE, ignore.case = TRUE)
}))

cat(sprintf("Found %d CSV file(s):\n", length(csv_files)))
cat(paste0("  [", seq_along(csv_files), "] ", basename(csv_files), "\n"))

if (length(csv_files) == 0) stop("No matching CSV files found — check path.")

# apply index filter if set
if (!is.null(load_index)) {
  if (load_index > length(csv_files)) stop("load_index out of range.")
  cat(sprintf("\nload_index = %d — loading file [%d] only.\n", load_index, load_index))
  csv_files <- csv_files[load_index]
} else {
  cat("\nload_index = NULL — loading all files.\n")
}


# *****************************************************
# 3. Load files ----
# *****************************************************
ts("Loading files...")

for (f in csv_files) {
  
  base <- basename(f)
  if (grepl("OwnerTransfer", base, ignore.case = TRUE)) {
    suffix   <- regmatches(base, regexpr("\\d{6}(?=_data)", base, perl = TRUE))
    obj_name <- paste0("owner_transfer_", suffix)
  } else if (grepl("property3", base, ignore.case = TRUE)) {
    obj_name <- "property"
  } else {
    warning("Skipping unrecognized file: ", base)
    next
  }
  
  ts(sprintf("  Loading %s  ->  %s", base, obj_name))
  
  df <- as.data.frame(data.table::fread(
    f,
    na.strings = c("", "NA", "N/A", "null"),
    showProgress = TRUE
  ))
  
  cat(sprintf("  %s rows x %d columns\n",
              formatC(nrow(df), format = "d", big.mark = ","), ncol(df)))
  
  assign(obj_name, df, envir = .GlobalEnv)
  rm(df)
}

cat("\nObjects now in environment:\n")
cat(paste0("  ", ls(envir = .GlobalEnv, pattern = "^(owner_transfer_|property)"), "\n"))
message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2),
        " minutes")


gc()

# ---- STOP HERE — sections below not yet in use ----





# ******************************
# 4. Close out ----

cat("\nObjects now in environment:\n")
cat(paste0("  ", ls(envir = .GlobalEnv, pattern = "^(owner_transfer_|property)"), "\n"))

message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2),
        " minutes")

savehistory(paste0(root, "Process/Logs/1-02_history_", timestamp, ".txt"))
sink()