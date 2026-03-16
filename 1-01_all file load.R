# **************************************************
#                     DETAILS
#
# Purpose:   Load Owner Transfer Source Files into Memory
#            (September 2025 Transfer)
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

data_input_s <- paste0(secure, "Data/Source/")

transfer_root <- paste0(data_input_s, "Cotality/September 2025 Transfer")

start_time <- Sys.time()

cat("\n====================================================\n")
cat("  1-01_load.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

# load packages
pkgs <- c("data.table")
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
  list.files(d, pattern = "^UNIVERSITY_OF_CALIFORNIA.*\\.csv$",
             full.names = TRUE, recursive = TRUE, ignore.case = TRUE)
}))

cat(sprintf("Found %d CSV file(s):\n", length(csv_files)))
cat(paste0("  ", basename(csv_files), "\n"))

if (length(csv_files) == 0) stop("No matching CSV files found — check path.")

# *****************************************************
# 3. Name resolver ----
# *****************************************************
resolve_name <- function(path) {
  base <- basename(path)
  if (grepl("hist_property", base, ignore.case = TRUE)) {
    # extract two-digit number: split filename on "_", grab piece after 8-digit dpc number
    parts <- strsplit(base, "_")[[1]]
    num   <- parts[which(nchar(parts) == 8 & grepl("^[0-9]+$", parts)) + 1]
    return(paste0("hist_", num))
  } else if (grepl("OwnerTransfer", base, ignore.case = TRUE)) {
    # extract last 6 digits before _data
    num <- regmatches(base, regexpr("\\d{6}(?=_data)", base, perl = TRUE))
    return(paste0("owner_transfer_", num))
  } else if (grepl("property3", base, ignore.case = TRUE)) {
    return("property")
  } else {
    # fallback: shouldn't hit this given file discovery filter
    warning("Could not resolve name for: ", base, " — skipping.")
    return(NULL)
  }
}

# *****************************************************
# 4. Load files ----
# *****************************************************

n_rows <- 15000L

for (f in csv_files) {
  
  obj_name <- resolve_name(f)
  if (is.null(obj_name)) next
  
  ts(sprintf("  Loading %s  ->  %s", basename(f), obj_name))
  
  df <- as.data.frame(data.table::fread(
    f, nrows = n_rows,
    na.strings = c("", "NA", "N/A", "null"),
    showProgress = FALSE
  ))
  
  cat(sprintf("  %s rows x %d columns\n",
              formatC(nrow(df), format = "d", big.mark = ","), ncol(df)))
  
  assign(obj_name, df, envir = .GlobalEnv)
  rm(df)
}

# ******************************
# 5. Close out ----
cat("\n\n")
cat(strrep("=", 70), "\n")
ts("ALL FILES LOADED")
cat("\nObjects now in environment:\n")
cat(paste0("  ", ls(envir = .GlobalEnv, pattern = "^(hist_|owner_transfer_|property)"), "\n"))
message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2),
        " minutes")
cat(strrep("=", 70), "\n")

beep(3)