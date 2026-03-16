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
# 5. some looks ----


# ------------------------------------------------------------------------------
# Check whether hist_01 through hist_15 all have the same column structure
# ------------------------------------------------------------------------------

hist_list <- mget(sprintf("hist_%02d", 1:15))

same_colnames <- all(
  sapply(hist_list, function(x) identical(names(x), names(hist_list[[1]])))
)

print(same_colnames)

# ----------------------------------------------------------------------
# Quick checks on TAX YEAR distributions
# ----------------------------------------------------------------------

summary(hist_01$`TAX YEAR`)
summary(hist_02$`TAX YEAR`)
summary(hist_03$`TAX YEAR`)
summary(hist_15$`TAX YEAR`)

# Optional: confirm expected number of unique CLIPs in hist_01
length(unique(hist_01$CLIP))

# ----------------------------------------------------------------------
# Example tabulations from hist_08
# ----------------------------------------------------------------------

table(hist_08$`OWNER OCCUPANCY CODE`, useNA = "always")

table(
  hist_08$`OWNER OCCUPANCY CODE`,
  hist_08$`OWNER 1 CORPORATE INDICATOR`,
  useNA = "always"
)

table(
  hist_08$`OWNER OCCUPANCY CODE`,
  hist_08$`OWNER OWNERSHIP RIGHTS CODE`,
  useNA = "always"
)

table(
  hist_08$`HOMESTEAD EXEMPT INDICATOR`,
  hist_08$`OWNER 1 CORPORATE INDICATOR`,
  useNA = "always"
)

table(
  hist_08$`OWNER OCCUPANCY CODE`,
  hist_08$`HOMESTEAD EXEMPT INDICATOR`,
  useNA = "always"
)

summary(hist_08$`YEAR BUILT`)


# ----------------------------------------------------------------------
# Trying to figure out diff in owner transfer files
# ----------------------------------------------------------------------


identical(
  names(owner_transfer_014500),
  names(owner_transfer_144502)
)

length(intersect(
  owner_transfer_014500$CLIP,
  owner_transfer_144502$CLIP
))


# find overlapping CLIPs
common_clips <- intersect(
  owner_transfer_014500$CLIP,
  owner_transfer_144502$CLIP
)

# add source labels
owner_transfer_014500$source <- "014500"
owner_transfer_144502$source <- "144502"

# bind rows from both datasets for those CLIPs
ot_common <- rbind(
  owner_transfer_014500[owner_transfer_014500$CLIP %in% common_clips, ],
  owner_transfer_144502[owner_transfer_144502$CLIP %in% common_clips, ]
)

# drop rows where CLIP is NA
ot_common <- ot_common[!is.na(ot_common$CLIP), ]

# sort for easier comparison
ot_common <- ot_common[order(ot_common$CLIP), ]

# inspect
View(ot_common)

# see if identical
# split the two sources
ot014500 <- ot_common[ot_common$source == "014500", ]
ot144502 <- ot_common[ot_common$source == "144502", ]

# remove the source column for comparison
ot014500$source <- NULL
ot144502$source <- NULL

# check if the data are identical
identical(ot014500, ot144502)


# ----------------------------------------------------------------------
# Example tabulations from owner_transfer_014500
# ----------------------------------------------------------------------




table(
  owner_transfer_014500$`RESIDENTIAL INDICATOR`,
  owner_transfer_014500$`INVESTOR PURCHASE INDICATOR`,
  useNA = "always"
)

table(
  owner_transfer_014500$`BUYER 1 CORPORATE INDICATOR`,
  owner_transfer_014500$`INVESTOR PURCHASE INDICATOR`,
  useNA = "always"
)



# ----------------------------------------------------------------------
# Checking some dates
# ----------------------------------------------------------------------

owner_transfer_014500$`SALE DERIVED DATE` <- as.Date(
  as.character(owner_transfer_014500$`SALE DERIVED DATE`),
  format = "%Y%m%d"
)

owner_transfer_014500$`TRANSACTION BATCH DATE` <- as.Date(
  as.character(owner_transfer_014500$`TRANSACTION BATCH DATE`),
  format = "%Y%m%d"
)

owner_transfer_014500$`SALE DERIVED RECORDING DATE` <- as.Date(
  as.character(owner_transfer_014500$`SALE DERIVED RECORDING DATE`),
  format = "%Y%m%d"
)

summary(owner_transfer_014500$`SALE DERIVED DATE`)
summary(owner_transfer_014500$`SALE DERIVED RECORDING DATE`)
summary(owner_transfer_014500$`TRANSACTION BATCH DATE`)


summary(as.numeric(owner_transfer_014500$`SALE DERIVED DATE` - owner_transfer_014500$`SALE DERIVED RECORDING DATE`))
summary(as.numeric(owner_transfer_014500$`SALE DERIVED DATE` - owner_transfer_014500$`TRANSACTION BATCH DATE`))
summary(as.numeric(owner_transfer_014500$`SALE DERIVED RECORDING DATE` - owner_transfer_014500$`TRANSACTION BATCH DATE`))



# other owner transfer 
owner_transfer_144502$`SALE DERIVED DATE` <- as.Date(
  as.character(owner_transfer_144502$`SALE DERIVED DATE`),
  format = "%Y%m%d"
)

owner_transfer_144502$`TRANSACTION BATCH DATE` <- as.Date(
  as.character(owner_transfer_144502$`TRANSACTION BATCH DATE`),
  format = "%Y%m%d"
)

owner_transfer_144502$`SALE DERIVED RECORDING DATE` <- as.Date(
  as.character(owner_transfer_144502$`SALE DERIVED RECORDING DATE`),
  format = "%Y%m%d"
)

summary(owner_transfer_144502$`SALE DERIVED DATE`)
summary(owner_transfer_144502$`SALE DERIVED RECORDING DATE`)
summary(owner_transfer_144502$`TRANSACTION BATCH DATE`)


summary(as.numeric(owner_transfer_144502$`SALE DERIVED DATE` - owner_transfer_144502$`SALE DERIVED RECORDING DATE`))
summary(as.numeric(owner_transfer_144502$`SALE DERIVED DATE` - owner_transfer_144502$`TRANSACTION BATCH DATE`))
summary(as.numeric(owner_transfer_144502$`SALE DERIVED RECORDING DATE` - owner_transfer_144502$`TRANSACTION BATCH DATE`))




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