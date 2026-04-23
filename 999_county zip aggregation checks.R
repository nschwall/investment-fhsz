

# **************************************************
#                     DETAILS
#
# Purpose:   Check whether CDI county-level policy counts are simply
#            the sum of ZIP-level counts (with cross-county ZIPs
#            arbitrarily assigned to one county), or represent an
#            independent count that correctly handles ZIPs crossing
#            county lines.
#
#            Step 1: Check if any ZIPs appear in multiple counties
#                    in either ZIP file (would indicate CDI splits
#                    cross-county ZIPs across counties at ZIP level)
#            Step 2: Sum ZIP-level data to county x year
#            Step 3: Compare against CDI's published county file
#                    (the PDF, read in here as a CSV you create)
#
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2026-03-18
# Started:   MM/DD/YYYY
# **************************************************

# *************
# 1. Setup ----
user         <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root         <- paste0(user, "Fire Investment/")
data_source  <- paste0(root, "Data/Source/Insurance/")
data_derived <- paste0(root, "Data/Derived/Insurance/")

pkgs <- c("dplyr", "readxl", "readr", "tidyr", "tools")
invisible(lapply(pkgs, function(p) {
  if (!require(p, character.only = TRUE)) {
    install.packages(p); library(p, character.only = TRUE)
  }
}))
rm(pkgs)

clean_num <- function(x) {
  x <- gsub(",", "", as.character(x))
  x[x %in% c("-", "NA", "")] <- NA
  as.numeric(x)
}
title_county <- function(x) tools::toTitleCase(tolower(trimws(x)))

cat("\n====================================================\n")
cat("  999_county_zip_rollup_check.R\n")
cat("====================================================\n\n")


# *****************************************************
# 2. Load ZIP files (with county column retained) ----
# *****************************************************
# The county column in the ZIP files is CDI's own assignment --
# one county per ZIP. We keep it here specifically to check
# whether any ZIP appears under multiple counties.

cat("Loading ZIP files (retaining county column)...\n")

raw_2023 <- read_excel(
  paste0(data_source, "Residential-Insurance-Voluntary-Market-New-Renew-NonRenew-by-ZIP-2020-2023.xlsx"),
  sheet     = "New Renew Nonrenew",
  col_types = "text"
) %>%
  transmute(
    county    = title_county(`County`),
    zip       = sprintf("%05s", trimws(`ZIP Code`)),
    year      = as.integer(`Year`),
    vol_new   = clean_num(`New`),
    vol_renew = clean_num(`Renewed`),
    vol_non   = clean_num(`Non-Renewed`)
  )

raw_2021 <- read_excel(
  paste0(data_source, "Residential-Property-Voluntary-Market-New-Renew-NonRenew-by-ZIP-2015-2021.xlsx"),
  sheet     = "New Renew NonRenew",
  col_types = "text"
) %>%
  transmute(
    county    = title_county(`County`),
    zip       = sprintf("%05s", trimws(`ZIP Code`)),
    year      = as.integer(`Year`),
    vol_new   = clean_num(`New`),
    vol_renew = clean_num(`Renewed`),
    vol_non   = clean_num(as.numeric(clean_num(`Insurer-Initiated Nonrenewed`)) +
                            as.numeric(clean_num(`Insured-Initiated Nonrenewed`)))
  )

cat(sprintf("  2020-2023: %d rows, %d unique ZIPs\n",
            nrow(raw_2023), n_distinct(raw_2023$zip)))
cat(sprintf("  2015-2021: %d rows, %d unique ZIPs\n",
            nrow(raw_2021), n_distinct(raw_2021$zip)))


# *****************************************************
# 3. Step 1: Are any ZIPs split across counties? ----
# *****************************************************
# For each ZIP, find how many distinct counties it appears under.
# If > 1: CDI splits that ZIP across counties at the ZIP level.
# If all = 1: CDI assigns each ZIP to exactly one county.

cat("\n====================================================\n")
cat("STEP 1: ZIP-to-county assignment check\n")
cat("====================================================\n")

check_zip_counties <- function(df, label) {
  zip_county_map <- df %>%
    filter(!is.na(county), county != "") %>%
    distinct(zip, county) %>%
    group_by(zip) %>%
    summarise(n_counties = n_distinct(county),
              counties   = paste(sort(county), collapse = " | "),
              .groups    = "drop")
  
  n_split <- sum(zip_county_map$n_counties > 1)
  cat(sprintf("\n  [%s]\n", label))
  cat(sprintf("  Total unique ZIPs:              %d\n", nrow(zip_county_map)))
  cat(sprintf("  ZIPs assigned to 1 county:      %d\n", sum(zip_county_map$n_counties == 1)))
  cat(sprintf("  ZIPs assigned to 2+ counties:   %d\n", n_split))
  
  if (n_split > 0) {
    cat("\n  ZIPs appearing in multiple counties:\n")
    zip_county_map %>%
      filter(n_counties > 1) %>%
      arrange(desc(n_counties), zip) %>%
      print(n = 30)
  } else {
    cat("  -> Each ZIP assigned to exactly one county. No splits found.\n")
  }
  
  invisible(zip_county_map)
}

map_2023 <- check_zip_counties(raw_2023, "2020-2023 file")
map_2021 <- check_zip_counties(raw_2021, "2015-2021 file")

# Combined across both files
cat("\n  [Combined across both files]\n")
map_combined <- bind_rows(
  raw_2023 %>% distinct(zip, county),
  raw_2021 %>% distinct(zip, county)
) %>%
  filter(!is.na(county), county != "") %>%
  distinct(zip, county) %>%
  group_by(zip) %>%
  summarise(n_counties = n_distinct(county),
            counties   = paste(sort(county), collapse = " | "),
            .groups    = "drop")

n_split_combined <- sum(map_combined$n_counties > 1)
cat(sprintf("  ZIPs with different county assignments across the two files: %d\n",
            n_split_combined))
if (n_split_combined > 0) {
  cat("  (This would mean CDI reassigned a ZIP between report vintages)\n")
  map_combined %>% filter(n_counties > 1) %>% print(n = 20)
} else {
  cat("  -> County assignments are consistent across both files.\n")
}


# *****************************************************
# 4. Step 2: Sum ZIPs to county x year ----
# *****************************************************
# Use each file's own county assignment to roll up to county.
# This is what a naive county aggregation from ZIP data would give you.

cat("\n====================================================\n")
cat("STEP 2: Roll up ZIP data to county x year\n")
cat("====================================================\n")

zip_to_county <- function(df, label, years_keep) {
  df %>%
    filter(year %in% years_keep,
           !is.na(county), county != "") %>%
    group_by(county, year) %>%
    summarise(
      vol_new_zip   = sum(vol_new,   na.rm = TRUE),
      vol_renew_zip = sum(vol_renew, na.rm = TRUE),
      vol_non_zip   = sum(vol_non,   na.rm = TRUE),
      .groups = "drop"
    )
}

# Use 2020-2023 doc for 2020+, 2015-2021 doc for 2015-2019
zip_rolled_2023 <- zip_to_county(raw_2023, "2020-2023", 2020:2023)
zip_rolled_2021 <- zip_to_county(raw_2021, "2015-2021", 2015:2019)

zip_rolled <- bind_rows(zip_rolled_2023, zip_rolled_2021) %>%
  arrange(county, year)

cat(sprintf("  ZIP-rolled panel: %d rows | %d counties | years: %s\n",
            nrow(zip_rolled),
            n_distinct(zip_rolled$county),
            paste(sort(unique(zip_rolled$year)), collapse = ", ")))


# *****************************************************
# 5. Step 3: Load county file ----
# *****************************************************
# Load the county-level data. This script expects ins_county.rds
# produced by 2-01. If that hasn't been run yet, load the CSVs directly.

cat("\n====================================================\n")
cat("STEP 3: Load county file and compare\n")
cat("====================================================\n")

rds_path <- paste0(data_derived, "ins_county.rds")

if (file.exists(rds_path)) {
  cat("  Loading ins_county.rds from 2-01...\n")
  ins_county_wide <- readRDS(rds_path)
  
  # Pivot back to long for comparison
  county_long <- ins_county_wide %>%
    pivot_longer(
      cols         = -county,
      names_to     = c(".value", "year"),
      names_pattern = "(.+)_(\\d{4})"
    ) %>%
    mutate(year = as.integer(year)) %>%
    filter(!is.na(vol_non))   # drop years with no data
  
} else {
  stop("ins_county.rds not found. Run 2-01 first, or adapt this script to load CSVs directly.")
}

cat(sprintf("  County panel: %d rows | %d counties | years: %s\n",
            nrow(county_long),
            n_distinct(county_long$county),
            paste(sort(unique(county_long$year)), collapse = ", ")))


# *****************************************************
# 6. Compare ZIP rollup vs. county file ----
# *****************************************************

cat("\n--- Joining ZIP rollup to county file ---\n")

comp <- county_long %>%
  select(county, year, vol_new, vol_renew, vol_non) %>%
  inner_join(zip_rolled, by = c("county", "year")) %>%
  mutate(
    diff_new   = vol_new   - vol_new_zip,
    diff_renew = vol_renew - vol_renew_zip,
    diff_non   = vol_non   - vol_non_zip,
    pct_diff_non = 100 * diff_non / pmax(vol_non, 1)
  )

n_matched    <- nrow(comp)
n_county_only <- nrow(county_long) - n_matched
n_zip_only    <- nrow(zip_rolled)  - n_matched

cat(sprintf("  Matched county-year rows:   %d\n", n_matched))
cat(sprintf("  County file only:           %d\n", n_county_only))
cat(sprintf("  ZIP rollup only:            %d\n", n_zip_only))

cat("\n--- Agreement by column ---\n")
check_col <- function(diff_vec, label) {
  n_exact  <- sum(diff_vec == 0, na.rm = TRUE)
  n_total  <- sum(!is.na(diff_vec))
  max_diff <- max(abs(diff_vec), na.rm = TRUE)
  mean_diff <- mean(abs(diff_vec), na.rm = TRUE)
  cat(sprintf("  %-15s  exact=%d/%d (%.1f%%)  max_diff=%s  mean_diff=%.1f\n",
              label, n_exact, n_total, 100*n_exact/n_total,
              formatC(max_diff, format="d", big.mark=","),
              mean_diff))
}
check_col(comp$diff_new,   "vol_new")
check_col(comp$diff_renew, "vol_renew")
check_col(comp$diff_non,   "vol_non")

cat("\n--- Year-level totals comparison ---\n")
comp %>%
  group_by(year) %>%
  summarise(
    county_vol_non = sum(vol_non,     na.rm = TRUE),
    zip_vol_non    = sum(vol_non_zip, na.rm = TRUE),
    diff           = county_vol_non - zip_vol_non,
    pct_diff       = 100 * diff / pmax(county_vol_non, 1),
    .groups = "drop"
  ) %>%
  print()

cat("\n--- Largest discrepancies (vol_non, any year) ---\n")
comp %>%
  filter(abs(diff_non) > 0) %>%
  arrange(desc(abs(diff_non))) %>%
  select(county, year, vol_non, vol_non_zip, diff_non, pct_diff_non) %>%
  head(20) %>%
  print()

cat("\n====================================================\n")
cat("INTERPRETATION GUIDE\n")
cat("====================================================\n")
cat("
If exact match rates are ~100% and year totals agree:
  -> County file IS just a ZIP rollup using CDI's own county assignments.
     Cross-county ZIPs are handled by arbitrary single-county assignment
     upstream (at the ZIP file level). No independent county count exists.

If there are systematic differences:
  -> County file uses a different (possibly cleaner) methodology.
     Differences may reflect cross-county ZIP corrections or a separate
     data collection. In that case, prefer the county file for county-level
     analysis and the ZIP file for ZIP-level analysis -- do not try to
     reconcile them.

If Step 1 found ZIPs assigned to multiple counties:
  -> CDI splits those ZIPs across counties at the ZIP level. The rollup
     would then correctly apportion policies across counties for those ZIPs,
     and you would expect the county file to match the rollup.
")

cat("\nDone.\n")