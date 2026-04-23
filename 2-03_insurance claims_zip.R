



# **************************************************
#                     DETAILS
#
# Purpose:   Clean CDI Personal Property Experience (PPE) pivot data
#            to produce a ZIP x year panel of average Coverage A
#            amounts and Coverage A fire losses, kept separate by
#            policy type.
#
#            Variables pulled from PPE pivot:
#              avg_cov_a             -- average Coverage A (structure)
#                                       amount, exposure-weighted by CDI
#                                       within each policy type
#              ncat_cova_fire_losses -- non-catastrophic fire losses ($)
#              ncat_cova_fire_claims -- non-catastrophic fire claim count
#              cat_cova_fire_losses  -- catastrophic fire losses ($)
#              cat_cova_fire_claims  -- catastrophic fire claim count
#
#            Policy types kept separate -- NOT aggregated across types.
#            Aggregating avg_cov_a across policy types would require
#            a policy count weight, and the PPE exposure unit definition
#            has not been confirmed for this data product. If aggregation
#            is needed in future, contact CDI to confirm exposure unit
#            definition before using earned_exp as a weight.
#
#            Policy types retained:
#              HO = homeowners owner-occupied (ISO HO-1/2/3/5/8)
#              CO = condo unit owner (ISO HO-6)
#              DO = dwelling owner-occupied (ISO DP-1/2/3)
#              DT = dwelling tenant-occupied, landlord (ISO DP-1/2/3)
#
#            Excluded:
#              RT = renter contents (ISO HO-4)
#              MH = mobile home (ISO HO-7)
#              FP = farm package
#
#            Policy type definitions from CDI PPE methodology document
#            (Format-of-the-Report-and-Methodology.docx, July 2025).
#
# Inputs:    Data/Derived/Insurance/cdi_pivot_data.csv
# Outputs:   Data/Derived/2-03_ppe_zip.rds       (long panel)
#            Data/Derived/2-03_ppe_zip_wide.rds   (wide panel)
#
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2026-03-19
# Started:   MM/DD/YYYY
# Updated:   MM/DD/YYYY
# **************************************************

# *************
# 1. Setup ----
user         <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root         <- paste0(user, "Fire Investment/")
data_ins     <- paste0(root, "Data/Derived/Insurance/")
data_derived <- paste0(root, "Data/Derived/")

timestamp  <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file   <- paste0(root, "Process/Logs/2-03_log_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)
start_time <- Sys.time()

cat("\n====================================================\n")
cat("  2-03_cdi_ppe_zip.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

set.seed(123)

pkgs <- c("dplyr", "readr", "tidyr")
invisible(lapply(pkgs, function(p) {
  if (!require(p, character.only = TRUE)) {
    install.packages(p); library(p, character.only = TRUE)
  }
}))
rm(pkgs)

ts <- function(label = "") {
  cat(sprintf("\n[%s]  %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), label))
}

KEPT_TYPES <- c("HO", "CO", "DO", "DT")


# *****************************************************
# 2. Load raw pivot data ----
# *****************************************************
ts("Loading PPE pivot data...")

raw <- read_csv(
  paste0(data_ins, "cdi_pivot_data.csv"),
  col_types = cols(.default = "c"),
  show_col_types = FALSE
)

cat(sprintf("  Raw rows: %d\n", nrow(raw)))
cat(sprintf("  Source values: %s\n",
            paste(unique(raw$Source), collapse = ", ")))
cat(sprintf("  Policy types: %s\n",
            paste(sort(unique(raw$`Policy Type`)), collapse = ", ")))
cat(sprintf("  Calendar years: %s\n",
            paste(sort(unique(raw$`Calendar Year`)), collapse = ", ")))


# *****************************************************
# 3. Filter and clean ----
# *****************************************************
ts("Filtering and cleaning...")

ppe <- raw %>%
  filter(
    Source        == "All Companies",
    `Policy Type` %in% KEPT_TYPES
  ) %>%
  transmute(
    zip                   = sprintf("%05.0f", as.numeric(`ZIP Code`)),
    year                  = as.integer(`Calendar Year`) + 2000,
    policy_type           = `Policy Type`,
    county                = County,
    avg_cov_a             = as.numeric(`Avg Cov A`),
    ncat_cova_fire_losses = as.numeric(`NonCat CovA Fire Losses`),
    ncat_cova_fire_claims = as.numeric(`NonCat CovA Fire Count`),
    cat_cova_fire_losses  = as.numeric(`Cat CovA Fire Losses`),
    cat_cova_fire_claims  = as.numeric(`Cat CovA Fire Count`)
  ) %>%
  mutate(
    total_cova_fire_losses = ncat_cova_fire_losses + cat_cova_fire_losses,
    total_cova_fire_claims = ncat_cova_fire_claims + cat_cova_fire_claims
  ) %>%
  arrange(zip, year, policy_type)

cat(sprintf("  After filter: %d rows\n", nrow(ppe)))
cat(sprintf("  Years: %s\n", paste(sort(unique(ppe$year)), collapse = ", ")))
cat(sprintf("  Unique ZIPs: %d\n", n_distinct(ppe$zip)))
cat(sprintf("  Policy types: %s\n",
            paste(sort(unique(ppe$policy_type)), collapse = ", ")))
cat(sprintf("  Excluded: RT, MH, FP\n"))


# *****************************************************
# 4. Spot checks ----
# *****************************************************
ts("Spot checks...")

cat("\n--- Row counts by policy type and year ---\n")
ppe %>%
  count(policy_type, year) %>%
  tidyr::pivot_wider(names_from = year, values_from = n) %>%
  print()

cat("\n--- Average Coverage A by policy type and year (statewide mean) ---\n")
ppe %>%
  group_by(policy_type, year) %>%
  summarise(mean_avg_cov_a = round(mean(avg_cov_a, na.rm = TRUE), 0),
            .groups = "drop") %>%
  tidyr::pivot_wider(names_from = year, values_from = mean_avg_cov_a) %>%
  print()

cat("\n--- Statewide fire losses by policy type and year ---\n")
ppe %>%
  group_by(policy_type, year) %>%
  summarise(
    total_losses_M = round(sum(total_cova_fire_losses, na.rm = TRUE) / 1e6, 0),
    total_claims   = sum(total_cova_fire_claims, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  print(n = 40)

cat("\n--- CAT vs NonCAT split (HO, statewide by year) ---\n")
ppe %>%
  filter(policy_type == "HO") %>%
  group_by(year) %>%
  summarise(
    ncat_M = round(sum(ncat_cova_fire_losses, na.rm = TRUE) / 1e6, 0),
    cat_M  = round(sum(cat_cova_fire_losses,  na.rm = TRUE) / 1e6, 0),
    .groups = "drop"
  ) %>%
  mutate(cat_pct = round(100 * cat_M / (ncat_M + cat_M), 1)) %>%
  print()

cat("\n--- NAs by column ---\n")
ppe %>%
  summarise(across(everything(), ~sum(is.na(.)))) %>%
  tidyr::pivot_longer(everything(), names_to = "col", values_to = "n_na") %>%
  filter(n_na > 0) %>%
  print()

cat("\n--- Sample: Butte County 2020 (Camp Fire year), HO ---\n")
ppe %>%
  filter(county == "Butte", year == 2020, policy_type == "HO") %>%
  select(zip, avg_cov_a, ncat_cova_fire_losses,
         cat_cova_fire_losses, total_cova_fire_claims) %>%
  arrange(desc(cat_cova_fire_losses)) %>%
  print()


# *****************************************************
# 5. Save ----
# *****************************************************
ts("Saving outputs...")

# (A) long panel: one row per ZIP x year x policy_type
saveRDS(ppe, paste0(data_derived, "2-03_ppe_zip.rds"))
cat(sprintf("  (A) Saved 2-03_ppe_zip.rds: %d rows x %d cols\n",
            nrow(ppe), ncol(ppe)))

# (B) wide panel: one row per ZIP
# columns: {policy_type}_{variable}_{year}
# e.g. HO_avg_cov_a_2021, DT_cat_cova_fire_losses_2022
ppe_wide <- ppe %>%
  select(zip, county, year, policy_type,
         avg_cov_a,
         ncat_cova_fire_losses, ncat_cova_fire_claims,
         cat_cova_fire_losses,  cat_cova_fire_claims,
         total_cova_fire_losses, total_cova_fire_claims) %>%
  pivot_wider(
    id_cols     = c(zip, county),
    names_from  = c(policy_type, year),
    values_from = c(avg_cov_a,
                    ncat_cova_fire_losses, ncat_cova_fire_claims,
                    cat_cova_fire_losses,  cat_cova_fire_claims,
                    total_cova_fire_losses, total_cova_fire_claims),
    names_glue  = "{policy_type}_{.value}_{year}",
    names_sort  = TRUE
  ) %>%
  arrange(zip)

saveRDS(ppe_wide, paste0(data_derived, "2-03_ppe_zip_wide.rds"))
cat(sprintf("  (B) Saved 2-03_ppe_zip_wide.rds: %d rows x %d cols\n",
            nrow(ppe_wide), ncol(ppe_wide)))


# *****************************************************
# 6. Close out ----
# *****************************************************
cat("\n\n")
cat(strrep("=", 70), "\n")
ts("DONE")
message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2),
        " minutes")
cat(strrep("=", 70), "\n")
savehistory(paste0(root, "Process/Logs/2-03_history_", timestamp, ".txt"))
sink()


