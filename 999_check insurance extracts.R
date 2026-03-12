# ============================================================
# compare_cdi_versions.R
# Compares:
#   (A) cdi_pivot_data.csv  -- extracted from pivot cache
#   (B) Residential_Property_Coverage_clean.xlsx -- your clean version
# ============================================================

setwd("C:/Users/Nora Schwaller/Dropbox (Personal)/Fire Investment/Data/Derived/Insurance")

library(tidyverse)
library(readxl)

# ---- 1. Load pivot extraction (A) --------------------------

cat("Loading pivot extraction...\n")

pivot_raw <- read_csv("cdi_pivot_data.csv", show_col_types = FALSE)

# Filter to All Companies + HO only, fix year
pivot <- pivot_raw %>%
  filter(Source == "All Companies", `Policy Type` == "HO") %>%
  transmute(
    year    = as.integer(`Calendar Year`) + 2000,
    zip     = sprintf("%05.0f", `ZIP Code`),
    county  = County,
    prem_pivot  = as.numeric(`Earned Prem`),
    exp_pivot   = as.numeric(`Earned Exp`),
    ncat_fire_claims_pivot = as.numeric(`NonCat CovA Fire Count`),
    ncat_fire_loss_pivot   = as.numeric(`NonCat CovA Fire Losses`)
  )

cat(sprintf("Pivot rows (HO, All Companies): %d\n", nrow(pivot)))


# ---- 2. Load clean version (B) -----------------------------
# Structure: year row, then policy type row, then county row, then ZIP rows
# We need to parse the hierarchy to tag each ZIP row with year, policy type, county

cat("\nLoading clean version...\n")

raw_rows <- read_excel(
  "Residential Property Coverage_clean.xlsx",
  sheet = "All Companies",
  col_names = TRUE
) %>%
  rename(key = `Calendar Year`)   # first col is the identifier

# Classify each row
years_valid      <- as.character(18:23)
policy_types     <- c("CO","DO","DT","FP","HO","MH","RT","Grand Total")
grand_total_vals <- c("Grand Total")

clean_parsed <- raw_rows %>%
  mutate(
    row_type = case_when(
      as.character(key) %in% years_valid                        ~ "year",
      as.character(key) %in% policy_types                       ~ "policy_type",
      grepl("^[0-9]{5}$", as.character(key))                   ~ "zip",
      !is.na(key) & !grepl("^[0-9]", as.character(key)) &
        !as.character(key) %in% policy_types                    ~ "county",
      TRUE                                                       ~ "other"
    )
  )

# Walk rows to tag each ZIP with its year, policy type, county
current_year   <- NA
current_ptype  <- NA
current_county <- NA

tagged <- list()
for (i in seq_len(nrow(clean_parsed))) {
  rt  <- clean_parsed$row_type[i]
  key <- as.character(clean_parsed$key[i])
  
  if (rt == "year")        { current_year   <- as.integer(key) + 2000; next }
  if (rt == "policy_type") { current_ptype  <- key;                    next }
  if (rt == "county")      { current_county <- key;                    next }
  if (rt == "zip") {
    tagged[[length(tagged) + 1]] <- tibble(
      year   = current_year,
      ptype  = current_ptype,
      county = current_county,
      zip    = sprintf("%05s", key),
      prem_clean = as.numeric(clean_parsed$`Earned Premium`[i]),
      exp_clean  = as.numeric(clean_parsed$`Earned Exposure`[i]),
      ncat_fire_claims_clean = as.numeric(clean_parsed$`Non-Cat Cov A Fire Claims`[i]),
      ncat_fire_loss_clean   = as.numeric(clean_parsed$`Non-Cat Cov A Fire Losses`[i])
    )
  }
}

clean <- bind_rows(tagged) %>%
  filter(ptype == "HO")

cat(sprintf("Clean rows (HO): %d\n", nrow(clean)))


# ---- 3. Join and compare -----------------------------------

cat("\nJoining on year + zip...\n")

comp <- full_join(pivot, clean, by = c("year", "zip")) %>%
  mutate(
    prem_diff     = prem_pivot - prem_clean,
    prem_pct_diff = prem_diff / prem_clean * 100,
    exp_diff      = exp_pivot - exp_clean
  )

n_both       <- sum(!is.na(comp$prem_pivot) & !is.na(comp$prem_clean))
n_pivot_only <- sum(!is.na(comp$prem_pivot) &  is.na(comp$prem_clean))
n_clean_only <- sum( is.na(comp$prem_pivot) & !is.na(comp$prem_clean))

cat(sprintf("\n--- Coverage ---\n"))
cat(sprintf("  Matched (in both):   %d\n", n_both))
cat(sprintf("  Pivot only:          %d\n", n_pivot_only))
cat(sprintf("  Clean only:          %d\n", n_clean_only))


# ---- 4. Numeric agreement ----------------------------------

matched <- comp %>% filter(!is.na(prem_pivot), !is.na(prem_clean))

cat(sprintf("\n--- Earned Premium agreement (matched rows) ---\n"))
cat(sprintf("  Exact match:         %d / %d (%.1f%%)\n",
            sum(matched$prem_diff == 0, na.rm=TRUE), nrow(matched),
            mean(matched$prem_diff == 0, na.rm=TRUE)*100))
cat(sprintf("  Max abs difference:  %.0f\n", max(abs(matched$prem_diff), na.rm=TRUE)))
cat(sprintf("  Mean abs pct diff:   %.4f%%\n", mean(abs(matched$prem_pct_diff), na.rm=TRUE)))

cat(sprintf("\n--- Earned Exposure agreement ---\n"))
cat(sprintf("  Exact match:         %d / %d (%.1f%%)\n",
            sum(matched$exp_diff == 0, na.rm=TRUE), nrow(matched),
            mean(matched$exp_diff == 0, na.rm=TRUE)*100))
cat(sprintf("  Max abs difference:  %.0f\n", max(abs(matched$exp_diff), na.rm=TRUE)))


# ---- 5. Flag large discrepancies ---------------------------

big_diffs <- matched %>%
  filter(abs(prem_pct_diff) > 1) %>%
  select(year, zip, county.x, prem_pivot, prem_clean, prem_pct_diff) %>%
  arrange(desc(abs(prem_pct_diff)))

cat(sprintf("\n--- Rows with >1%% premium difference: %d ---\n", nrow(big_diffs)))
if (nrow(big_diffs) > 0) {
  print(head(big_diffs, 20))
}


# ---- 6. Year-level totals comparison -----------------------

cat("\n--- Year-level premium totals ---\n")

year_comp <- matched %>%
  group_by(year) %>%
  summarise(
    total_pivot = sum(prem_pivot, na.rm=TRUE),
    total_clean = sum(prem_clean, na.rm=TRUE),
    pct_diff    = (total_pivot - total_clean) / total_clean * 100,
    .groups = "drop"
  )

print(year_comp)

cat("\nDone. If exact match rates are >99% and year totals agree, pivot extraction is clean.\n")