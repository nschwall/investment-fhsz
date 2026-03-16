# ============================================================
# compare_cdi_versions.R
# Compares:
#   (A) cdi_pivot_data.csv  -- extracted from pivot cache
#   (B) Residential Property Coverage_clean.xlsx -- your clean version
# ============================================================

setwd("C:/Users/Nora Schwaller/Dropbox (Personal)/Fire Investment/Data/Derived/Insurance")

library(tidyverse)
library(readxl)

# ---- 1. Load pivot extraction (A) --------------------------

cat("Loading pivot extraction...\n")

pivot_raw <- read_csv("cdi_pivot_data.csv", show_col_types = FALSE)

pivot <- pivot_raw %>%
  filter(Source == "All Companies") %>%
  transmute(
    year   = as.integer(`Calendar Year`) + 2000,
    zip    = sprintf("%05.0f", `ZIP Code`),
    ptype  = `Policy Type`,
    county = County,
    prem_pivot            = as.numeric(`Earned Prem`),
    exp_pivot             = as.numeric(`Earned Exp`),
    ncat_fire_claims_pivot = as.numeric(`NonCat CovA Fire Count`),
    ncat_fire_loss_pivot   = as.numeric(`NonCat CovA Fire Losses`),
    ncat_smoke_claims_pivot = as.numeric(`NonCat Cov A Smoke Count`),
    ncat_smoke_loss_pivot   = as.numeric(`NonCat CovA Smoke Losses`),
    cat_fire_claims_pivot  = as.numeric(`Cat CovA Fire Count`),
    cat_fire_loss_pivot    = as.numeric(`Cat CovA Fire Losses`)
  )

cat(sprintf("Pivot rows (All Companies): %d\n", nrow(pivot)))
cat("Policy types in pivot:", paste(sort(unique(pivot$ptype)), collapse=", "), "\n")


# ---- 2. Load clean version (B) -----------------------------

cat("\nLoading clean version...\n")

raw_rows <- read_excel(
  "Residential Property Coverage_clean.xlsx",
  sheet = "All Companies",
  col_names = TRUE
) %>%
  rename(key = `Calendar Year`)

years_valid  <- as.character(18:23)
policy_types <- c("CO","DO","DT","FP","HO","MH","RT","Grand Total")

clean_parsed <- raw_rows %>%
  mutate(
    row_type = case_when(
      as.character(key) %in% years_valid                   ~ "year",
      as.character(key) %in% policy_types                  ~ "policy_type",
      grepl("^[0-9]{5}$", as.character(key))              ~ "zip",
      !is.na(key) & !grepl("^[0-9]", as.character(key)) &
        !as.character(key) %in% policy_types               ~ "county",
      TRUE                                                  ~ "other"
    )
  )

current_year <- current_ptype <- current_county <- NA
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
      prem_clean             = as.numeric(clean_parsed$`Earned Premium`[i]),
      exp_clean              = as.numeric(clean_parsed$`Earned Exposure`[i]),
      ncat_fire_claims_clean  = as.numeric(clean_parsed$`Non-Cat Cov A Fire Claims`[i]),
      ncat_fire_loss_clean    = as.numeric(clean_parsed$`Non-Cat Cov A Fire Losses`[i]),
      ncat_smoke_claims_clean = as.numeric(clean_parsed$`Non-Cat Cov A Smoke Claims`[i]),
      ncat_smoke_loss_clean   = as.numeric(clean_parsed$`Non-Cat Cov A Smoke Losses`[i]),
      cat_fire_claims_clean   = as.numeric(clean_parsed$`Cat Cov A Fire Claims`[i]),
      cat_fire_loss_clean     = as.numeric(clean_parsed$`Cat Cov A Fire Losses`[i])
    )
  }
}

clean <- bind_rows(tagged)
cat(sprintf("Clean rows (all policy types): %d\n", nrow(clean)))
cat("Policy types in clean:", paste(sort(unique(clean$ptype)), collapse=", "), "\n")


# ---- 3. Join on year + zip + policy type -------------------

cat("\nJoining on year + zip + policy type...\n")

comp <- full_join(pivot, clean, by = c("year", "zip", "ptype"))

n_both       <- sum(!is.na(comp$prem_pivot) & !is.na(comp$prem_clean))
n_pivot_only <- sum(!is.na(comp$prem_pivot) &  is.na(comp$prem_clean))
n_clean_only <- sum( is.na(comp$prem_pivot) & !is.na(comp$prem_clean))

cat(sprintf("\n--- Row coverage ---\n"))
cat(sprintf("  Matched (in both):   %d\n", n_both))
cat(sprintf("  Pivot only:          %d\n", n_pivot_only))
cat(sprintf("  Clean only:          %d\n", n_clean_only))

matched <- comp %>% filter(!is.na(prem_pivot), !is.na(prem_clean))


# ---- 4. Check each numeric column --------------------------

check_col <- function(a, b, label) {
  diff <- a - b
  exact <- sum(diff == 0, na.rm=TRUE)
  total <- sum(!is.na(diff))
  na_a  <- sum(is.na(a))
  na_b  <- sum(is.na(b))
  cat(sprintf("  %-30s exact=%d/%d (%.1f%%)  max_diff=%.0f  NA_pivot=%d  NA_clean=%d\n",
              label, exact, total, exact/total*100,
              max(abs(diff), na.rm=TRUE), na_a, na_b))
}

cat("\n--- Column-by-column agreement (matched rows) ---\n")
check_col(matched$prem_pivot,              matched$prem_clean,              "Earned Premium")
check_col(matched$exp_pivot,               matched$exp_clean,               "Earned Exposure")
check_col(matched$ncat_fire_claims_pivot,  matched$ncat_fire_claims_clean,  "NonCat CovA Fire Claims")
check_col(matched$ncat_fire_loss_pivot,    matched$ncat_fire_loss_clean,    "NonCat CovA Fire Losses")
check_col(matched$ncat_smoke_claims_pivot, matched$ncat_smoke_claims_clean, "NonCat CovA Smoke Claims")
check_col(matched$ncat_smoke_loss_pivot,   matched$ncat_smoke_loss_clean,   "NonCat CovA Smoke Losses")
check_col(matched$cat_fire_claims_pivot,   matched$cat_fire_claims_clean,   "Cat CovA Fire Claims")
check_col(matched$cat_fire_loss_pivot,     matched$cat_fire_loss_clean,     "Cat CovA Fire Losses")


# ---- 5. Suppressed cell check ------------------------------
# In both versions, small counts are suppressed (shown as - or NA)
# Check: do NAs appear in the same rows in both versions?

cat("\n--- Suppressed cell agreement (NonCat CovA Fire Claims) ---\n")
both_na    <- sum( is.na(matched$ncat_fire_claims_pivot) &  is.na(matched$ncat_fire_claims_clean))
pivot_only_na <- sum( is.na(matched$ncat_fire_claims_pivot) & !is.na(matched$ncat_fire_claims_clean))
clean_only_na <- sum(!is.na(matched$ncat_fire_claims_pivot) &  is.na(matched$ncat_fire_claims_clean))
neither_na <- sum(!is.na(matched$ncat_fire_claims_pivot) & !is.na(matched$ncat_fire_claims_clean))

cat(sprintf("  Both suppressed:     %d\n", both_na))
cat(sprintf("  Pivot suppressed only: %d\n", pivot_only_na))
cat(sprintf("  Clean suppressed only: %d\n", clean_only_na))
cat(sprintf("  Neither suppressed:  %d\n", neither_na))


# ---- 6. Policy type breakdown ------------------------------

cat("\n--- Agreement by policy type (Earned Premium) ---\n")
matched %>%
  group_by(ptype) %>%
  summarise(
    n          = n(),
    exact_prem = sum(prem_pivot == prem_clean, na.rm=TRUE),
    pct_exact  = exact_prem / n * 100,
    max_diff   = max(abs(prem_pivot - prem_clean), na.rm=TRUE),
    .groups = "drop"
  ) %>%
  print()


# ---- 7. Year totals by policy type -------------------------

cat("\n--- Year totals (all policy types, Earned Premium) ---\n")
matched %>%
  group_by(year) %>%
  summarise(
    total_pivot = sum(prem_pivot, na.rm=TRUE),
    total_clean = sum(prem_clean, na.rm=TRUE),
    pct_diff    = (total_pivot - total_clean) / total_clean * 100,
    .groups = "drop"
  ) %>%
  print()

cat("\nDone.\n")