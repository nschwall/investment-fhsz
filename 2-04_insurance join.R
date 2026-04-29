

# **************************************************
#                     DETAILS
#
# Purpose:   Construct county-level insurance stress variables from
#            ins_county.rds and ins_county_overlap.rds (2-01), join
#            to California county FIPS via tidycensus, append to
#            1-05_analytic.rds and 1-05_prop_wide.rds, and run
#            Wilcoxon + Cohen's d tests by corporate buyer status.
#
# ---- VARIABLES CONSTRUCTED ----
#
#   nonrenew_rate_{year}  -- sum(vol_non) / sum(vol_renew_lag)
#                            pooled across 3 years within window
#                            (vol_renew_lag = prior-year vol_renew)
#
#   fair_share_{year}     -- sum(fair_new + fair_renew) /
#                            sum(vol_new + vol_renew + fair_new + fair_renew)
#                            pooled across 3 years within window
#
# ---- 3-YEAR WINDOWS (end-year labeled, pooled denominators) ----
#
#   Old doc (CDI 2015-2021 report):
#     _2018 = pool(2016, 2017, 2018)   -- ins_county.rds
#                                          lag for 2016 uses 2015 vol_renew (present)
#     _2019 = pool(2017, 2018, 2019)   -- ins_county.rds
#     _2020 = pool(2018, 2019, 2020)   -- ins_county.rds (2018-2019)
#                                          + ins_county_overlap v2015_ (2020)
#     _2021 = pool(2019, 2020, 2021)   -- ins_county.rds (2019)
#                                          + ins_county_overlap v2015_ (2020-2021)
#
#   New doc (CDI 2020-2023 report):
#     _2022 = pool(2020, 2021, 2022)   -- ins_county.rds
#                                          lag for 2020 uses 2019 vol_renew (old-doc)
#     _2023 = pool(2021, 2022, 2023)   -- ins_county.rds
#                                          lag for 2021 uses 2020 vol_renew (new-doc)
#
#   No window crosses document boundaries.
#   _2017 excluded: lag for 2015 would require 2014 data (not available).
#   New-doc panels passed to make_window() include 2019 (old-doc) as lag
#   anchor for 2020, so the lag is available without crossing doc boundaries
#   for the nonrenew calculation.
#
# ---- TRANSACTION-YEAR MATCHING ----
#
#   sale_yr = year(sale_date)
#   nonrenew_saleyr / fair_saleyr matched to window by sale year:
#     sale_yr <= 2018  -> _2018   (earliest available window)
#     sale_yr == 2019  -> _2019
#     sale_yr == 2020  -> _2020
#     sale_yr == 2021  -> _2021
#     sale_yr == 2022  -> _2022
#     sale_yr >= 2023  -> _2023
#
# ---- STATISTICAL TESTS ----
#
#   Wilcoxon rank-sum + Cohen's d (on ranks) for nonrenew_saleyr and
#   fair_saleyr by buy1_corp (1 = corporate, 0 = non-corporate).
#
# ---- INPUTS ----
#
#   Data/Derived/Insurance/ins_county.rds
#   Data/Derived/Insurance/ins_county_overlap.rds
#   1-05_analytic.rds   (secure drive)
#   1-05_prop_wide.rds  (secure drive)
#
# ---- OUTPUTS ----
#
#   2-04_ins_county_panel.rds   -- county x year annual rates (long)
#   2-04_ins_county_wide.rds    -- county x window 3-yr pooled (wide)
#   2-04_analytic.rds           -- analytic + insurance vars
#   2-04_prop_wide.rds          -- prop_wide + insurance vars
#
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2026-04-29
# Started:   MM/DD/YYYY
# Updated:   MM/DD/YYYY
# **************************************************


# *************
# 1. Setup ----

user    <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root    <- paste0(user, "Fire Investment/")
secure  <- "Y:/Institutional Investment/"

data_ins      <- paste0(root,   "Data/Derived/Insurance/")
data_output_s <- paste0(secure, "Data/Derived/Fire Investment/")

timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file  <- paste0(secure, "Process/Fire Investment/Logs/2-04_log_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)

start_time <- Sys.time()
set.seed(123)

cat("\n====================================================\n")
cat("  2-04_insurance_county_panel.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

pkgs <- c("dplyr", "tidyr", "tidycensus", "stringr", "lubridate")
invisible(lapply(pkgs, function(p) {
  if (!require(p, character.only = TRUE)) {
    install.packages(p); library(p, character.only = TRUE)
  }
}))
rm(pkgs)

ts <- function(label = "") {
  cat(sprintf("\n[%s]  %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), label))
}

cohens_d_ranks <- function(x, g) {
  r  <- rank(x, na.last = "keep")
  m1 <- mean(r[g == 1], na.rm = TRUE); m0 <- mean(r[g == 0], na.rm = TRUE)
  s1 <- sd(r[g == 1],   na.rm = TRUE); s0 <- sd(r[g == 0],   na.rm = TRUE)
  n1 <- sum(!is.na(r[g == 1]));        n0 <- sum(!is.na(r[g == 0]))
  sp <- sqrt(((n1 - 1) * s1^2 + (n0 - 1) * s0^2) / (n1 + n0 - 2))
  (m1 - m0) / sp
}


# *****************************************************
# 2. Load ins_county.rds and pivot to long ----
# *****************************************************

ts("Loading ins_county.rds...")

ins_wide <- readRDS(paste0(data_ins, "ins_county.rds"))
cat(sprintf("  ins_county: %d rows x %d cols\n", nrow(ins_wide), ncol(ins_wide)))

ins_long <- ins_wide %>%
  pivot_longer(
    cols      = -county,
    names_to  = c(".value", "year"),
    names_sep = "_(?=[0-9]{4}$)",
    names_transform = list(year = as.integer)
  ) %>%
  arrange(county, year)

cat(sprintf("  After pivot: %d rows | years: %s\n",
            nrow(ins_long),
            paste(sort(unique(ins_long$year)), collapse = ", ")))
rm(ins_wide)


# *****************************************************
# 3. Load ins_county_overlap.rds -- old-doc 2020 and 2021 ----
# *****************************************************
# v2015_ prefix = old doc values for 2020 and 2021.
# Used only for old-doc windows _2020 and _2021.

ts("Loading ins_county_overlap.rds and extracting old-doc 2020-2021...")

overlap <- readRDS(paste0(data_ins, "ins_county_overlap.rds"))
cat(sprintf("  ins_county_overlap: %d rows x %d cols\n", nrow(overlap), ncol(overlap)))

needed <- c(
  "v2015_vol_new_2020",  "v2015_vol_renew_2020", "v2015_vol_non_2020",
  "v2015_fair_new_2020", "v2015_fair_renew_2020",
  "v2015_vol_new_2021",  "v2015_vol_renew_2021", "v2015_vol_non_2021",
  "v2015_fair_new_2021", "v2015_fair_renew_2021"
)
missing_cols <- setdiff(needed, names(overlap))
if (length(missing_cols) > 0) {
  stop(sprintf("Missing columns in ins_county_overlap.rds: %s",
               paste(missing_cols, collapse = ", ")))
}
cat("  Required columns confirmed.\n")

overlap_old <- bind_rows(
  overlap %>% transmute(
    county, year = 2020L,
    vol_new = v2015_vol_new_2020, vol_renew = v2015_vol_renew_2020,
    vol_non = v2015_vol_non_2020, fair_new  = v2015_fair_new_2020,
    fair_renew = v2015_fair_renew_2020
  ),
  overlap %>% transmute(
    county, year = 2021L,
    vol_new = v2015_vol_new_2021, vol_renew = v2015_vol_renew_2021,
    vol_non = v2015_vol_non_2021, fair_new  = v2015_fair_new_2021,
    fair_renew = v2015_fair_renew_2021
  )
)

cat(sprintf("  Old-doc 2020-2021 rows: %d\n", nrow(overlap_old)))
rm(overlap, needed, missing_cols)


# *****************************************************
# 4. Build doc-specific long panels ----
# *****************************************************

ts("Building doc-specific long panels...")

# Old-doc panel: 2015-2019 from ins_long + 2020-2021 from overlap_old
ins_long_old <- bind_rows(
  ins_long %>% filter(year %in% 2015:2019),
  overlap_old
) %>% arrange(county, year)

# New-doc panel: 2020-2023 from ins_long
ins_long_new <- ins_long %>% filter(year %in% 2020:2023)

cat(sprintf("  Old-doc panel: %d rows | years: %s\n",
            nrow(ins_long_old),
            paste(sort(unique(ins_long_old$year)), collapse = ", ")))
cat(sprintf("  New-doc panel: %d rows | years: %s\n",
            nrow(ins_long_new),
            paste(sort(unique(ins_long_new$year)), collapse = ", ")))

rm(overlap_old)


# *****************************************************
# 5. Annual rates for panel output ----
# *****************************************************

ts("Computing annual rates (for panel output, new-doc values throughout)...")

ins_rates <- ins_long %>%
  arrange(county, year) %>%
  group_by(county) %>%
  mutate(
    vol_renew_lag = lag(vol_renew, n = 1),
    nonrenew_rate = if_else(
      !is.na(vol_non) & !is.na(vol_renew_lag) & vol_renew_lag > 0,
      vol_non / vol_renew_lag, NA_real_
    ),
    fair_denom = vol_new + vol_renew + fair_new + fair_renew,
    fair_share = if_else(
      !is.na(fair_denom) & fair_denom > 0,
      (fair_new + fair_renew) / fair_denom, NA_real_
    )
  ) %>%
  ungroup() %>%
  select(county, year, vol_new, vol_renew, vol_non, fair_new, fair_renew,
         vol_renew_lag, nonrenew_rate, fair_share)

cat(sprintf("  Annual panel: %d rows\n", nrow(ins_rates)))


# *****************************************************
# 6. Pooled 3-year window function ----
# *****************************************************
# Approach B: pool numerators and denominators across the window years.
#
# nonrenew_rate = sum(vol_non_t) / sum(vol_renew_{t-1})
#   Lag computed within the panel passed in before filtering to window years.
#   Panel passed in must include the year prior to window start for lag.
#
# fair_share = sum(fair_new + fair_renew) /
#              sum(vol_new + vol_renew + fair_new + fair_renew)
#
# Returns NA for a county if any window year is missing.

make_window <- function(panel, years, end_label) {
  lagged <- panel %>%
    arrange(county, year) %>%
    group_by(county) %>%
    mutate(vol_renew_lag = lag(vol_renew, n = 1)) %>%
    ungroup() %>%
    filter(year %in% years)
  
  lagged %>%
    group_by(county) %>%
    summarise(
      n_years           = n(),
      sum_vol_non       = sum(vol_non,       na.rm = FALSE),
      sum_vol_renew_lag = sum(vol_renew_lag, na.rm = FALSE),
      sum_fair          = sum(fair_new + fair_renew, na.rm = FALSE),
      sum_all           = sum(vol_new + vol_renew + fair_new + fair_renew, na.rm = FALSE),
      .groups = "drop"
    ) %>%
    transmute(
      county,
      !!paste0("nonrenew_rate_", end_label) := if_else(
        n_years == length(years) & !is.na(sum_vol_renew_lag) & sum_vol_renew_lag > 0,
        sum_vol_non / sum_vol_renew_lag, NA_real_
      ),
      !!paste0("fair_share_", end_label) := if_else(
        n_years == length(years) & !is.na(sum_all) & sum_all > 0,
        sum_fair / sum_all, NA_real_
      )
    )
}


# *****************************************************
# 7. Compute all 3-year windows ----
# *****************************************************

ts("Computing 3-year pooled windows...")

# Old-doc windows: ins_long_old covers 2015-2021.
# 2015 is present as lag anchor for the 2016 nonrenew calculation in _2018.
w2018 <- make_window(ins_long_old, 2016:2018, "2018")
w2019 <- make_window(ins_long_old, 2017:2019, "2019")
w2020 <- make_window(ins_long_old, 2018:2020, "2020")
w2021 <- make_window(ins_long_old, 2019:2021, "2021")

# New-doc windows: need 2019 as lag anchor for the 2020 nonrenew calculation.
# Build a panel that prepends old-doc 2019 to new-doc 2020-2023.
# Only used for the lag -- 2019 itself is not a window year.
ins_long_new_with_anchor <- bind_rows(
  ins_long_old %>% filter(year == 2019),
  ins_long_new
) %>% arrange(county, year)

w2022 <- make_window(ins_long_new_with_anchor, 2020:2022, "2022")
w2023 <- make_window(ins_long_new_with_anchor, 2021:2023, "2023")

ins_county_wide <- w2018 %>%
  left_join(w2019, by = "county") %>%
  left_join(w2020, by = "county") %>%
  left_join(w2021, by = "county") %>%
  left_join(w2022, by = "county") %>%
  left_join(w2023, by = "county") %>%
  arrange(county)

cat(sprintf("  Wide window file: %d rows x %d cols\n",
            nrow(ins_county_wide), ncol(ins_county_wide)))

cat("\n--- Statewide mean by window ---\n")
ins_county_wide %>%
  summarise(across(c(starts_with("nonrenew_rate_"), starts_with("fair_share_")),
                   ~round(mean(., na.rm = TRUE), 4))) %>%
  pivot_longer(everything(), names_to = "variable", values_to = "mean") %>%
  print(n = 20)

rm(w2018, w2019, w2020, w2021, w2022, w2023, ins_long_new_with_anchor)


# *****************************************************
# 8. Pull county FIPS from tidycensus ----
# *****************************************************

ts("Pulling California county FIPS from tidycensus (ACS 2020)...")

# census_api_key("YOUR_KEY_HERE", install = TRUE)  # set once per machine

county_fips_raw <- tidycensus::get_acs(
  geography = "county",
  variables = "B01003_001",
  state     = "CA",
  year      = 2020,
  survey    = "acs5",
  geometry  = FALSE
)

county_fips <- county_fips_raw %>%
  transmute(
    county_fips = GEOID,
    county_name_census = str_remove(NAME, " County, California$") %>%
      str_to_title() %>% str_trim()
  )

cat(sprintf("  Counties returned: %d\n", nrow(county_fips)))
rm(county_fips_raw)


# *****************************************************
# 9. Match CDI county names to census FIPS ----
# *****************************************************

ts("Matching CDI county names to census FIPS...")

county_crosswalk <- ins_county_wide %>%
  mutate(county_name_ins = str_to_title(str_trim(county))) %>%
  select(county, county_name_ins) %>%
  left_join(county_fips, by = c("county_name_ins" = "county_name_census"))

n_matched   <- sum(!is.na(county_crosswalk$county_fips))
n_unmatched <- sum( is.na(county_crosswalk$county_fips))
cat(sprintf("  Matched: %d  |  Unmatched: %d\n", n_matched, n_unmatched))

if (n_unmatched > 0) {
  cat("\n  --- Unmatched CDI names ---\n")
  county_crosswalk %>% filter(is.na(county_fips)) %>% print()
  cat("\n  --- Census names (for reference) ---\n")
  print(sort(county_fips$county_name_census))
  # Manual fixes -- uncomment and edit as needed:
  # county_crosswalk <- county_crosswalk %>%
  #   mutate(county_fips = case_when(
  #     county_name_ins == "Some Cdi Name" ~ "06XXX",
  #     TRUE ~ county_fips
  #   ))
}

# stopifnot(
#   "Unmatched counties remain -- add manual fixes above" =
#     sum(is.na(county_crosswalk$county_fips)) == 0
# )
# Review unmatched output above and add manual fixes before re-running if needed.
if (n_unmatched == 0) {
  cat("  All counties matched.\n")
} else {
  cat(sprintf("  WARNING: %d unmatched counties -- insurance vars will be NA for these.\n",
              n_unmatched))
}

crosswalk <- county_crosswalk %>% select(county, county_fips)


# *****************************************************
# 10. Add FIPS to county files ----
# *****************************************************

ts("Adding FIPS to county files...")

ins_county_panel <- ins_rates %>%
  left_join(crosswalk, by = "county") %>%
  select(county_fips, county, year, everything()) %>%
  arrange(county_fips, year)

ins_county_wide <- ins_county_wide %>%
  left_join(crosswalk, by = "county") %>%
  select(county_fips, county, everything()) %>%
  arrange(county_fips)

cat(sprintf("  Annual panel: %d rows x %d cols\n",
            nrow(ins_county_panel), ncol(ins_county_panel)))
cat(sprintf("  Wide windows: %d rows x %d cols\n",
            nrow(ins_county_wide), ncol(ins_county_wide)))


# *****************************************************
# 11. Join to 1-05_analytic.rds ----
# *****************************************************

ts("Loading 1-05_analytic.rds and joining insurance variables...")

analytic <- readRDS(paste0(data_output_s, "1-05_analytic.rds"))
cat(sprintf("  analytic: %s rows x %d cols\n",
            formatC(nrow(analytic), format = "d", big.mark = ","), ncol(analytic)))

stopifnot("geoid"     %in% names(analytic))
stopifnot("sale_date" %in% names(analytic))
stopifnot("buy1_corp" %in% names(analytic))

analytic_ins <- analytic %>%
  mutate(county_fips = substr(geoid, 1, 5)) %>%
  left_join(ins_county_wide %>% select(-county), by = "county_fips") %>%
  mutate(
    sale_yr = year(sale_date),
    nonrenew_saleyr = case_when(
      sale_yr <= 2018 ~ nonrenew_rate_2018,
      sale_yr == 2019 ~ nonrenew_rate_2019,
      sale_yr == 2020 ~ nonrenew_rate_2020,
      sale_yr == 2021 ~ nonrenew_rate_2021,
      sale_yr == 2022 ~ nonrenew_rate_2022,
      sale_yr >= 2023 ~ nonrenew_rate_2023
    ),
    fair_saleyr = case_when(
      sale_yr <= 2018 ~ fair_share_2018,
      sale_yr == 2019 ~ fair_share_2019,
      sale_yr == 2020 ~ fair_share_2020,
      sale_yr == 2021 ~ fair_share_2021,
      sale_yr == 2022 ~ fair_share_2022,
      sale_yr >= 2023 ~ fair_share_2023
    )
  )

cat(sprintf("  After join: %s rows x %d cols\n",
            formatC(nrow(analytic_ins), format = "d", big.mark = ","),
            ncol(analytic_ins)))

cat("\n--- Sale year distribution ---\n")
analytic_ins %>% count(sale_yr) %>% arrange(sale_yr) %>% print()

cat("\n--- Join coverage ---\n")
cat(sprintf("  nonrenew_saleyr matched: %s  |  NA: %s\n",
            formatC(sum(!is.na(analytic_ins$nonrenew_saleyr)), format = "d", big.mark = ","),
            formatC(sum( is.na(analytic_ins$nonrenew_saleyr)), format = "d", big.mark = ",")))
cat(sprintf("  fair_saleyr     matched: %s  |  NA: %s\n",
            formatC(sum(!is.na(analytic_ins$fair_saleyr)),     format = "d", big.mark = ","),
            formatC(sum( is.na(analytic_ins$fair_saleyr)),     format = "d", big.mark = ",")))


# *****************************************************
# 12. Wilcoxon + Cohen's d by buy1_corp ----
# *****************************************************

ts("Running Wilcoxon + Cohen's d tests by corporate buyer status...")

test_vars <- c("nonrenew_saleyr", "fair_saleyr")

corp_df <- analytic_ins %>%
  filter(!is.na(buy1_corp)) %>%
  select(buy1_corp, all_of(test_vars))

n_corp    <- sum(corp_df$buy1_corp == 1L)
n_noncorp <- sum(corp_df$buy1_corp == 0L)

cat(sprintf("\n  Corporate:     %s txns\n", formatC(n_corp,    format = "d", big.mark = ",")))
cat(sprintf("  Non-corporate: %s txns\n",   formatC(n_noncorp, format = "d", big.mark = ",")))

cat(sprintf("\n  %-20s  %10s  %10s  %10s  %9s  %8s  %10s\n",
            "Variable", "Mean Corp", "Mean Non-C", "Diff", "Diff %",
            "Cohen d", "Wilcox p"))
cat(sprintf("  %s\n", strrep("-", 85)))

for (v in test_vars) {
  corp_vals    <- corp_df[[v]][corp_df$buy1_corp == 1L]
  noncorp_vals <- corp_df[[v]][corp_df$buy1_corp == 0L]
  
  m_corp    <- mean(corp_vals,    na.rm = TRUE)
  m_noncorp <- mean(noncorp_vals, na.rm = TRUE)
  diff_abs  <- m_corp - m_noncorp
  diff_pct  <- 100 * diff_abs / abs(m_noncorp)
  d         <- cohens_d_ranks(corp_df[[v]], corp_df$buy1_corp)
  wt        <- wilcox.test(corp_vals, noncorp_vals, exact = FALSE)
  
  stars <- case_when(
    wt$p.value < 0.001 ~ "***",
    wt$p.value < 0.01  ~ "**",
    wt$p.value < 0.05  ~ "*",
    TRUE               ~ ""
  )
  
  cat(sprintf("  %-20s  %10.4f  %10.4f  %+10.4f  %+8.1f%%  %8.3f  %.2e %s\n",
              v, m_corp, m_noncorp, diff_abs, diff_pct, d, wt$p.value, stars))
}

cat(sprintf("  %s\n", strrep("-", 85)))
cat("  Significance: *** p<0.001  ** p<0.01  * p<0.05\n")
cat("  Cohen's d on ranks: |d| ~0.2 small, ~0.5 medium, ~0.8 large\n")
cat("  Note: N is large -- use Cohen's d for practical magnitude.\n")

rm(corp_df, n_corp, n_noncorp)


# *****************************************************
# 13. Join to 1-05_prop_wide.rds ----
# *****************************************************

ts("Loading 1-05_prop_wide.rds and joining insurance variables...")

prop_wide <- readRDS(paste0(data_output_s, "1-05_prop_wide.rds"))
cat(sprintf("  prop_wide: %s rows x %d cols\n",
            formatC(nrow(prop_wide), format = "d", big.mark = ","), ncol(prop_wide)))

stopifnot("geoid" %in% names(prop_wide))

prop_wide_ins <- prop_wide %>%
  mutate(county_fips = substr(geoid, 1, 5)) %>%
  left_join(ins_county_wide %>% select(-county), by = "county_fips")

cat(sprintf("  After join: %s rows x %d cols\n",
            formatC(nrow(prop_wide_ins), format = "d", big.mark = ","),
            ncol(prop_wide_ins)))
cat(sprintf("  nonrenew_rate_2023 matched: %s  |  NA: %s\n",
            formatC(sum(!is.na(prop_wide_ins$nonrenew_rate_2023)), format = "d", big.mark = ","),
            formatC(sum( is.na(prop_wide_ins$nonrenew_rate_2023)), format = "d", big.mark = ",")))


# *****************************************************
# 14. Spot checks ----
# *****************************************************

ts("Spot checks...")

cat("\n--- Top 10 counties by nonrenew_rate_2023 ---\n")
ins_county_wide %>%
  arrange(desc(nonrenew_rate_2023)) %>%
  select(county, nonrenew_rate_2023, fair_share_2023) %>%
  head(10) %>% mutate(across(where(is.numeric), ~round(., 4))) %>% print()

cat("\n--- Top 10 counties by fair_share_2023 ---\n")
ins_county_wide %>%
  arrange(desc(fair_share_2023)) %>%
  select(county, fair_share_2023, nonrenew_rate_2023) %>%
  head(10) %>% mutate(across(where(is.numeric), ~round(., 4))) %>% print()

cat("\n--- Change: nonrenew_rate_2019 -> nonrenew_rate_2023 (top movers) ---\n")
ins_county_wide %>%
  mutate(change = nonrenew_rate_2023 - nonrenew_rate_2019) %>%
  arrange(desc(change)) %>%
  select(county, nonrenew_rate_2019, nonrenew_rate_2023, change) %>%
  head(10) %>% mutate(across(where(is.numeric), ~round(., 4))) %>% print()


# *****************************************************
# 15. Save ----
# *****************************************************

ts("Saving outputs...")

out_panel     <- paste0(data_output_s, "2-04_ins_county_panel.rds")
out_wide      <- paste0(data_output_s, "2-04_ins_county_wide.rds")
out_analytic  <- paste0(data_output_s, "2-04_analytic.rds")
out_prop_wide <- paste0(data_output_s, "2-04_prop_wide.rds")

saveRDS(ins_county_panel, out_panel)
cat(sprintf("  (A) ins_county_panel  -> %d rows x %d cols\n",
            nrow(ins_county_panel), ncol(ins_county_panel)))

saveRDS(ins_county_wide, out_wide)
cat(sprintf("  (B) ins_county_wide   -> %d rows x %d cols\n",
            nrow(ins_county_wide), ncol(ins_county_wide)))

saveRDS(analytic_ins, out_analytic)
cat(sprintf("  (C) analytic          -> %s rows x %d cols\n",
            formatC(nrow(analytic_ins), format = "d", big.mark = ","), ncol(analytic_ins)))

saveRDS(prop_wide_ins, out_prop_wide)
cat(sprintf("  (D) prop_wide         -> %s rows x %d cols\n",
            formatC(nrow(prop_wide_ins), format = "d", big.mark = ","), ncol(prop_wide_ins)))


# *****************************************************
# 16. Close out ----
# *****************************************************

cat("\n\n")
cat(strrep("=", 70), "\n")
cat("  Insurance variables added:\n")
cat("    nonrenew_rate_2018 ... nonrenew_rate_2023  (6 windows)\n")
cat("    fair_share_2018    ... fair_share_2023     (6 windows)\n")
cat("    sale_yr            -- year(sale_date)\n")
cat("    nonrenew_saleyr    -- nonrenew_rate matched to sale year\n")
cat("    fair_saleyr        -- fair_share matched to sale year\n")
cat("\n  Window definitions (pooled numerator/denominator):\n")
cat("    _2018 = pool(2016-2018)  old doc\n")
cat("    _2019 = pool(2017-2019)  old doc\n")
cat("    _2020 = pool(2018-2020)  old doc (2020 from ins_county_overlap v2015_)\n")
cat("    _2021 = pool(2019-2021)  old doc (2020-21 from ins_county_overlap v2015_)\n")
cat("    _2022 = pool(2020-2022)  new doc (2019 old-doc used as lag anchor only)\n")
cat("    _2023 = pool(2021-2023)  new doc\n")
cat("  _2017 excluded: lag for 2015 requires 2014 data (not available).\n")
cat("  sale_yr <= 2018 maps to _2018 (earliest available window).\n")
cat(strrep("=", 70), "\n")

message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")

savehistory(paste0(secure, "Process/Fire Investment/Logs/2-04_history_", timestamp, ".txt"))
sink()



