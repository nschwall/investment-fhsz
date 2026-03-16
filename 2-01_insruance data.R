
# **************************************************
#                     DETAILS
#
# Purpose:   Load, clean, and merge CDI residential policy count files
#            (2015-2021 and 2020-2023), producing a single wide panel
#            at the county x year level (2015-2023)
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2026-03-16
# Started:   MM/DD/YYYY
# Updated:   MM/DD/YYYY
# **************************************************


# *************
# 1. Setup ----

user   <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root   <- paste0(user, "Fire Investment/")

data_derived <- paste0(root, "Data/Derived/Insurance/")

timestamp  <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file   <- paste0(root, "Process/Logs/2-01_log_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)

start_time <- Sys.time()
cat("\n====================================================\n")
cat("  2-01_fhsz_insurance_cotality.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

set.seed(123)

pkgs <- c("dplyr", "readr", "stringr", "tools")
invisible(lapply(pkgs, function(p) {
  if (!require(p, character.only = TRUE)) {
    install.packages(p)
    library(p, character.only = TRUE)
  }
}))
rm(pkgs)

ts <- function(label = "") {
  cat(sprintf("\n[%s]  %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), label))
}

# helper: strip commas and coerce to numeric (handles "-" suppression codes)
clean_num <- function(x) {
  x <- gsub(",", "", as.character(x))
  x[x == "-"] <- NA
  as.numeric(x)
}

# helper: title-case county names
title_county <- function(x) tools::toTitleCase(tolower(x))


# *****************************************************
# 2. Load 2020-2023 ----
# *****************************************************

ts("Loading 2020-2023 file...")

raw_2023 <- read_csv(
  paste0(data_derived, "residential plans and renewals_2020-2023.csv"),
  col_types = cols(.default = "c"),
  show_col_types = FALSE
)

cat(sprintf("  Raw rows: %d\n", nrow(raw_2023)))

# Fill county column downward (county only appears on first year row)
raw_2023 <- raw_2023 %>%
  mutate(county = na_if(county, "")) %>%
  tidyr::fill(county, .direction = "down")

# Clean numerics, title-case county
d2023 <- raw_2023 %>%
  mutate(
    county    = title_county(county),
    year      = as.integer(year),
    vol_new   = clean_num(vol_new),
    vol_renew = clean_num(vol_renew),
    vol_non   = clean_num(vol_non),
    fair_new  = clean_num(fair_new),
    fair_renew = clean_num(fair_renew),
    did_new   = clean_num(did_new),
    did_renew = clean_num(did_renew)
  )

cat(sprintf("  After fill: %d rows | years: %s\n",
            nrow(d2023), paste(sort(unique(d2023$year)), collapse = ", ")))


# *****************************************************
# 3. Load 2015-2021 ----
# *****************************************************

ts("Loading 2015-2021 file...")

raw_2021 <- read_csv(
  paste0(data_derived, "residential plans and renewals_2015-2021.csv"),
  col_types = cols(.default = "c"),
  show_col_types = FALSE
)

cat(sprintf("  Raw rows: %d\n", nrow(raw_2021)))

# Combine vol_non_ci + vol_non_ii into vol_non; keep originals too
d2021 <- raw_2021 %>%
  mutate(
    county      = title_county(County),
    year        = as.integer(Year),
    vol_new     = clean_num(vol_new),
    vol_renew   = clean_num(vol_renew),
    vol_non_ci  = clean_num(vol_non_ci),
    vol_non_ii  = clean_num(vol_non_ii),
    vol_non     = vol_non_ci + vol_non_ii,   # combined to match 2020-2023
    fair_new    = clean_num(fair_new),
    fair_renew  = clean_num(fair_renew),
    did_new     = NA_real_,                  # not in this file
    did_renew   = NA_real_
  ) %>%
  select(county, year, vol_new, vol_renew, vol_non, vol_non_ci, vol_non_ii,
         fair_new, fair_renew, did_new, did_renew)

cat(sprintf("  After clean: %d rows | years: %s\n",
            nrow(d2021), paste(sort(unique(d2021$year)), collapse = ", ")))


# *****************************************************
# 4. Validate: county sums vs. state rows ----
# *****************************************************

ts("Validating county sums against state totals...")

check_sums <- function(df, label, cols) {
  state_rows  <- df %>% filter(county == "State")
  county_rows <- df %>% filter(county != "State")
  
  county_sums <- county_rows %>%
    group_by(year) %>%
    summarise(across(all_of(cols), ~sum(.x, na.rm = TRUE)), .groups = "drop")
  
  comp <- state_rows %>%
    select(year, all_of(cols)) %>%
    inner_join(county_sums, by = "year", suffix = c("_state", "_sum"))
  
  cat(sprintf("\n  [%s] county sum vs. state row:\n", label))
  for (col in cols) {
    s_col <- paste0(col, "_state")
    c_col <- paste0(col, "_sum")
    if (!s_col %in% names(comp)) next
    diffs <- abs(comp[[s_col]] - comp[[c_col]])
    cat(sprintf("    %-15s  max_diff = %s  (years checked: %s)\n",
                col,
                formatC(max(diffs, na.rm = TRUE), format = "d", big.mark = ","),
                paste(comp$year, collapse = ", ")))
  }
}

check_sums(d2023, "2020-2023", c("vol_new","vol_renew","vol_non","fair_new","fair_renew","did_new","did_renew"))
check_sums(d2021, "2015-2021", c("vol_new","vol_renew","vol_non","fair_new","fair_renew"))


# *****************************************************
# 5. Drop state rows ----
# *****************************************************

ts("Dropping state rows...")

d2023 <- d2023 %>% filter(county != "State")
d2021 <- d2021 %>% filter(county != "State")

cat(sprintf("  2020-2023 county rows: %d\n", nrow(d2023)))
cat(sprintf("  2015-2021 county rows: %d\n", nrow(d2021)))


# *****************************************************
# 6. Compare overlapping years (2020 & 2021) ----
# *****************************************************

ts("Comparing overlapping years (2020, 2021)...")

overlap_cols <- c("vol_new","vol_renew","vol_non","fair_new","fair_renew")
overlap_years <- c(2020L, 2021L)

overlap_a <- d2023 %>%
  filter(year %in% overlap_years) %>%
  select(county, year, all_of(overlap_cols))

overlap_b <- d2021 %>%
  filter(year %in% overlap_years) %>%
  select(county, year, all_of(overlap_cols))

overlap_comp <- overlap_a %>%
  inner_join(overlap_b, by = c("county", "year"), suffix = c("_2023doc", "_2021doc"))

cat(sprintf("\n  Counties matched in both docs for overlap years: %d\n",
            nrow(overlap_comp)))

any_diff <- FALSE
for (col in overlap_cols) {
  a_col <- paste0(col, "_2023doc")
  b_col <- paste0(col, "_2021doc")
  diff  <- overlap_comp[[a_col]] - overlap_comp[[b_col]]
  n_diff <- sum(diff != 0, na.rm = TRUE)
  if (n_diff > 0) {
    any_diff <- TRUE
    cat(sprintf("\n  WARNING: %s — %d row(s) differ\n", col, n_diff))
    cat(sprintf("    max_abs_diff = %s  |  mean_abs_diff = %.1f\n",
                formatC(max(abs(diff), na.rm = TRUE), format = "d", big.mark = ","),
                mean(abs(diff), na.rm = TRUE)))
    # Show worst offenders
    mismatch <- overlap_comp %>%
      mutate(diff_val = abs(!!sym(a_col) - !!sym(b_col))) %>%
      filter(diff_val > 0) %>%
      arrange(desc(diff_val)) %>%
      select(county, year, !!sym(a_col), !!sym(b_col), diff_val) %>%
      head(10)
    print(mismatch)
  } else {
    cat(sprintf("  OK: %s — values match exactly across both docs\n", col))
  }
}

if (!any_diff) cat("\n  All overlapping values match exactly.\n")

# Decision: for overlapping years, prefer 2020-2023 doc values
# (more recent report; drop overlap years from 2015-2021 before appending)
cat("\n  -> Using 2020-2023 doc values for years 2020 and 2021.\n")


# *****************************************************
# 7. Append into single panel ----
# *****************************************************

ts("Appending into single panel (2015-2023)...")

# Drop overlap years from older file before binding
d2021_trim <- d2021 %>% filter(!year %in% overlap_years)

# -- Optional: keep vol_non_ci and vol_non_ii in the panel
# To include them, uncomment the block below and comment out the version above it.
# Note: vol_non_ci / vol_non_ii are NA for 2020-2023 (not reported separately).

# OPTION A (default): vol_non only
panel <- bind_rows(
  d2023 %>% select(county, year, vol_new, vol_renew, vol_non,
                   fair_new, fair_renew, did_new, did_renew),
  d2021_trim %>% select(county, year, vol_new, vol_renew, vol_non,
                        fair_new, fair_renew, did_new, did_renew)
) %>% arrange(county, year)

# OPTION B (commented out): include vol_non_ci and vol_non_ii
# panel <- bind_rows(
#   d2023 %>% mutate(vol_non_ci = NA_real_, vol_non_ii = NA_real_) %>%
#     select(county, year, vol_new, vol_renew, vol_non, vol_non_ci, vol_non_ii,
#            fair_new, fair_renew, did_new, did_renew),
#   d2021_trim %>%
#     select(county, year, vol_new, vol_renew, vol_non, vol_non_ci, vol_non_ii,
#            fair_new, fair_renew, did_new, did_renew)
# ) %>% arrange(county, year)

cat(sprintf("  Panel rows: %d | counties: %d | years: %s\n",
            nrow(panel),
            n_distinct(panel$county),
            paste(sort(unique(panel$year)), collapse = ", ")))


# *****************************************************
# 8. Spot checks ----
# *****************************************************

ts("Spot checks...")

cat("\n--- Row counts by year ---\n")
panel %>% count(year) %>% print()

cat("\n--- NAs by column ---\n")
panel %>%
  summarise(across(everything(), ~sum(is.na(.)))) %>%
  tidyr::pivot_longer(everything(), names_to = "col", values_to = "n_na") %>%
  filter(n_na > 0) %>%
  print()


# *****************************************************
# 9. Save ----
# *****************************************************

ts("Saving panel...")

out_path <- paste0(data_derived, "2-01_cdi_county_panel_2015_2023.csv")
write_csv(panel, out_path)
cat(sprintf("  Saved %d rows -> %s\n", nrow(panel), out_path))


# ******************************
# 10. Close out ----

cat("\n\n")
cat(strrep("=", 70), "\n")
ts("DONE")
message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2),
        " minutes")
cat(strrep("=", 70), "\n")
savehistory(paste0(root, "Process/Logs/2-01_history_", timestamp, ".txt"))
sink()

