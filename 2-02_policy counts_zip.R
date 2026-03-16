

# **************************************************
#                     DETAILS
#
# Purpose:   Load, clean, and merge CDI residential voluntary market
#            policy count files by ZIP (2015-2021 and 2020-2023),
#            producing a single wide panel at the ZIP x year level
#            (2015-2023)
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2026-03-16
# Started:   MM/DD/YYYY
# Updated:   MM/DD/YYYY
# **************************************************


# *************
# 1. Setup ----

user   <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root   <- paste0(user, "Fire Investment/")

data_source  <- paste0(root, "Data/Source/Insurance/")
data_derived <- paste0(root, "Data/Derived/Insurance/")

timestamp  <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file   <- paste0(root, "Process/Logs/2-02_log_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)

start_time <- Sys.time()
cat("\n====================================================\n")
cat("  2-02_cdi_zip.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

set.seed(123)

pkgs <- c("dplyr", "readxl", "tidyr", "tools")
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

# helper: coerce to numeric (handles "-" suppression codes and NAs)
clean_num <- function(x) {
  x <- gsub(",", "", as.character(x))
  x[x %in% c("-", "NA", "")] <- NA
  as.numeric(x)
}


# *****************************************************
# 2. Load 2020-2023 ----
# *****************************************************

ts("Loading 2020-2023 file...")

raw_2023 <- read_excel(
  paste0(data_source, "Residential-Insurance-Voluntary-Market-New-Renew-NonRenew-by-ZIP-2020-2023.xlsx"),
  sheet     = "New Renew Nonrenew",
  col_types = "text"
)

cat(sprintf("  Raw rows: %d\n", nrow(raw_2023)))

d2023 <- raw_2023 %>%
  transmute(
    zip       = sprintf("%05s", trimws(`ZIP Code`)),
    year      = as.integer(`Year`),
    vol_new   = clean_num(`New`),
    vol_renew = clean_num(`Renewed`),
    vol_non   = clean_num(`Non-Renewed`)
    # county dropped -- does not map cleanly to ZIP
  )

cat(sprintf("  After clean: %d rows | years: %s | unique ZIPs: %d\n",
            nrow(d2023),
            paste(sort(unique(d2023$year)), collapse = ", "),
            n_distinct(d2023$zip)))

rm(raw_2023)


# *****************************************************
# 3. Load 2015-2021 ----
# *****************************************************

ts("Loading 2015-2021 file...")

raw_2021 <- read_excel(
  paste0(data_source, "Residential-Property-Voluntary-Market-New-Renew-NonRenew-by-ZIP-2015-2021.xlsx"),
  sheet     = "New Renew NonRenew",
  col_types = "text"
)

cat(sprintf("  Raw rows: %d\n", nrow(raw_2021)))

# vol_non_ci (insurer-initiated) + vol_non_ii (insured-initiated) combined
# to match the single Non-Renewed column in the 2020-2023 file
d2021 <- raw_2021 %>%
  transmute(
    zip        = sprintf("%05s", trimws(`ZIP Code`)),
    year       = as.integer(`Year`),
    vol_new    = clean_num(`New`),
    vol_renew  = clean_num(`Renewed`),
    vol_non_ci = clean_num(`Insurer-Initiated Nonrenewed`),
    vol_non_ii = clean_num(`Insured-Initiated Nonrenewed`),
    vol_non    = vol_non_ci + vol_non_ii
    # county dropped -- does not map cleanly to ZIP
  ) %>%
  select(zip, year, vol_new, vol_renew, vol_non, vol_non_ci, vol_non_ii)

# OPTION B (commented out): keep vol_non_ci and vol_non_ii as separate columns
# in the final panel (they will be NA for 2020-2023 years)
# -- to activate: switch select() above to include vol_non_ci, vol_non_ii,
#    and update the bind_rows() in Section 7 to use Option B

cat(sprintf("  After clean: %d rows | years: %s | unique ZIPs: %d\n",
            nrow(d2021),
            paste(sort(unique(d2021$year)), collapse = ", "),
            n_distinct(d2021$zip)))

rm(raw_2021)


# *****************************************************
# 4. ZIP coverage check ----
# *****************************************************
# The two files have different ZIP counts (2020-2023: ~2,171 ZIPs;
# 2015-2021: ~2,654 ZIPs). The gap reflects ZIP codes that dropped out
# of the voluntary market entirely in later years, not a data error.

ts("Checking ZIP coverage across files...")

zips_2023 <- unique(d2023$zip)
zips_2021 <- unique(d2021$zip)

n_both       <- sum(zips_2023 %in% zips_2021)
n_2023_only  <- sum(!zips_2023 %in% zips_2021)
n_2021_only  <- sum(!zips_2021 %in% zips_2023)

cat(sprintf("  ZIPs in both files:      %d\n", n_both))
cat(sprintf("  ZIPs in 2020-2023 only:  %d\n", n_2023_only))
cat(sprintf("  ZIPs in 2015-2021 only:  %d\n", n_2021_only))

rm(zips_2023, zips_2021)


# *****************************************************
# 5. Compare overlapping years (2020 & 2021) ----
# *****************************************************
# The two CDI report vintages differ for 2020 and 2021. This is expected:
# CDI revised its reporting methodology starting with the 2020 report year,
# removing certain non-renewal and cancellation types, which reduces counts
# relative to the older file. Differences are real but reflect a reporting
# change, not data error. We use the 2020-2023 doc for 2020 onward (see
# Section 6), so the 2019->2020 annual change uses old-doc values for both
# years and 2020->2021 uses new-doc values for both years -- keeping each
# change internally consistent. Flag the 2019->2020 transition as a
# methodology break if publishing year-over-year trends.

ts("Comparing overlapping years (2020, 2021)...")

overlap_cols  <- c("vol_new", "vol_renew", "vol_non")
overlap_years <- c(2020L, 2021L)

overlap_a <- d2023 %>%
  filter(year %in% overlap_years) %>%
  select(zip, year, all_of(overlap_cols))

overlap_b <- d2021 %>%
  filter(year %in% overlap_years) %>%
  select(zip, year, all_of(overlap_cols))

overlap_comp <- overlap_a %>%
  inner_join(overlap_b, by = c("zip", "year"), suffix = c("_2023doc", "_2021doc"))

cat(sprintf("\n  ZIPs matched in both docs for overlap years: %d\n",
            nrow(overlap_comp)))

any_diff <- FALSE
for (col in overlap_cols) {
  a_col  <- paste0(col, "_2023doc")
  b_col  <- paste0(col, "_2021doc")
  diff   <- overlap_comp[[a_col]] - overlap_comp[[b_col]]
  n_diff <- sum(diff != 0, na.rm = TRUE)
  if (n_diff > 0) {
    any_diff <- TRUE
    cat(sprintf("\n  WARNING: %s — %d row(s) differ\n", col, n_diff))
    cat(sprintf("    max_abs_diff = %s  |  mean_abs_diff = %.1f\n",
                formatC(max(abs(diff), na.rm = TRUE), format = "d", big.mark = ","),
                mean(abs(diff), na.rm = TRUE)))
    mismatch <- overlap_comp %>%
      mutate(diff_val = abs(!!sym(a_col) - !!sym(b_col))) %>%
      filter(diff_val > 0) %>%
      arrange(desc(diff_val)) %>%
      select(zip, year, !!sym(a_col), !!sym(b_col), diff_val) %>%
      head(10)
    print(mismatch)
  } else {
    cat(sprintf("  OK: %s — values match exactly across both docs\n", col))
  }
}

if (!any_diff) cat("\n  All overlapping values match exactly.\n")

cat("\n  -> Using 2020-2023 doc values for years 2020 and 2021.\n")

rm(overlap_a, overlap_b, overlap_comp, any_diff)


# *****************************************************
# 6. Append into single panel ----
# *****************************************************
# 2015-2019: old doc only (d2021_trim)
# 2020-2023: new doc only (d2023)
# Annual changes across the 2019->2020 boundary mix doc vintages -- flag
# as a methodology break for published trend analysis (see Section 5 note).

ts("Appending into single panel (2015-2023)...")

d2021_trim <- d2021 %>% filter(!year %in% overlap_years)

# OPTION A (default): vol_non combined only
panel <- bind_rows(
  d2023 %>% select(zip, year, vol_new, vol_renew, vol_non),
  d2021_trim %>% select(zip, year, vol_new, vol_renew, vol_non)
) %>% arrange(zip, year)

# OPTION B (commented out): include vol_non_ci and vol_non_ii
# (will be NA for 2020-2023 years since not reported separately)
# panel <- bind_rows(
#   d2023 %>% mutate(vol_non_ci = NA_real_, vol_non_ii = NA_real_) %>%
#     select(zip, year, vol_new, vol_renew, vol_non, vol_non_ci, vol_non_ii),
#   d2021_trim %>%
#     select(zip, year, vol_new, vol_renew, vol_non, vol_non_ci, vol_non_ii)
# ) %>% arrange(zip, year)

cat(sprintf("  Panel rows: %d | unique ZIPs: %d | years: %s\n",
            nrow(panel),
            n_distinct(panel$zip),
            paste(sort(unique(panel$year)), collapse = ", ")))


# *****************************************************
# 7. Spot checks ----
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
# 8. Save ----
# *****************************************************
# Two outputs, both one row per ZIP with years as column suffixes:
#
# (A) ins_zip: analysis-ready wide panel, 2015-2023, new doc for 2020
#     onward. Columns: zip, then vol_new_2015 ... vol_non_2023.
#     This is the file to use for analysis.
#
# (B) ins_zip_overlap: archive of both doc vintages side by side across
#     all years. v2015_ prefix = 2015-2021 doc, v2020_ prefix = 2020-2023
#     doc. NAs where a doc does not cover a year. Use to inspect or
#     reconcile differences from CDI's methodology change; do not mix
#     vintages in trend analysis. Max differences in overlap years are
#     at the ZIP level -- see Section 5 output for details.

ts("Saving outputs...")

# (A) ins_zip: pivot to wide
ins_zip <- panel %>%
  pivot_wider(
    id_cols     = zip,
    names_from  = year,
    values_from = c(vol_new, vol_renew, vol_non),
    names_glue  = "{.value}_{year}",
    names_sort  = TRUE
  ) %>%
  arrange(zip)

saveRDS(ins_zip, paste0(data_derived, "ins_zip.rds"))
cat(sprintf("  (A) Saved ins_zip: %d rows x %d cols\n", nrow(ins_zip), ncol(ins_zip)))

# (B) ins_zip_overlap: full side-by-side archive, all years, both vintages
overlap_v2015 <- d2021 %>%
  select(zip, year, vol_new, vol_renew, vol_non) %>%
  pivot_wider(
    id_cols     = zip,
    names_from  = year,
    values_from = c(vol_new, vol_renew, vol_non),
    names_glue  = "v2015_{.value}_{year}"
  )

overlap_v2020 <- d2023 %>%
  select(zip, year, vol_new, vol_renew, vol_non) %>%
  pivot_wider(
    id_cols     = zip,
    names_from  = year,
    values_from = c(vol_new, vol_renew, vol_non),
    names_glue  = "v2020_{.value}_{year}"
  )

ins_zip_overlap <- overlap_v2015 %>%
  full_join(overlap_v2020, by = "zip") %>%
  arrange(zip)

saveRDS(ins_zip_overlap, paste0(data_derived, "ins_zip_overlap.rds"))
cat(sprintf("  (B) Saved ins_zip_overlap: %d rows x %d cols\n",
            nrow(ins_zip_overlap), ncol(ins_zip_overlap)))

rm(d2023, d2021, d2021_trim, panel, overlap_v2015, overlap_v2020)


# ******************************
# 9. Close out ----

cat("\n\n")
cat(strrep("=", 70), "\n")
ts("DONE")
message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2),
        " minutes")
cat(strrep("=", 70), "\n")
savehistory(paste0(root, "Process/Logs/2-02_history_", timestamp, ".txt"))
sink()
