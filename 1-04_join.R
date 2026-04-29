
FIX DROP FOR NA LAT LONG --- CURRENTLY IN 1-05... SHOULD BE HERE!

# **************************************************
#                     DETAILS
#
# Purpose:   Join cleaned OwnerTransfer analytic file (1-02_owner_transfer.rds)
#            with cleaned SFR property file (1-03_property_sfr.rds) on clip.
#            Validate match rate, inspect unmatched rows, and save joined file.
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2026-04-27
# Started:   MM/DD/YYYY
# Updated:   MM/DD/YYYY
#
# ---- INPUTS ----
#
#   1-02_owner_transfer.rds  — cleaned OwnerTransfer analytic sample
#                              (CA, SFR, 2019+, non-missing sale amt, corp flags applied)
#   1-03_property_sfr.rds    — cleaned SFR property file (prop_slim: 25 columns)
#
# ---- JOIN DESIGN ----
#
#   Key:   clip  (Cotality property identifier)
#          owner_transfer.clip_id  <->  prop_slim.clip
#   Type:  left join — keep all owner_transfer rows; attach property attributes.
#          Every OT transaction is expected to have a property record (it was a
#          real transaction on a real parcel). prop_matched == 0 is unexpected
#          and should be investigated, not simply flagged and retained.
#          The property file will have many CLIPs with no OT row (parcels that
#          did not transact in 2019+) — this is expected and not checked here.
#
# ---- OT COLUMNS DROPPED BEFORE JOIN ----
#
#   fips, apn_unformatted, prop_indicator, residential
#   — all duplicated by the property file, which is the canonical source.
#     Property versions are kept; OT versions dropped before joining.
#   mobile_home retained — distinct from manuf_home (property file).
#
# ---- OUTPUTS ----
#
#   analytic: 1-04_analytic.rds  — joined file, all left-join rows
#   log:      secure drive / Process / Logs
#   drop list: Dropbox / Process / Logs
# **************************************************


# *************
# 1. Setup ----

user   <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root   <- paste0(user, "Fire Investment/")
secure <- "Y:/Institutional Investment/"

data_output_s <- paste0(secure, "Data/Derived/Fire Investment/")

# logging
timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file  <- paste0(secure, "Process/Fire Investment/Logs/1-04_log_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)

start_time <- Sys.time()
set.seed(123)

drop_list <- list()

cat("\n====================================================\n")
cat("  1-04_join.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

# packages
pkgs <- c("dplyr", "data.table", "beepr", "ggplot2", "lubridate")
invisible(lapply(pkgs, function(p) {
  if (!require(p, character.only = TRUE)) {
    install.packages(p)
    library(p, character.only = TRUE)
  }
}))
rm(pkgs)

# helper: timestamped log message
ts <- function(label = "") {
  cat(sprintf("\n[%s]  %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), label))
}


# *****************************************************
# 2. Load inputs ----
# *****************************************************

ts("Loading 1-02_owner_transfer.rds...")
ot <- readRDS(paste0(data_output_s, "1-02_owner_transfer.rds"))
cat(sprintf("  owner_transfer: %s rows x %d columns\n",
            formatC(nrow(ot), format = "d", big.mark = ","), ncol(ot)))
cat(sprintf("  Columns: %s\n", paste(names(ot), collapse = ", ")))

drop_list <- c(drop_list, sprintf("owner_transfer loaded (1-02): %s rows x %d columns",
                                  formatC(nrow(ot), format = "d", big.mark = ","), ncol(ot)))

ts("Loading 1-03_property_sfr.rds...")
prop <- readRDS(paste0(data_output_s, "1-03_property_sfr.rds"))
cat(sprintf("  prop_slim: %s rows x %d columns\n",
            formatC(nrow(prop), format = "d", big.mark = ","), ncol(prop)))
cat(sprintf("  Columns: %s\n", paste(names(prop), collapse = ", ")))

drop_list <- c(drop_list, sprintf("prop_slim loaded (1-03): %s rows x %d columns",
                                  formatC(nrow(prop), format = "d", big.mark = ","), ncol(prop)))


# *****************************************************
# 3. Pre-join validation ----
# *****************************************************
ts("Pre-join validation...")

# Confirm join keys exist
stopifnot("clip_id" %in% names(ot))
stopifnot("clip"    %in% names(prop))

ot_clips   <- unique(ot$clip_id[!is.na(ot$clip_id)])
prop_clips <- unique(prop$clip[!is.na(prop$clip)])

n_ot_clips   <- length(ot_clips)
n_prop_clips <- length(prop_clips)
n_matched    <- sum(ot_clips %in% prop_clips)
n_unmatched  <- sum(!ot_clips %in% prop_clips)
pct_matched  <- round(100 * n_matched / n_ot_clips, 2)

cat(sprintf("  OwnerTransfer unique CLIPs:  %s\n", formatC(n_ot_clips,   format = "d", big.mark = ",")))
cat(sprintf("  Property unique CLIPs:       %s\n", formatC(n_prop_clips, format = "d", big.mark = ",")))
cat(sprintf("  OT CLIPs matched in prop:    %s (%.2f%%)\n",
            formatC(n_matched,   format = "d", big.mark = ","), pct_matched))
cat(sprintf("  OT CLIPs unmatched:          %s (%.2f%%)\n",
            formatC(n_unmatched, format = "d", big.mark = ","), 100 - pct_matched))

drop_list <- c(drop_list, sprintf(
  "CLIP match rate: %s of %s OT CLIPs matched in property file (%.2f%%)",
  formatC(n_matched, format = "d", big.mark = ","),
  formatC(n_ot_clips, format = "d", big.mark = ","),
  pct_matched
))

# Identify unmatched CLIPs for post-join diagnostics — kept in environment
unmatched_clips <- ot_clips[!ot_clips %in% prop_clips]
cat("\n  Unmatched CLIP sample (up to 20):\n")
print(head(unmatched_clips, 20))

# Are unmatched OT rows concentrated in any sale year or corp group?
# prop_matched here = TRUE means the clip_id was found in the property file
ot_check <- ot %>%
  dplyr::mutate(prop_matched = clip_id %in% prop_clips,
                sale_year    = lubridate::year(sale_date))

cat("\n  Sale year x prop_matched (rows, not unique CLIPs):\n")
print(table(ot_check$sale_year, ot_check$prop_matched,
            dnn = c("sale_year", "prop_matched")))

# buy1_corp x match rate — check for systematic bias
cat("\n  buy1_corp x prop_matched:\n")
print(table(ot_check$buy1_corp, ot_check$prop_matched,
            dnn = c("buy1_corp", "prop_matched"), useNA = "always"))

rm(ot_check)
# keep: ot_clips, prop_clips, unmatched_clips — used for post-join flag (Section 6)
rm(n_ot_clips, n_prop_clips, n_matched, n_unmatched, pct_matched)


# *****************************************************
# 4. Drop OT columns superseded by property file ----
# *****************************************************
# These fields originated in the property file; the property version is
# canonical. Drop from OT before joining to avoid carrying duplicates.
#
#   OT column        Property column    Notes
#   fips             fips_code          same field, different name
#   apn_unformatted  apn                same field, different name
#   prop_indicator   (prop file = SFR only, already filtered)
#   residential      (prop file = SFR only, already filtered)
#
#   NOTE: mobile_home (OT) and manuf_home (property) are DISTINCT concepts
#   and are both retained. mobile_home = pre-1976 HUD code; manuf_home =
#   post-1976 HUD code. Do not conflate.
#   bldg_count       (not in prop_slim — retain from OT if present)
# *****************************************************
ts("Dropping OT columns superseded by property file...")

ot_drop <- c("fips", "apn_unformatted", "prop_indicator", "residential")
ot_drop <- intersect(ot_drop, names(ot))   # only drop what actually exists

cat(sprintf("  Dropping from OT (%d): %s\n", length(ot_drop), paste(ot_drop, collapse = ", ")))
ot <- ot[, !names(ot) %in% ot_drop]

drop_list <- c(drop_list, sprintf(
  "OT columns dropped before join (superseded by property file): %s",
  paste(ot_drop, collapse = ", ")
))
rm(ot_drop)

cat(sprintf("  OT after drop: %s rows x %d columns\n",
            formatC(nrow(ot), format = "d", big.mark = ","), ncol(ot)))


# *****************************************************
# 5. Left join ----
# *****************************************************
ts("Joining owner_transfer (left) to prop_slim on clip_id == clip...")

analytic <- dplyr::left_join(
  ot,
  prop,
  by = c("clip_id" = "clip")
)

cat(sprintf("  Joined: %s rows x %d columns\n",
            formatC(nrow(analytic), format = "d", big.mark = ","), ncol(analytic)))

# Confirm row count preserved (left join must equal nrow(ot))
# Fan-out would mean duplicate CLIPs in the property file — confirmed absent in 1-03,
# but guard retained as a sanity check.
if (nrow(analytic) != nrow(ot)) {
  warning(sprintf(
    "Row count changed after join! Before: %d  After: %d  — unexpected duplicate CLIPs in property file.",
    nrow(ot), nrow(analytic)
  ))
  drop_list <- c(drop_list, sprintf(
    "WARNING: row count changed after join (before=%d, after=%d) — investigate duplicate property CLIPs",
    nrow(ot), nrow(analytic)
  ))
} else {
  cat("  Row count preserved (left join confirmed, no property fan-out).\n")
  drop_list <- c(drop_list, "left join row count confirmed: no fan-out from property duplicates")
}

drop_list <- c(drop_list, sprintf(
  "analytic joined file: %s rows x %d columns",
  formatC(nrow(analytic), format = "d", big.mark = ","), ncol(analytic)
))


# *****************************************************
# 6. Post-join validation ----
# *****************************************************
ts("Post-join validation...")

# prop_matched: set directly from whether clip_id was found in property file.
# This is the authoritative flag — do NOT use presence of lat/lon or any other
# property field as a proxy, since those can be legitimately missing even when
# the join succeeded.
analytic$prop_matched <- as.integer(analytic$clip_id %in% prop_clips)

n_prop_matched   <- sum(analytic$prop_matched == 1L)
n_prop_unmatched <- sum(analytic$prop_matched == 0L)
pct_matched_rows <- round(100 * n_prop_matched / nrow(analytic), 2)

cat(sprintf("  Rows with property match:    %s (%.2f%%)\n",
            formatC(n_prop_matched,   format = "d", big.mark = ","), pct_matched_rows))
cat(sprintf("  Rows without property match: %s (%.2f%%)\n",
            formatC(n_prop_unmatched, format = "d", big.mark = ","), 100 - pct_matched_rows))

# Every OT transaction SHOULD match a property record — flag if any do not.
if (n_prop_unmatched > 0) {
  warning(sprintf(
    "%s OT rows have no property match. Expectation: every transaction has a property file entry. Investigate.",
    formatC(n_prop_unmatched, format = "d", big.mark = ",")
  ))
  # Pull unmatched rows with key OT context columns for manual review
  unmatched_df <- analytic[analytic$prop_matched == 0L,
                           intersect(c("clip_id", "prev_clip", "sale_date", "sale_amt",
                                       "buy1_name", "buy1_corp", "sell1_name",
                                       "sale_doc_type", "batch_date"),
                                     names(analytic))]
  cat(sprintf("\n  Unmatched rows saved as 'unmatched_df' (%s rows x %d columns)\n",
              formatC(nrow(unmatched_df), format = "d", big.mark = ","), ncol(unmatched_df)))
  cat("\n  Head of unmatched_df:\n")
  print(head(unmatched_df, 20))
} else {
  cat("  All OT rows matched to a property record — as expected.\n")
  unmatched_df <- data.frame()   # empty placeholder so downstream code can reference it safely
}

n_unique_unmatched   <- length(unique(analytic$clip_id[analytic$prop_matched == 0L]))
n_unique_ot          <- length(unique(analytic$clip_id))
pct_unique_unmatched <- round(100 * n_unique_unmatched / n_unique_ot, 2)

cat(sprintf("  Unique OT CLIPs unmatched: %s of %s (%.2f%%)\n",
            formatC(n_unique_unmatched, format = "d", big.mark = ","),
            formatC(n_unique_ot,        format = "d", big.mark = ","),
            pct_unique_unmatched))

rm(ot_clips, prop_clips, unmatched_clips)

drop_list <- c(drop_list, sprintf(
  "post-join rows unmatched: %s (%.2f%%) | unique CLIPs unmatched: %s of %s (%.2f%%) — INVESTIGATE if > 0",
  formatC(n_prop_unmatched,   format = "d", big.mark = ","), 100 - pct_matched_rows,
  formatC(n_unique_unmatched, format = "d", big.mark = ","),
  formatC(n_unique_ot,        format = "d", big.mark = ","),
  pct_unique_unmatched
))

# Distribution of key property fields post-join — distinguish join miss vs true missingness
cat("\n  NA rate on key property columns (matched rows only):\n")
prop_key_cols <- intersect(
  c("lat", "lon", "sqft_living", "yr_built", "bdrms", "baths",
    "value_calc", "county", "city", "zip"),
  names(analytic)
)
matched_rows <- analytic[analytic$prop_matched == 1L, ]
for (col in prop_key_cols) {
  n_miss <- sum(is.na(matched_rows[[col]]))
  cat(sprintf("    %-20s  %s missing (%.1f%% of matched rows)\n", col,
              formatC(n_miss, format = "d", big.mark = ","),
              100 * n_miss / nrow(matched_rows)))
}
rm(matched_rows)

# buy1_corp rate: matched vs unmatched — check for systematic bias in corporate sample
cat("\n  buy1_corp rate: matched vs unmatched rows:\n")
print(prop.table(table(analytic$buy1_corp, analytic$prop_matched,
                       dnn = c("buy1_corp", "prop_matched")), margin = 2))# *****************************************************
# 7. Quick checks — analytic file ----
# *****************************************************
ts("Quick checks on joined analytic file...")

cat(sprintf("\n  Rows:    %s\n", formatC(nrow(analytic), format = "d", big.mark = ",")))
cat(sprintf("  Columns: %d\n", ncol(analytic)))

cat("\n  Column names:\n")
print(names(analytic))

cat("\n  Sale date range:\n")
print(summary(analytic$sale_date))

cat("\n  buy1_corp distribution:\n")
print(table(analytic$buy1_corp, useNA = "always"))

cat("\n  investor flag:\n")
print(table(analytic$investor, useNA = "always"))

if ("county" %in% names(analytic)) {
  cat("\n  Top 15 counties:\n")
  print(sort(table(analytic$county, useNA = "always"), decreasing = TRUE)[1:15])
}

# Sale date histogram — joined analytic file
dir.create(paste0(root, "Process/Images"), showWarnings = FALSE, recursive = TRUE)

p1 <- ggplot(analytic, aes(x = sale_date, fill = factor(prop_matched))) +
  geom_histogram(binwidth = 91, color = "white", position = "stack") +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  scale_fill_manual(values = c("0" = "#d62728", "1" = "steelblue"),
                    labels = c("0" = "No property match", "1" = "Matched"),
                    name   = NULL) +
  labs(title = "Sale Date — joined analytic file (2019+, 3-month bins)",
       subtitle = "Red = unmatched to property file",
       x = "Sale Date", y = "Count") +
  theme_minimal()
print(p1)
ggsave(paste0(root, "Process/Images/1-04_sale_date_joined.png"),
       plot = p1, width = 10, height = 5, dpi = 150)

message("Elapsed so far: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")


# *****************************************************
# 8. Build property-wide file ----
# *****************************************************
# One row per property (clip). clip + lat + lon from prop_slim,
# plus wide transaction history: transactN_sale_date, transactN_sale_amt, transactN_buy1_corp.
# Use for: block-level transaction rates, multi-transaction counts, time filtering.
# *****************************************************
ts("Building property-wide file with transaction history (wide)...")

txn_cols <- c("sale_date", "sale_amt", "buy1_corp")

# Ensure clip types match between analytic and prop before joining
analytic$clip_id <- as.character(analytic$clip_id)
prop$clip        <- as.character(prop$clip)

analytic_ord <- analytic[order(analytic$clip_id, analytic$sale_date), ]
analytic_ord$txn_num <- ave(seq_len(nrow(analytic_ord)), analytic_ord$clip_id, FUN = seq_along)

txn_count_per_clip <- tapply(analytic_ord$txn_num, analytic_ord$clip_id, max)
max_txn <- max(txn_count_per_clip)
cat(sprintf("  Max transactions per property: %d\n", max_txn))

# Flag extreme transaction counts — likely data artifacts
high_txn_threshold <- 20L
n_high_txn <- sum(txn_count_per_clip > high_txn_threshold)
cat(sprintf("  Properties with > %d transactions: %s — review for data artifacts\n",
            high_txn_threshold, formatC(n_high_txn, format = "d", big.mark = ",")))
if (n_high_txn > 0) {
  cat("  Sample high-txn CLIPs:\n")
  print(head(sort(txn_count_per_clip[txn_count_per_clip > high_txn_threshold], decreasing = TRUE), 20))
}

cat("  Transaction count distribution:\n")
print(table(txn_count_per_clip))

# Hard drop CLIPs with > 7 transactions — once per year over 7 years is already
# generous for a real SFR. Above this is almost certainly data artifacts
# (CLIP reuse, parcel reassignment). 382 properties, 0.02% of unique CLIPs.
txn_drop_threshold <- 7L
clips_to_drop <- names(txn_count_per_clip)[txn_count_per_clip > txn_drop_threshold]
n_clips_dropped <- length(clips_to_drop)
n_rows_dropped  <- sum(analytic$clip_id %in% clips_to_drop)

cat(sprintf("  Dropping CLIPs with > %d transactions: %s CLIPs, %s rows (%.3f%% of unique CLIPs)\n",
            txn_drop_threshold,
            formatC(n_clips_dropped, format = "d", big.mark = ","),
            formatC(n_rows_dropped,  format = "d", big.mark = ","),
            100 * n_clips_dropped / length(txn_count_per_clip)))

drop_list <- c(drop_list, sprintf(
  "dropped CLIPs with > %d transactions (data artifacts): %s CLIPs, %s rows (0.02%% of unique CLIPs)",
  txn_drop_threshold,
  formatC(n_clips_dropped, format = "d", big.mark = ","),
  formatC(n_rows_dropped,  format = "d", big.mark = ",")
))

analytic     <- analytic[!analytic$clip_id %in% clips_to_drop, ]
analytic_ord <- analytic_ord[!analytic_ord$clip_id %in% clips_to_drop, ]
txn_count_per_clip <- txn_count_per_clip[!names(txn_count_per_clip) %in% clips_to_drop]

cat(sprintf("  analytic after drop: %s rows\n", formatC(nrow(analytic), format = "d", big.mark = ",")))
rm(clips_to_drop, n_clips_dropped, n_rows_dropped)

prop_txn_wide <- reshape(
  analytic_ord[, c("clip_id", "txn_num", txn_cols)],
  idvar = "clip_id", timevar = "txn_num", direction = "wide", sep = "_transact"
)
old_nms <- names(prop_txn_wide)
names(prop_txn_wide) <- sub("^(.+)_transact(\\d+)$", "transact\\2_\\1", old_nms)
names(prop_txn_wide)[names(prop_txn_wide) == "clip_id"] <- "clip"

prop_base <- prop[, c("clip", "lat", "lon")]
prop_wide <- dplyr::left_join(prop_base, prop_txn_wide, by = "clip")

# n_transact: look up from txn_count_per_clip using character clip as key
prop_wide$n_transact <- as.integer(txn_count_per_clip[as.character(prop_wide$clip)])
prop_wide$n_transact[is.na(prop_wide$n_transact)] <- 0L
prop_wide$transacted <- as.integer(prop_wide$n_transact > 0L)

cat(sprintf("  prop_wide: %s rows x %d columns\n",
            formatC(nrow(prop_wide), format = "d", big.mark = ","), ncol(prop_wide)))
cat(sprintf("  Transacted: %s  |  Not transacted: %s  |  Multi-txn: %s\n",
            formatC(sum(prop_wide$transacted == 1L), format = "d", big.mark = ","),
            formatC(sum(prop_wide$transacted == 0L), format = "d", big.mark = ","),
            formatC(sum(prop_wide$n_transact  >  1L), format = "d", big.mark = ",")))

drop_list <- c(drop_list, sprintf("prop_wide: max transactions per property (pre-cap): %d", max_txn))
rm(analytic_ord, prop_txn_wide, prop_base, old_nms, max_txn, txn_cols, high_txn_threshold, n_high_txn, txn_drop_threshold)


# *****************************************************
# 9. Save ----
# *****************************************************
ts("Saving outputs...")

out_analytic  <- paste0(data_output_s, "1-04_analytic.rds")
out_prop_wide <- paste0(data_output_s, "1-04_prop_wide.rds")
out_unmatched <- paste0(data_output_s, "1-04_unmatched_clips.rds")

saveRDS(analytic,  file = out_analytic);  cat(sprintf("  analytic  -> %s\n", out_analytic))
saveRDS(prop_wide, file = out_prop_wide); cat(sprintf("  prop_wide -> %s\n", out_prop_wide))
if (nrow(unmatched_df) > 0) {
  saveRDS(unmatched_df, file = out_unmatched)
  cat(sprintf("  unmatched -> %s\n", out_unmatched))
}

drop_list <- c(drop_list,
               sprintf("1-04_analytic.rds:  %s rows x %d cols", formatC(nrow(analytic),  format = "d", big.mark = ","), ncol(analytic)),
               sprintf("1-04_prop_wide.rds: %s rows x %d cols", formatC(nrow(prop_wide), format = "d", big.mark = ","), ncol(prop_wide)),
               if (nrow(unmatched_df) > 0) sprintf("1-04_unmatched_clips.rds: %s rows — investigate in 999", formatC(nrow(unmatched_df), format = "d", big.mark = ","))
)


# *****************************************************
# 10. Close out ----
# *****************************************************
cat("\n\nObjects saved:\n")
cat(sprintf("  analytic:  %s\n", out_analytic))
cat(sprintf("  prop_wide: %s\n", out_prop_wide))
if (nrow(unmatched_df) > 0) cat(sprintf("  unmatched: %s\n", out_unmatched))

# drop list -> Dropbox (no PII, used for write-up)
drop_list_file <- paste0(root, "Process/Logs/1-04_drop_list_", timestamp, ".txt")
writeLines(as.character(drop_list), drop_list_file)
cat(sprintf("  Drop list: %s\n", drop_list_file))

cat("\nDrop list summary:\n")
invisible(lapply(drop_list, function(x) cat(sprintf("  %s\n", x))))

cat("\n====================================================\n")
cat("  Decisions needed after reviewing output:\n")
cat("  1. Any unmatched OT rows? Expectation is zero — investigate with 999 file if not.\n")
cat("     Flag: prop_matched == 0; unmatched_df in environment + saved as 1-04_unmatched_clips.rds.\n")
cat("  2. Review NA rates on key property columns among MATCHED rows (Section 6).\n")
cat("     These are true missingness in the property file, not join failures.\n")
cat("  3. Review max transaction count per property — check for any data anomalies.\n")
cat("  4. Outputs:\n")
cat("       1-04_analytic.rds    — transacted properties (use for transaction-level analysis)\n")
cat("       1-04_unmatched_clips.rds — OT rows with no property match (investigate)\n")
cat("       1-04_prop_wide.rds   — all properties + wide txn history (use for block-level rates)\n")
cat("====================================================\n")

message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")

beep()
savehistory(paste0(secure, "Process/Fire Investment/Logs/1-04_history_", timestamp, ".txt"))
sink()


