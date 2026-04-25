

# **************************************************
#                     DETAILS
#
# Purpose:   Scratch — load owner_transfer_144502, clean,
#            and explore corporate/other name-matching flags
#            on a random 500k row subset
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2026-04-23
# Started:   MM/DD/YYYY
# Updated:   MM/DD/YYYY
# **************************************************


# *************
# 1. Setup ----

user   <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root   <- paste0(user, "Fire Investment/")
secure <- "Y:/Institutional Investment/"

data_input_s  <- paste0(secure, "Data/Source/")
transfer_root <- paste0(data_input_s, "Cotality/September 2025 Transfer")

start_time <- Sys.time()

cat("\n====================================================\n")
cat("  999_owner_transfer_144502_explore.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

# load packages
pkgs <- c("dplyr", "tidyverse", "data.table", "ggplot2", "lubridate", "R.utils")
invisible(lapply(pkgs, function(p) {
  if (!require(p, character.only = TRUE)) {
    install.packages(p)
    library(p, character.only = TRUE)
  }
}))
rm(pkgs)

# timestamp defined AFTER packages load to avoid R.utils masking
timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file  <- paste0(secure, "Process/Fire Investment/Logs/999_owner_transfer_explore_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)

# helpers
ts <- function(label = "") {
  cat(sprintf("\n[%s]  %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), label))
}

convert_date_cols <- function(df) {
  date_cols <- names(df)[grepl("date", names(df), ignore.case = TRUE)]
  if (length(date_cols) == 0) {
    cat("  No columns with 'date' in name found.\n")
    return(df)
  }
  cat(sprintf("  Found %d date column(s): %s\n", length(date_cols),
              paste(date_cols, collapse = ", ")))
  cat("\n  Head of date columns:\n")
  print(as.data.frame(head(df[, date_cols, drop = FALSE])))
  for (col in date_cols) {
    sample_vals <- na.omit(as.character(df[[col]]))
    looks_yyyymmdd <- length(sample_vals) > 0 &&
      all(nchar(sample_vals[1:min(10, length(sample_vals))]) == 8)
    if (looks_yyyymmdd) {
      df[[col]] <- as.Date(as.character(df[[col]]), format = "%Y%m%d")
      cat(sprintf("  Converted %s -> Date\n", col))
    } else {
      cat(sprintf("  Skipped %s (does not look like YYYYMMDD)\n", col))
    }
  }
  df
}

recode_corp <- function(corp_col, name_col) {
  dplyr::case_when(
    corp_col == "Y"                     ~ 1L,
    is.na(corp_col) & !is.na(name_col) ~ 0L,
    TRUE                                ~ NA_integer_
  )
}


# *****************************************************
# 2. Discover and load owner_transfer_144502 ----
# *****************************************************
ts("Scanning for CSV files...")

skip_dirs <- c("Zip Folders", "Meta Data")
subdirs   <- list.dirs(transfer_root, full.names = TRUE, recursive = FALSE)
subdirs   <- subdirs[!basename(subdirs) %in% skip_dirs]

csv_files <- unlist(lapply(subdirs, function(d) {
  list.files(d,
             pattern = "^UNIVERSITY_OF_CALIFORNIA.*(OwnerTransfer|property3).*\\.csv$",
             full.names = TRUE, recursive = TRUE, ignore.case = TRUE)
}))
csv_files <- csv_files[!grepl("hist_", csv_files, ignore.case = TRUE)]

f_ot1 <- csv_files[grepl("144502", csv_files)]
ts(sprintf("Loading %s", basename(f_ot1)))

ot <- as.data.frame(data.table::fread(
  f_ot1,
  na.strings = c("", "NA", "N/A", "null"),
  showProgress = TRUE
))

cat(sprintf("  %s rows x %d columns\n",
            formatC(nrow(ot), format = "d", big.mark = ","),
            ncol(ot)))


# *****************************************************
# 3. Normalize column names ----
# *****************************************************
old_names <- names(ot)
new_names <- tolower(old_names)
new_names <- gsub("[^a-z0-9]+", "_", new_names)
new_names <- gsub("_+", "_", new_names)
new_names <- sub("^_", "", new_names)
new_names <- sub("_$", "", new_names)
if (any(duplicated(new_names))) stop("Name collision after normalization.")
names(ot) <- new_names


# *****************************************************
# 4. Convert date columns ----
# *****************************************************
ts("Converting date columns...")
ot <- convert_date_cols(ot)


# *****************************************************
# 5. Rename and select keepers ----
# *****************************************************
ot <- ot %>%
  dplyr::rename(
    clip_id             = clip,
    prev_clip           = previous_clip,
    fips                = fips_code,
    apn_unformatted     = apn_parcel_number_unformatted,
    landuse_code        = land_use_code_static,
    county_use          = county_use_description_static,
    state_use           = state_use_description_static,
    zoning              = zoning_code_static,
    prop_indicator      = property_indicator_code_static,
    mobile_home         = mobile_home_indicator,
    bldg_count          = total_number_of_buildings,
    situs_state         = deed_situs_state_static,
    situs_zip           = deed_situs_zip_code_static,
    situs_county        = deed_situs_county_static,
    situs_addr          = deed_situs_street_address_static,
    situs_city          = deed_situs_city_static,
    txn_id              = owner_transfer_composite_transaction_id,
    batch_date          = transaction_batch_date,
    sale_date           = sale_derived_date,
    sale_rec_date       = sale_derived_recording_date,
    sale_amt            = sale_amount,
    sale_type           = sale_type_code,
    sale_doc_type       = sale_document_type_code,
    deed_cat            = deed_category_type_code,
    primary_cat         = primary_category_code,
    pending             = pending_record_indicator,
    investor            = investor_purchase_indicator,
    residential         = residential_indicator,
    cash                = cash_purchase_indicator,
    mortgage            = mortgage_purchase_indicator,
    interfamily         = interfamily_related_indicator,
    new_constr          = new_construction_indicator,
    resale              = resale_indicator,
    short_sale          = short_sale_indicator,
    reo                 = foreclosure_reo_indicator,
    reo_sale            = foreclosure_reo_sale_indicator,
    buy1_name           = buyer_1_full_name,
    buy1_corp           = buyer_1_corporate_indicator,
    buy2_name           = buyer_2_full_name,
    buy2_corp           = buyer_2_corporate_indicator,
    buy3_name           = buyer_3_full_name,
    buy3_corp           = buyer_3_corporate_indicator,
    buy4_name           = buyer_4_full_name,
    buy4_corp           = buyer_4_corporate_indicator,
    buy_occ             = buyer_occupancy_code,
    buy_rights          = buyer_ownership_rights_code,
    partial_int         = partial_interest_indicator,
    sell1_name          = seller_1_full_name,
    record_action       = record_action_indicator
  ) %>%
  dplyr::select(
    clip_id, prev_clip, fips, apn_unformatted,
    landuse_code, county_use, state_use, zoning,
    prop_indicator, mobile_home, bldg_count,
    situs_addr, situs_city, situs_state, situs_zip, situs_county,
    txn_id, batch_date, sale_date, sale_rec_date,
    sale_amt, sale_type, sale_doc_type, deed_cat, primary_cat, pending,
    investor, residential, cash, mortgage, interfamily,
    new_constr, resale, short_sale, reo, reo_sale,
    buy1_name, buy1_corp,
    buy2_name, buy2_corp,
    buy3_name, buy3_corp,
    buy4_name, buy4_corp,
    buy_occ, buy_rights, partial_int,
    sell1_name, record_action
  )

cat(sprintf("  Slimmed to: %s rows x %d columns\n",
            formatC(nrow(ot), format = "d", big.mark = ","),
            ncol(ot)))


# *****************************************************
# 5b. Suss out key fields before subsetting ----
# *****************************************************

# checking what values prop_indicator takes and whether
# mobile_home is a useful complement for filtering to SFR
cat("\nprop_indicator x mobile_home:\n")
print(table(ot$prop_indicator, ot$mobile_home, useNA = "always"))

# checking whether bldg_count helps narrow down SFR
cat("\nprop_indicator x bldg_count:\n")
print(table(ot$prop_indicator, ot$bldg_count, useNA = "always"))

# checking what sale_doc_type values look like
cat("\nsale_doc_type:\n")
print(table(ot$sale_doc_type, useNA = "always"))

# cross-tab sale_doc_type x interfamily to validate non-arms-length flag
cat("\nsale_doc_type x interfamily:\n")
print(table(ot$sale_doc_type, ot$interfamily, useNA = "always"))

# CLIPs should have duplicates — one row per transaction
cat("\nTotal rows:\n")
print(nrow(ot))
cat("\nUnique CLIPs:\n")
print(length(unique(ot$clip_id)))
cat("\nCLIPs appearing more than once:\n")
print(sum(table(ot$clip_id) > 1))


# *****************************************************
# 5c. Recode buyX_corp on full file ----
# *****************************************************
# 1  = corporate (Y in original)
# 0  = buyer present but not flagged corporate
# NA = no buyer at that position

ts("Recoding buyX_corp indicators...")

ot$buy1_corp <- recode_corp(ot$buy1_corp, ot$buy1_name)
ot$buy2_corp <- recode_corp(ot$buy2_corp, ot$buy2_name)
ot$buy3_corp <- recode_corp(ot$buy3_corp, ot$buy3_name)
ot$buy4_corp <- recode_corp(ot$buy4_corp, ot$buy4_name)

cat("\nbuy1_corp after recode:\n")
print(table(ot$buy1_corp, useNA = "always"))

cat("\nbuy2_corp after recode:\n")
print(table(ot$buy2_corp, useNA = "always"))

cat("\nbuy3_corp after recode:\n")
print(table(ot$buy3_corp, useNA = "always"))

cat("\nbuy4_corp after recode:\n")
print(table(ot$buy4_corp, useNA = "always"))

cat("\nbuy1_corp x buy2_corp:\n")
print(table(ot$buy1_corp, ot$buy2_corp, useNA = "always"))

cat("\nbuy1_corp x buy3_corp:\n")
print(table(ot$buy1_corp, ot$buy3_corp, useNA = "always"))

cat("\nbuy1_corp x buy4_corp:\n")
print(table(ot$buy1_corp, ot$buy4_corp, useNA = "always"))

cat("\nbuy1_corp = 0 but buy2_corp = 1 (corporate entity listed second on deed):\n")
print(head(
  ot[!is.na(ot$buy1_corp) & ot$buy1_corp == 0 &
       !is.na(ot$buy2_corp) & ot$buy2_corp == 1,
     c("buy1_name", "buy2_name")],
  20
))

ot$corp_consistent <- apply(
  ot[, c("buy1_corp", "buy2_corp", "buy3_corp", "buy4_corp")],
  1,
  function(x) {
    present <- x[!is.na(x)]
    if (length(present) <= 1) return(NA)
    as.integer(length(unique(present)) == 1)
  }
)

cat("\ncorp_consistent:\n")
print(table(ot$corp_consistent, useNA = "always"))

cat("\nSample of inconsistent rows:\n")
print(head(
  ot[!is.na(ot$corp_consistent) & ot$corp_consistent == 0,
     c("buy1_name", "buy1_corp", "buy2_name", "buy2_corp",
       "buy3_name", "buy3_corp", "buy4_name", "buy4_corp")],
  20
))


# *****************************************************
# 5d. Explore prop_indicator with use descriptions ----
# *****************************************************
# using county_use, state_use, and zoning to decode prop_indicator
# goal is to identify which prop_indicator values correspond to SFR

ts("Exploring prop_indicator with use descriptions...")

# unique combinations of prop_indicator, landuse_code, county_use, state_use
# sorted by prop_indicator to group them — look for SFR patterns
cat("\nUnique prop_indicator x landuse_code x county_use (top 100):\n")
combos <- unique(ot[, c("prop_indicator", "landuse_code", "county_use", "state_use")])
combos <- combos[order(combos$prop_indicator, combos$landuse_code), ]
print(head(combos, 100))

# frequency of each prop_indicator — how many transactions per type
cat("\nprop_indicator frequency:\n")
print(sort(table(ot$prop_indicator, useNA = "always"), decreasing = TRUE))

# for each prop_indicator, what are the most common county_use descriptions?
# this is the most direct way to decode what each code means
cat("\nTop county_use values by prop_indicator:\n")
for (pi in sort(unique(na.omit(ot$prop_indicator)))) {
  cat(sprintf("\n  prop_indicator = %s:\n", pi))
  sub <- ot$county_use[ot$prop_indicator == pi]
  tbl <- sort(table(sub, useNA = "always"), decreasing = TRUE)
  print(head(tbl, 5))
}

# zoning codes by prop_indicator — secondary check
# zoning is county-specific so less clean but may help for edge cases
cat("\nTop zoning values by prop_indicator:\n")
for (pi in sort(unique(na.omit(ot$prop_indicator)))) {
  cat(sprintf("\n  prop_indicator = %s:\n", pi))
  sub <- ot$zoning[ot$prop_indicator == pi]
  tbl <- sort(table(sub, useNA = "always"), decreasing = TRUE)
  print(head(tbl, 5))
}


# *****************************************************
# 6. Histograms (full file) ----
# *****************************************************
dir.create(paste0(root, "Process/Images"), showWarnings = FALSE, recursive = TRUE)

p1 <- ggplot(ot, aes(x = sale_date)) +
  geom_histogram(binwidth = 365, fill = "steelblue", color = "white") +
  scale_x_date(date_breaks = "5 years", date_labels = "%Y") +
  labs(title = "Sale Date Distribution — owner_transfer_144502 (all dates)",
       x = "Sale Date", y = "Count") +
  theme_minimal()

print(p1)

ggsave(paste0(root, "Process/Images/999_sale_date_hist_144502_all.png"),
       plot = p1, width = 10, height = 5, dpi = 150)

p2 <- ggplot(
  dplyr::filter(ot, sale_date >= as.Date("2015-01-01")),
  aes(x = sale_date)) +
  geom_histogram(binwidth = 91, fill = "steelblue", color = "white") +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  labs(title = "Sale Date Distribution — owner_transfer_144502 (2015+, 3-month bins)",
       x = "Sale Date", y = "Count") +
  theme_minimal()

print(p2)

ggsave(paste0(root, "Process/Images/999_sale_date_hist_144502_2015plus.png"),
       plot = p2, width = 10, height = 5, dpi = 150)


# *****************************************************
# 7. Random 500k subset for flag exploration ----
# *****************************************************
ts("Sampling 500k rows for flag exploration...")

set.seed(123)
ot_sub <- ot[sample(nrow(ot), 500000), ]

cat(sprintf("  Subset: %s rows x %d columns\n",
            formatC(nrow(ot_sub), format = "d", big.mark = ","),
            ncol(ot_sub)))


# *****************************************************
# 8. Corporate / other name-matching flags ----
# *****************************************************
ts("Creating name-matching flags...")

corp_keywords <- "\\b(properties|corp|llc|ltd|homes|inc|lp|investments|borrower|development|holdings|residential|land|american|company|houses|rent|home|group|builders|partners|investment|holding|housing|management|partnership|realty|enterprises|interests|series|bank|limited|associates|corporation|real estate|realestate|acquisition|acquisitions|capital|ventures|assets|fund|reit|blackstone|invitation|progress|tricon|amherst|firstkey|vinebrook|cerberus|pretium|fundrise|roofstock|opendoor|offerpad|zillow|mynd|starwood|brookfield|belonging|hldg co|ptshp)\\b"

other_keywords <- "\\b(trust|district|current owner|poa|trustee|guardian|custodian|esq|atty|et al|estate)\\b"

ot_sub$corp_flag  <- grepl(corp_keywords, ot_sub$buy1_name, ignore.case = TRUE)
ot_sub$other_flag <- grepl(other_keywords, ot_sub$buy1_name, ignore.case = TRUE)

ot_sub$other_flag <- ifelse(
  grepl("real estate", ot_sub$buy1_name, ignore.case = TRUE),
  FALSE,
  ot_sub$other_flag
)

ot_sub$corp_flag <- ifelse(ot_sub$other_flag == TRUE, FALSE, ot_sub$corp_flag)

cat("\ncorp_flag:\n")
print(table(ot_sub$corp_flag, useNA = "always"))

cat("\nother_flag:\n")
print(table(ot_sub$other_flag, useNA = "always"))

cat("\ncorp_flag x other_flag (should be no TRUE/TRUE):\n")
print(table(ot_sub$corp_flag, ot_sub$other_flag, useNA = "always"))

cat("\ncorp_flag x buy1_corp:\n")
print(table(ot_sub$corp_flag, ot_sub$buy1_corp, useNA = "always"))

cat("\ncorp_flag x investor:\n")
print(table(ot_sub$corp_flag, ot_sub$investor, useNA = "always"))

cat("\ncorp_flag x buy_occ:\n")
print(table(ot_sub$corp_flag, ot_sub$buy_occ, useNA = "always"))

cat("\nother_flag x buy1_corp:\n")
print(table(ot_sub$other_flag, ot_sub$buy1_corp, useNA = "always"))

cat("\nSample buy1_name where corp_flag = TRUE:\n")
print(head(unique(ot_sub$buy1_name[ot_sub$corp_flag == TRUE]), 30))

cat("\nSample buy1_name where other_flag = TRUE:\n")
print(head(unique(ot_sub$buy1_name[ot_sub$other_flag == TRUE]), 30))

cat("\ncorp_flag = FALSE but buy1_corp = 1 (Cotality says corporate, keywords missed):\n")
print(head(unique(ot_sub$buy1_name[ot_sub$corp_flag == FALSE & ot_sub$buy1_corp == 1]), 30))

cat("\ncorp_flag = TRUE but buy1_corp = 0 (keywords caught, Cotality missed):\n")
print(head(unique(ot_sub$buy1_name[ot_sub$corp_flag == TRUE & ot_sub$buy1_corp == 0]), 30))

cat("\nMost common words in corp_flag = FALSE but buy1_corp = 1:\n")
missed <- ot_sub$buy1_name[ot_sub$corp_flag == FALSE & ot_sub$buy1_corp == 1]
missed <- na.omit(missed)
words  <- unlist(strsplit(tolower(missed), "\\s+"))
print(sort(table(words), decreasing = TRUE)[1:50])


# *****************************************************
# 9. buy1_corp and investor flag comparison ----
# *****************************************************
cat("\nbuy1_corp x investor:\n")
print(table(ot_sub$buy1_corp, ot_sub$investor, useNA = "always"))

cat("\ninvestor = 1, buy1_corp = 0 (individuals Cotality flags as investor):\n")
print(head(unique(ot_sub$buy1_name[ot_sub$investor == 1 & ot_sub$buy1_corp == 0]), 30))

cat("\nbuy1_corp = 1, investor = 0 (corporates Cotality doesn't flag as investor):\n")
print(head(unique(ot_sub$buy1_name[ot_sub$buy1_corp == 1 & ot_sub$investor == 0]), 30))


# *****************************************************
# 10. buy1_corp and buy_occ comparison ----
# *****************************************************
cat("\nbuy1_corp x buy_occ:\n")
print(table(ot_sub$buy1_corp, ot_sub$buy_occ, useNA = "always"))

cat("\ninvestor x buy_occ:\n")
print(table(ot_sub$investor, ot_sub$buy_occ, useNA = "always"))

cat("\nbuy1_corp = 1, buy_occ = S (corporate but owner-occupied — data quality check):\n")
print(head(unique(ot_sub$buy1_name[ot_sub$buy1_corp == 1 & ot_sub$buy_occ == "S"]), 30))

cat("\nbuy1_corp = 0, buy_occ = T (absentee non-corporate — individual/small landlords):\n")
print(head(unique(ot_sub$buy1_name[ot_sub$buy1_corp == 0 & ot_sub$buy_occ == "T"]), 30))

cat("\ninvestor = 1, buy_occ = S (flagged investor but owner-occupied — odd cases):\n")
print(head(unique(ot_sub$buy1_name[ot_sub$investor == 1 & ot_sub$buy_occ == "S"]), 30))

cat("\ninvestor = 0, buy_occ = T (absentee but not flagged investor):\n")
print(head(unique(ot_sub$buy1_name[ot_sub$investor == 0 & ot_sub$buy_occ == "T"]), 30))


# ******************************
# 11. Close out ----

message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")

savehistory(paste0(secure, "Process/Fire Investment/Logs/999_owner_transfer_explore_history_", timestamp, ".txt"))
sink()

