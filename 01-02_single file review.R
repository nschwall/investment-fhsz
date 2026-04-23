


# **************************************************
#                     DETAILS
#
# Purpose:   Load and clean Cotality source files
#            (September 2025 Transfer):
#              - owner_transfer_144502  (OwnerTransfer, batch 1)
#              - owner_transfer_014500  (OwnerTransfer, batch 2)
#              - property               (current property characteristics)
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
data_output_s <- paste0(secure, "Data/Derived/")

transfer_root <- paste0(data_input_s, "Cotality/September 2025 Transfer")

# logging
timestamp  <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file   <- paste0(secure, "Process/Logs/1-02_log_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)

start_time <- Sys.time()
set.seed(123)

cat("\n====================================================\n")
cat("  1-02_load_clean.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

# load packages
pkgs <- c("dplyr", "tidyverse", "data.table", "beepr", "R.utils", "ggplot2", "lubridate")
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
  list.files(d,
             pattern = "^UNIVERSITY_OF_CALIFORNIA.*(OwnerTransfer|property3).*\\.csv$",
             full.names = TRUE, recursive = TRUE, ignore.case = TRUE)
}))

# exclude hist files — only want the two OwnerTransfer files and the one property file
csv_files <- csv_files[!grepl("hist_", csv_files, ignore.case = TRUE)]

cat(sprintf("Found %d CSV file(s):\n", length(csv_files)))
cat(paste0("  [", seq_along(csv_files), "] ", basename(csv_files), "\n"))

if (length(csv_files) == 0) stop("No matching CSV files found — check path.")


# *****************************************************
# 3. owner_transfer_144502 ----
# *****************************************************

f_ot1 <- csv_files[grepl("144502", csv_files)]
ts(sprintf("Loading %s", basename(f_ot1)))

owner_transfer_144502 <- as.data.frame(data.table::fread(
  f_ot1,
  na.strings = c("", "NA", "N/A", "null"),
  showProgress = TRUE
))

cat(sprintf("  %s rows x %d columns\n",
            formatC(nrow(owner_transfer_144502), format = "d", big.mark = ","),
            ncol(owner_transfer_144502)))

# --- 3a. Normalize column names ----
old_names <- names(owner_transfer_144502)
new_names <- tolower(old_names)
new_names <- gsub("[^a-z0-9]+", "_", new_names)
new_names <- gsub("_+", "_", new_names)
new_names <- sub("^_", "", new_names)
new_names <- sub("_$", "", new_names)
if (any(duplicated(new_names))) stop("Name collision after normalization — check columns.")
names(owner_transfer_144502) <- new_names

# --- 3b. Shorten fields ----
owner_transfer_144502 <- owner_transfer_144502 %>%
  dplyr::rename(
    # IDs & keys
    apn_unformatted     = apn_parcel_number_unformatted,
    apn_seq             = apn_sequence_number,
    composite_key       = composite_property_linkage_key,
    apn_orig            = original_apn,
    tax_acct            = tax_account_number,
    parcel_id           = online_formatted_parcel_id,
    txn_fips            = transaction_fips_code,
    txn_id              = owner_transfer_composite_transaction_id,
    batch_date          = transaction_batch_date,
    batch_seq           = transaction_batch_sequence_number,
    clip_id             = clip,
    prev_clip           = previous_clip,
    fips                = fips_code,
    
    # Use / property meta
    landuse_code        = land_use_code_static,
    county_use          = county_use_description_static,
    state_use           = state_use_description_static,
    prop_indicator      = property_indicator_code_static,
    zoning              = zoning_code_static,
    mobile_home         = mobile_home_indicator,
    year_built          = actual_year_built_static,
    year_built_eff      = effective_year_built_static,
    bldg_count          = total_number_of_buildings,
    
    # Situs (deed) address
    situs_house_num     = deed_situs_house_number_static,
    situs_house_num_suf = deed_situs_house_number_suffix_static,
    situs_house_num2    = deed_situs_house_number_2_static,
    situs_dir           = deed_situs_direction_static,
    situs_street        = deed_situs_street_name_static,
    situs_mode          = deed_situs_mode_static,
    situs_quad          = deed_situs_quadrant_static,
    situs_unit          = deed_situs_unit_number_static,
    situs_city          = deed_situs_city_static,
    situs_state         = deed_situs_state_static,
    situs_zip           = deed_situs_zip_code_static,
    situs_county        = deed_situs_county_static,
    situs_route         = deed_situs_carrier_route_static,
    situs_addr          = deed_situs_street_address_static,
    addr_conf_code      = standardized_address_confidence_code,
    
    # Sale / document
    sale_type           = sale_type_code,
    sale_amt            = sale_amount,
    sale_date           = sale_derived_date,
    sale_rec_date       = sale_derived_recording_date,
    sale_doc_type       = sale_document_type_code,
    sale_doc_num        = sale_recorded_document_number,
    sale_doc_book       = sale_recorded_document_book_number,
    sale_doc_page       = sale_recorded_document_page_number,
    own_xfer_pct        = ownership_transfer_percentage,
    
    # Flags
    pending             = pending_record_indicator,
    split_code          = multi_or_split_parcel_code,
    primary_cat         = primary_category_code,
    deed_cat            = deed_category_type_code,
    cash                = cash_purchase_indicator,
    mortgage            = mortgage_purchase_indicator,
    interfamily         = interfamily_related_indicator,
    investor            = investor_purchase_indicator,
    resale              = resale_indicator,
    new_constr          = new_construction_indicator,
    residential         = residential_indicator,
    short_sale          = short_sale_indicator,
    reo                 = foreclosure_reo_indicator,
    reo_sale            = foreclosure_reo_sale_indicator,
    
    # Buyer names + corp flags
    buy1_name           = buyer_1_full_name,
    buy1_last           = buyer_1_last_name,
    buy1_first_mi       = buyer_1_first_name_and_middle_initial,
    buy1_corp           = buyer_1_corporate_indicator,
    
    buy2_name           = buyer_2_full_name,
    buy2_last           = buyer_2_last_name,
    buy2_first_mi       = buyer_2_first_name_and_middle_initial,
    buy2_corp           = buyer_2_corporate_indicator,
    
    buy3_name           = buyer_3_full_name,
    buy3_last           = buyer_3_last_name,
    buy3_first_mi       = buyer_3_first_name_and_middle_initial,
    buy3_corp           = buyer_3_corporate_indicator,
    
    buy4_name           = buyer_4_full_name,
    buy4_last           = buyer_4_last_name,
    buy4_first_mi       = buyer_4_first_name_and_middle_initial,
    buy4_corp           = buyer_4_corporate_indicator,
    
    buy_etal            = buyer_etal_code,
    buy_rights          = buyer_ownership_rights_code,
    buy_rel             = buyer_relationship_type_code,
    buy_occ             = buyer_occupancy_code,
    partial_int         = partial_interest_indicator,
    
    # Buyer mailing
    buy_m_housenum      = buyer_mailing_house_number,
    buy_m_housenum_suf  = buyer_mailing_house_number_suffix,
    buy_m_housenum2     = buyer_mailing_house_number_2,
    buy_m_dir           = buyer_mailing_direction,
    buy_m_street        = buyer_mailing_street_name,
    buy_m_mode          = buyer_mailing_mode,
    buy_m_quad          = buyer_mailing_quadrant,
    buy_m_unit          = buyer_mailing_unit_number,
    buy_m_city          = buyer_mailing_city,
    buy_m_state         = buyer_mailing_state,
    buy_m_zip           = buyer_mailing_zip_code,
    buy_m_crt           = buyer_mailing_carrier_route,
    buy_m_addr          = buyer_mailing_street_address,
    buy_m_opt_out       = buyer_mailing_opt_out_indicator,
    
    # Sellers & record action
    sell1_name          = seller_1_full_name,
    sell1_last          = seller_1_last_name,
    sell1_first         = seller_1_first_name,
    sell2_name          = seller_2_full_name,
    record_action       = record_action_indicator
  )

cat(sprintf("  Cleaned: %s rows x %d columns\n",
            formatC(nrow(owner_transfer_144502), format = "d", big.mark = ","),
            ncol(owner_transfer_144502)))

# --- 3c. Quick checks ----
cat("\nSale date range:\n")
print(summary(as.Date(as.character(owner_transfer_144502$sale_date), format = "%Y%m%d")))
cat("\nbuy1_corp:\n")
print(table(owner_transfer_144502$buy1_corp, useNA = "always"))
cat("\ninvestor:\n")
print(table(owner_transfer_144502$investor, useNA = "always"))

# --- 3d. Save ----
out_ot1 <- paste0(data_output_s, "1-02_owner_transfer_144502.rds")
saveRDS(owner_transfer_144502, file = out_ot1)
cat(sprintf("\n  Saved -> %s\n", out_ot1))

message("Elapsed so far: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")


# ======================================================
# STOP HERE — check owner_transfer_144502 before continuing
# confirm: date range, buy1_corp coverage, investor flag,
#          and whether rename block above needs any adjustments
# ======================================================
stop("STOP — review owner_transfer_144502 before loading next file.
     Comment out this line when ready to continue.")


# *****************************************************
# 4. owner_transfer_014500 ----
# *****************************************************

f_ot2 <- csv_files[grepl("014500", csv_files)]
ts(sprintf("Loading %s", basename(f_ot2)))

owner_transfer_014500 <- as.data.frame(data.table::fread(
  f_ot2,
  na.strings = c("", "NA", "N/A", "null"),
  showProgress = TRUE
))

cat(sprintf("  %s rows x %d columns\n",
            formatC(nrow(owner_transfer_014500), format = "d", big.mark = ","),
            ncol(owner_transfer_014500)))

# --- 4a. Normalize column names ----
old_names <- names(owner_transfer_014500)
new_names <- tolower(old_names)
new_names <- gsub("[^a-z0-9]+", "_", new_names)
new_names <- gsub("_+", "_", new_names)
new_names <- sub("^_", "", new_names)
new_names <- sub("_$", "", new_names)
if (any(duplicated(new_names))) stop("Name collision after normalization — check columns.")
names(owner_transfer_014500) <- new_names

# --- 4b. Shorten fields ----
# same rename block as section 3b — columns should be identical
owner_transfer_014500 <- owner_transfer_014500 %>%
  dplyr::rename(
    apn_unformatted     = apn_parcel_number_unformatted,
    apn_seq             = apn_sequence_number,
    composite_key       = composite_property_linkage_key,
    apn_orig            = original_apn,
    tax_acct            = tax_account_number,
    parcel_id           = online_formatted_parcel_id,
    txn_fips            = transaction_fips_code,
    txn_id              = owner_transfer_composite_transaction_id,
    batch_date          = transaction_batch_date,
    batch_seq           = transaction_batch_sequence_number,
    clip_id             = clip,
    prev_clip           = previous_clip,
    fips                = fips_code,
    landuse_code        = land_use_code_static,
    county_use          = county_use_description_static,
    state_use           = state_use_description_static,
    prop_indicator      = property_indicator_code_static,
    zoning              = zoning_code_static,
    mobile_home         = mobile_home_indicator,
    year_built          = actual_year_built_static,
    year_built_eff      = effective_year_built_static,
    bldg_count          = total_number_of_buildings,
    situs_house_num     = deed_situs_house_number_static,
    situs_house_num_suf = deed_situs_house_number_suffix_static,
    situs_house_num2    = deed_situs_house_number_2_static,
    situs_dir           = deed_situs_direction_static,
    situs_street        = deed_situs_street_name_static,
    situs_mode          = deed_situs_mode_static,
    situs_quad          = deed_situs_quadrant_static,
    situs_unit          = deed_situs_unit_number_static,
    situs_city          = deed_situs_city_static,
    situs_state         = deed_situs_state_static,
    situs_zip           = deed_situs_zip_code_static,
    situs_county        = deed_situs_county_static,
    situs_route         = deed_situs_carrier_route_static,
    situs_addr          = deed_situs_street_address_static,
    addr_conf_code      = standardized_address_confidence_code,
    sale_type           = sale_type_code,
    sale_amt            = sale_amount,
    sale_date           = sale_derived_date,
    sale_rec_date       = sale_derived_recording_date,
    sale_doc_type       = sale_document_type_code,
    sale_doc_num        = sale_recorded_document_number,
    sale_doc_book       = sale_recorded_document_book_number,
    sale_doc_page       = sale_recorded_document_page_number,
    own_xfer_pct        = ownership_transfer_percentage,
    pending             = pending_record_indicator,
    split_code          = multi_or_split_parcel_code,
    primary_cat         = primary_category_code,
    deed_cat            = deed_category_type_code,
    cash                = cash_purchase_indicator,
    mortgage            = mortgage_purchase_indicator,
    interfamily         = interfamily_related_indicator,
    investor            = investor_purchase_indicator,
    resale              = resale_indicator,
    new_constr          = new_construction_indicator,
    residential         = residential_indicator,
    short_sale          = short_sale_indicator,
    reo                 = foreclosure_reo_indicator,
    reo_sale            = foreclosure_reo_sale_indicator,
    buy1_name           = buyer_1_full_name,
    buy1_last           = buyer_1_last_name,
    buy1_first_mi       = buyer_1_first_name_and_middle_initial,
    buy1_corp           = buyer_1_corporate_indicator,
    buy2_name           = buyer_2_full_name,
    buy2_last           = buyer_2_last_name,
    buy2_first_mi       = buyer_2_first_name_and_middle_initial,
    buy2_corp           = buyer_2_corporate_indicator,
    buy3_name           = buyer_3_full_name,
    buy3_last           = buyer_3_last_name,
    buy3_first_mi       = buyer_3_first_name_and_middle_initial,
    buy3_corp           = buyer_3_corporate_indicator,
    buy4_name           = buyer_4_full_name,
    buy4_last           = buyer_4_last_name,
    buy4_first_mi       = buyer_4_first_name_and_middle_initial,
    buy4_corp           = buyer_4_corporate_indicator,
    buy_etal            = buyer_etal_code,
    buy_rights          = buyer_ownership_rights_code,
    buy_rel             = buyer_relationship_type_code,
    buy_occ             = buyer_occupancy_code,
    partial_int         = partial_interest_indicator,
    buy_m_housenum      = buyer_mailing_house_number,
    buy_m_housenum_suf  = buyer_mailing_house_number_suffix,
    buy_m_housenum2     = buyer_mailing_house_number_2,
    buy_m_dir           = buyer_mailing_direction,
    buy_m_street        = buyer_mailing_street_name,
    buy_m_mode          = buyer_mailing_mode,
    buy_m_quad          = buyer_mailing_quadrant,
    buy_m_unit          = buyer_mailing_unit_number,
    buy_m_city          = buyer_mailing_city,
    buy_m_state         = buyer_mailing_state,
    buy_m_zip           = buyer_mailing_zip_code,
    buy_m_crt           = buyer_mailing_carrier_route,
    buy_m_addr          = buyer_mailing_street_address,
    buy_m_opt_out       = buyer_mailing_opt_out_indicator,
    sell1_name          = seller_1_full_name,
    sell1_last          = seller_1_last_name,
    sell1_first         = seller_1_first_name,
    sell2_name          = seller_2_full_name,
    record_action       = record_action_indicator
  )

cat(sprintf("  Cleaned: %s rows x %d columns\n",
            formatC(nrow(owner_transfer_014500), format = "d", big.mark = ","),
            ncol(owner_transfer_014500)))

# --- 4c. Quick checks ----
cat("\nSale date range:\n")
print(summary(as.Date(as.character(owner_transfer_014500$sale_date), format = "%Y%m%d")))
cat("\nColumn names identical to 144502?\n")
print(identical(names(owner_transfer_014500), names(owner_transfer_144502)))
cat("\nCLIP overlap with 144502:\n")
print(length(intersect(owner_transfer_014500$clip_id, owner_transfer_144502$clip_id)))

# --- 4d. Save ----
out_ot2 <- paste0(data_output_s, "1-02_owner_transfer_014500.rds")
saveRDS(owner_transfer_014500, file = out_ot2)
cat(sprintf("\n  Saved -> %s\n", out_ot2))

message("Elapsed so far: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")


# *****************************************************
# 5. property ----
# *****************************************************

f_prop <- csv_files[grepl("property3_dpc", csv_files, ignore.case = TRUE)]
ts(sprintf("Loading %s", basename(f_prop)))

property <- as.data.frame(data.table::fread(
  f_prop,
  na.strings = c("", "NA", "N/A", "null"),
  showProgress = TRUE
))

cat(sprintf("  %s rows x %d columns\n",
            formatC(nrow(property), format = "d", big.mark = ","),
            ncol(property)))

# --- 5a. Normalize column names ----
old_names <- names(property)
new_names <- tolower(old_names)
new_names <- gsub("[^a-z0-9]+", "_", new_names)
new_names <- gsub("_+", "_", new_names)
new_names <- sub("^_", "", new_names)
new_names <- sub("_$", "", new_names)
if (any(duplicated(new_names))) stop("Name collision after normalization — check columns.")
names(property) <- new_names

# --- 5b. Shorten fields ----
# TODO: review colnames(property) and fill in rename block
# key fields to keep will include: clip, fips, apn, lat/lon,
# situs address, owner fields, land use, property indicator,
# assessed values, year built, sq ft, unit count
# property <- property %>% dplyr::rename( ... )

cat("\nProperty column names for review:\n")
print(colnames(property))

# --- 5c. Save ----
out_prop <- paste0(data_output_s, "1-02_property.rds")
saveRDS(property, file = out_prop)
cat(sprintf("\n  Saved -> %s\n", out_prop))

message("Elapsed so far: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")


# ******************************
# 6. Close out ----

cat("\n\nObjects saved to derived folder:\n")
cat(sprintf("  %s\n", out_ot1))
cat(sprintf("  %s\n", out_ot2))
cat(sprintf("  %s\n", out_prop))

message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")

savehistory(paste0(secure, "Process/Logs/1-02_history_", timestamp, ".txt"))
sink()


