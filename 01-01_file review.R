

# **************************************************
#                     DETAILS
#
# Purpose:   Review Owner Transfer Files
# Author:    Nora Schwaller
# Started:   09/30/2025
# Updated:   MM/DD/YYYY
# **************************************************


# *************
# 1. Setup ----


# UPDATE THESE PATHS TO YOUR LOCAL MACHINE
# set user
user <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root <- paste0(user, "Fire Investment/")  
secure <- ("Y:/Institutional Investment/")

# define folders 
data_input    <- paste0(root, "Data/Source/")
data_output   <- paste0(root, "Data/Derived/")

data_input_s    <- paste0(secure, "Data/Source")
data_output_s   <- paste0(secure, "Data/Derived/")



# UPDATE AA-BB TO MATCH THIS SCRIPT'S NUMBER
# logging
timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file <- paste0(root, "Process/Logs/01-01_log_", timestamp, ".txt")  # UPDATE AA-BB
sink(log_file, split = TRUE)

start_time <- Sys.time()
set.seed(123) 


#load packages - add packages needed for script, installs if needed
pkgs <- c("dplyr", "tidyverse", "data.table", "beepr", "R.utils", "ggplot2", "lubridate")  
invisible(lapply(pkgs, function(p) {
  if (!require(p, character.only = TRUE)) {
    install.packages(p)
    library(p, character.only = TRUE)
  }
}))
rm(pkgs)


# *******************
# 2. Load data ----

owner_transfer <- data.table::fread(
  file = file.path(
    data_input_s, "Cotality",
    "UNIVERSITY_OF_CALIFORNIA_SAN_DIEGO_OwnerTransfer_v3_dpc_01930332_20250908_144502_data",
    "UNIVERSITY_OF_CALIFORNIA_SAN_DIEGO_OwnerTransfer_v3_dpc_01930332_20250908_144502_data.csv"
  ),
  na.strings = c("", "NA", "N/A", "null"),
  showProgress = TRUE
)


temp <- owner_transfer
owner_transfer <- temp


prop_dpc <- data.table::fread(
  file = file.path(
    data_input_s, "Cotality",
    "UNIVERSITY_OF_CALIFORNIA_SAN_DIEGO_property3_dpc_01930334_20250908_144500_data",
    "UNIVERSITY_OF_CALIFORNIA_SAN_DIEGO_property3_dpc_01930334_20250908_144500_data.csv"
  ),
  na.strings = c("", "NA", "N/A", "null"),
  showProgress = TRUE
)


# *******************
# 3. clean up data ----

colnames(owner_transfer)
head(as.data.frame(owner_transfer))

#Normalize names: lowercase + underscores (with a safety check)
old_names <- names(owner_transfer)
new_names <- tolower(old_names)
new_names <- gsub("[^a-z0-9]+", "_", new_names)  # spaces, punctuation -> "_"
new_names <- gsub("_+", "_", new_names)          # collapse multiple "_"
new_names <- sub("^_", "", new_names)            # trim leading "_"
new_names <- sub("_$", "", new_names)            # trim trailing "_"

if (any(duplicated(new_names))) {
  dups <- unique(new_names[duplicated(new_names)])
  stop("Name collisions after normalization: ", paste(dups, collapse = ", "),
       "\nExamples:\n",
       paste(utils::head(
         apply(cbind(old_names, new_names), 1, paste, collapse = " -> "),
         20
       ), collapse = "\n"))
}
names(owner_transfer) <- new_names


# 2b) Shorten fields
owner_transfer <- owner_transfer %>%
  dplyr::rename(
    # IDs & keys
    apn_unformatted = apn_parcel_number_unformatted,
    apn_seq         = apn_sequence_number,
    composite_key   = composite_property_linkage_key,
    apn_orig        = original_apn,
    tax_acct        = tax_account_number,
    parcel_id       = online_formatted_parcel_id,
    txn_fips        = transaction_fips_code,
    txn_id          = owner_transfer_composite_transaction_id,
    batch_date      = transaction_batch_date,
    batch_seq       = transaction_batch_sequence_number,
    clip_id         = clip,
    prev_clip       = previous_clip,
    fips            = fips_code,
    
    # Use / property meta
    landuse_code    = land_use_code_static,
    county_use      = county_use_description_static,
    state_use       = state_use_description_static,
    prop_indicator  = property_indicator_code_static,
    zoning          = zoning_code_static,
    mobile_home     = mobile_home_indicator,
    year_built      = actual_year_built_static,
    year_built_eff  = effective_year_built_static,
    bldg_count      = total_number_of_buildings,
    
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
    sale_type     = sale_type_code,
    sale_amt      = sale_amount,
    sale_date     = sale_derived_date,
    sale_rec_date = sale_derived_recording_date,
    sale_doc_type = sale_document_type_code,
    sale_doc_num  = sale_recorded_document_number,
    sale_doc_book = sale_recorded_document_book_number,
    sale_doc_page = sale_recorded_document_page_number,
    own_xfer_pct  = ownership_transfer_percentage,
    
    # Flags
    pending      = pending_record_indicator,
    split_code   = multi_or_split_parcel_code,
    primary_cat  = primary_category_code,
    deed_cat     = deed_category_type_code,
    cash         = cash_purchase_indicator,
    mortgage     = mortgage_purchase_indicator,
    interfamily  = interfamily_related_indicator,
    investor     = investor_purchase_indicator,
    resale       = resale_indicator,
    new_constr   = new_construction_indicator,
    residential  = residential_indicator,
    short_sale   = short_sale_indicator,
    reo          = foreclosure_reo_indicator,
    reo_sale     = foreclosure_reo_sale_indicator,
    
    # Buyer names + corp flags
    buy1_name     = buyer_1_full_name,
    buy1_last     = buyer_1_last_name,
    buy1_first_mi = buyer_1_first_name_and_middle_initial,
    buy1_corp     = buyer_1_corporate_indicator,
    
    buy2_name     = buyer_2_full_name,
    buy2_last     = buyer_2_last_name,
    buy2_first_mi = buyer_2_first_name_and_middle_initial,
    buy2_corp     = buyer_2_corporate_indicator,
    
    buy3_name     = buyer_3_full_name,
    buy3_last     = buyer_3_last_name,
    buy3_first_mi = buyer_3_first_name_and_middle_initial,
    buy3_corp     = buyer_3_corporate_indicator,
    
    buy4_name     = buyer_4_full_name,
    buy4_last     = buyer_4_last_name,
    buy4_first_mi = buyer_4_first_name_and_middle_initial,
    buy4_corp     = buyer_4_corporate_indicator,
    
    buy_etal   = buyer_etal_code,
    buy_rights = buyer_ownership_rights_code,
    buy_rel    = buyer_relationship_type_code,
    # buy_c_o    = buyer_care_of_name,
    buy_occ    = buyer_occupancy_code,
    partial_int  = partial_interest_indicator,
    
    # Buyer mailing
    buy_m_housenum     = buyer_mailing_house_number,
    buy_m_housenum_suf = buyer_mailing_house_number_suffix,
    buy_m_housenum2    = buyer_mailing_house_number_2,
    buy_m_dir          = buyer_mailing_direction,
    buy_m_street       = buyer_mailing_street_name,
    buy_m_mode         = buyer_mailing_mode,
    buy_m_quad         = buyer_mailing_quadrant,
    buy_m_unit         = buyer_mailing_unit_number,
    buy_m_city         = buyer_mailing_city,
    buy_m_state        = buyer_mailing_state,
    buy_m_zip          = buyer_mailing_zip_code,
    buy_m_crt          = buyer_mailing_carrier_route,
    buy_m_addr         = buyer_mailing_street_address,
    buy_m_opt_out      = buyer_mailing_opt_out_indicator,
    
    # Sellers & record action
    sell1_name   = seller_1_full_name,
    sell1_last   = seller_1_last_name,
    sell1_first  = seller_1_first_name,
    sell2_name   = seller_2_full_name,
    record_action  = record_action_indicator
)


head(as.data.frame(owner_transfer))
colnames(owner_transfer)


# *******************
# 4. start looking at relevant variables ----


table(owner_transfer$buy1_corp, useNA = "always")
with(owner_transfer, table(is.na(sale_date), buy1_corp, useNA = "always"))



table(owner_transfer$situs_state, useNA = "always")
with(owner_transfer, table(situs_state, buy1_corp, useNA = "always"))


table(owner_transfer$landuse_code, useNA = "always")
with(owner_transfer, table(landuse_code , buy1_corp, useNA = "always"))





# ---- NAs and duplicates: clip_id 
table(is.na(owner_transfer$clip_id))
table(duplicated(owner_transfer$clip_id))  # TRUE = duplicate rows (incl. NA beyond the first)

# ---- NAs and duplicates: apn_unformatted 
table(is.na(owner_transfer$apn_unformatted))
table(duplicated(owner_transfer$apn_unformatted))

# ---- NAs and duplicates: parcel_id
table(is.na(owner_transfer$parcel_id))
table(duplicated(owner_transfer$parcel_id))




# *******************
# 4. histograms ----

owner_transfer$sale_date <- ymd(owner_transfer$sale_date)
summary(owner_transfer$sale_date)
table(owner_transfer$sale_date > as.Date("2019-01-01"))

# 1) buy1_corp == "Y"
ggplot(dplyr::filter(owner_transfer, buy1_corp == "Y", !is.na(sale_date), sale_date > as.Date("2019-01-01")),
       aes(x = sale_date)) +
  geom_histogram(binwidth = 30, boundary = as.Date("2000-01-01")) +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  labs(title = "Sale Date Histogram — buy1_corp = 'Y'", x = "Sale date", y = "Count")

# 2) buy1_corp is NA
ggplot(dplyr::filter(owner_transfer, is.na(buy1_corp), !is.na(sale_date), sale_date > as.Date("2019-01-01")),
       aes(x = sale_date)) +
  geom_histogram(binwidth = 30, boundary = as.Date("2000-01-01")) +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  labs(title = "Sale Date Histogram — buy1_corp = NA", x = "Sale date", y = "Count")



ggplot(owner_transfer[buy1_corp == "Y" & !is.na(sale_date)], aes(x = sale_amt)) +
  geom_histogram(binwidth = 50000, fill = "steelblue", color = "white") +
  labs(title = "Histogram: Buyer1 = Corporate (Y)", x = "Sale Amount", y = "Count")





# histogram for buyer 1 corporate = "Y"
ggplot(owner_transfer[buy1_corp == "Y"], aes(x = sale_amt)) +
  geom_histogram(binwidth = 50000, fill = "steelblue", color = "white") +
  labs(title = "Histogram: Buyer1 = Corporate (Y)", x = "Sale Amount", y = "Count")

# histogram for buyer 1 corporate = NA (non-corporate buyers)
ggplot(owner_transfer[is.na(buy1_corp)], aes(x = sale_amt)) +
  geom_histogram(binwidth = 50000, fill = "darkred", color = "white") +
  labs(title = "Histogram: Buyer1 = Not Corporate (NA)", x = "Sale Amount", y = "Count")





# ******************************
# X. Save & Close out


# ---- Subset situs fields & save to secure output ----
situs_subset <- owner_transfer %>%
  dplyr::select(
    situs_house_num,
    situs_house_num_suf,
    situs_house_num2,
    situs_dir,
    situs_street,
    situs_mode,
    situs_quad,
    situs_unit,
    situs_city,
    situs_state,
    situs_zip
  )




# ******************************
# X. Save & Close out


# # save file
# saveRDS(x, file = paste0(data_output, "AA-BB_description.rds"))
# save(x, file = paste0(data_output, "AA-BB_description.RData"))

# time spent
message("Elapsed: ", round(difftime(Sys.time(), start_time, units = "mins"), 2),
        " minutes")


# UPDATE AA-BB TO MATCH THIS SCRIPT'S NUMBER
# record session info
savehistory(paste0(root, "Process/Logs/01-01_history_", timestamp, ".txt")) 
sink() #closes log


