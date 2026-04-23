


# ============================================================
# Download CAL FIRE Fire Severity Zones from ArcGIS REST API
# All 4 layers saved as:
#   - .rds  (easy to reload in R, full column names preserved)
#   - .shp  (shapefile for ArcGIS)
# Requires: httr, jsonlite, sf, beepr
# ============================================================

# install.packages(c("httr", "jsonlite", "sf", "beepr"))

library(httr)
library(jsonlite)
library(sf)
library(beepr)

BASE_URL <- "https://services.gis.ca.gov/arcgis/rest/services/Environment/Fire_Severity_Zones/MapServer"

LAYERS <- list(
  `0` = "SRA_State_Responsibility_Areas",
  `1` = "LRA_Local_Responsibility_Areas",
  `2` = "SRA_LRA_Awaiting_Zoning",
  `3` = "Federal_Responsibility_Areas"
)

output_dir <- "C:/Users/Nora Schwaller/Dropbox (Personal)/Fire Investment/Data/Derived/FHSZ 2007"
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

rds_dir <- file.path(output_dir, "RDS")
shp_dir <- file.path(output_dir, "Shapefiles")
dir.create(rds_dir, showWarnings = FALSE)
dir.create(shp_dir, showWarnings = FALSE)

# ---- Prepare sf object for shapefile export ----
prep_for_shp <- function(sf_obj, layer_name) {
  # Drop ALL shape metric columns
  drop_pattern <- "^shape"
  keep <- !grepl(drop_pattern, names(sf_obj), ignore.case = TRUE) | names(sf_obj) == "geometry"
  sf_obj <- sf_obj[, keep]
  
  # Rename every non-geometry column to col_01, col_02, etc.
  # This is guaranteed to be unique, short, and GDAL-safe.
  # A CSV sidecar file records the mapping so you know what each column is.
  attr_cols <- names(sf_obj)[names(sf_obj) != "geometry"]
  safe_names <- sprintf("col_%02d", seq_along(attr_cols))
  
  # Save the name mapping as a CSV next to the shapefile
  col_map <- data.frame(
    shp_col   = safe_names,
    orig_col  = attr_cols,
    stringsAsFactors = FALSE
  )
  csv_path <- file.path(shp_dir, paste0(layer_name, "_columns.csv"))
  write.csv(col_map, csv_path, row.names = FALSE)
  cat(sprintf("  Column map saved -> %s\n", csv_path))
  
  # Apply safe names
  names(sf_obj)[names(sf_obj) != "geometry"] <- safe_names
  sf_obj
}

# ---- Download one layer with pagination ----
download_layer <- function(layer_id, layer_name) {
  cat(sprintf("\nDownloading Layer %s: %s ...\n", layer_id, layer_name))
  
  all_features <- list()
  offset <- 0
  batch  <- 1000
  
  repeat {
    url  <- sprintf("%s/%s/query", BASE_URL, layer_id)
    resp <- GET(url, query = list(
      where             = "1=1",
      outFields         = "*",
      f                 = "geojson",
      outSR             = "4326",
      resultOffset      = offset,
      resultRecordCount = batch
    ))
    
    if (http_error(resp)) {
      cat(sprintf("  HTTP error %s — skipping layer.\n", status_code(resp)))
      return(NULL)
    }
    
    data <- fromJSON(rawToChar(resp$content), simplifyVector = FALSE)
    
    if (!is.null(data$error)) {
      cat(sprintf("  API error: %s\n", data$error$message))
      return(NULL)
    }
    
    features     <- data$features
    n            <- length(features)
    all_features <- c(all_features, features)
    cat(sprintf("  Fetched %d features (total so far: %d)\n", n, length(all_features)))
    
    if (n < batch) break
    offset <- offset + batch
  }
  
  if (length(all_features) == 0) {
    cat("  No features found.\n")
    return(NULL)
  }
  
  # Parse into sf object
  geojson_str <- toJSON(
    list(type = "FeatureCollection", features = all_features),
    auto_unbox = TRUE
  )
  sf_obj <- st_read(geojson_str, quiet = TRUE)
  
  # Save .rds with full original column names
  rds_path <- file.path(rds_dir, paste0(layer_name, ".rds"))
  saveRDS(sf_obj, rds_path)
  cat(sprintf("  Saved RDS -> %s\n", rds_path))
  
  # Save .shp with safe column names
  sf_shp   <- prep_for_shp(sf_obj, layer_name)
  shp_path <- file.path(shp_dir, paste0(layer_name, ".shp"))
  st_write(sf_shp, shp_path, delete_layer = TRUE, quiet = TRUE)
  cat(sprintf("  Saved Shapefile -> %s\n", shp_path))
  
  return(sf_obj)
}

# ---- Download all layers, beep on finish or failure ----
tryCatch({
  sf_layers <- mapply(
    download_layer,
    layer_id   = names(LAYERS),
    layer_name = unlist(LAYERS),
    SIMPLIFY   = FALSE
  )
  names(sf_layers) <- unlist(LAYERS)
  
  cat("\n============================================================\n")
  cat("Done! Files saved to:\n")
  cat("  R files:    ", rds_dir, "\n")
  cat("  Shapefiles: ", shp_dir, "\n")
  cat("  (Each shapefile has a _columns.csv mapping col_01 etc. to original names)\n")
  cat("============================================================\n")
  
  beep(sound = 1)  # pleasant ding on success
  
}, error = function(e) {
  cat(sprintf("\nERROR: %s\n", e$message))
  beep(sound = 9)  # Wilhelm scream on failure
})

# ---- To reload an RDS file later in R: ----
# sra <- readRDS("C:/Users/Nora Schwaller/Dropbox (Personal)/Fire Investment/Data/Derived/FHSZ 2007/RDS/SRA_State_Responsibility_Areas.rds")
# plot(sra["HAZ_CLASS"])