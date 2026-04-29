

# **************************************************
#                     DETAILS
#
# Purpose:   Load California WUI block-level change shapefile
#            (1990-2020), clean and slim to relevant columns,
#            reproject to CA Albers (EPSG:3310), and join to
#            2-04_analytic.rds and 2-04_prop_wide.rds via
#            point-in-polygon spatial join on property lat/lon.
#
#            Source: Radeloff et al. WUI change dataset
#              CA_wui_block_1990_2020_change_v4
#            Unit: Census block (15-char GEOID in BLK20)
#            Years: 1990, 2000, 2010, 2020 classifications
#
# ---- WUI COLUMNS KEPT ----
#
#   wuiclass_2020  -- WUICLASS_2: full WUI class string, 2020
#   wuiflag_2020   -- WUIFLAG202: WUI flag (0=non-WUI, 1=intermix, 2=interface)
#   vegpc_2019     -- VEG2019PC:  % vegetation cover, 2019
#   wuiclass_2010  -- WUICLASS_1: full WUI class string, 2010
#   wuiflag_2010   -- WUIFLAG201: WUI flag, 2010
#   vegpc_2011     -- VEG2011PC:  % vegetation cover, 2011
#   density_2020   -- extracted from wuiclass_2020 (High/Med/Low/VeryLow/Uninhabited/Water)
#   density_2010   -- extracted from wuiclass_2010
#   density_changed -- density_2020 != density_2010
#   flag_changed    -- wuiflag_2020 != wuiflag_2010
#
# ---- JOIN DESIGN ----
#
#   Property lat/lon (WGS84 EPSG:4326) converted to sf points,
#   reprojected to CA Albers (EPSG:3310) to match WUI polygons,
#   then st_join(st_within) to assign each property to a WUI block.
#   Same approach as 1-05 block group join.
#
# ---- INPUTS ----
#
#   Data/Source/WUI/.../CA_wui_block_1990_2020_change_v4.shp
#   2-04_analytic.rds   (secure drive)
#   2-04_prop_wide.rds  (secure drive)
#
# ---- OUTPUTS ----
#
#   3-01_wui_3310.rds     -- cleaned WUI sf object, EPSG:3310
#   3-01_analytic.rds     -- analytic + WUI vars
#   3-01_prop_wide.rds    -- prop_wide + WUI vars
#
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2026-04-29
# Started:   MM/DD/YYYY
# Updated:   MM/DD/YYYY
# **************************************************


# *************
# 1. Setup ----

user   <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root   <- paste0(user, "Fire Investment/")
secure <- "Y:/Institutional Investment/"

data_wui      <- paste0(root,   "Data/Source/WUI/CA_wui_block_1990_2020_change_v4/")
data_derived  <- paste0(root,   "Data/Derived/WUI/")
data_output_s <- paste0(secure, "Data/Derived/Fire Investment/")

dir.create(data_derived, showWarnings = FALSE, recursive = TRUE)

timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file  <- paste0(secure, "Process/Fire Investment/Logs/3-01_log_", timestamp, ".txt")
sink(log_file, split = TRUE)
on.exit(sink(), add = TRUE)

start_time <- Sys.time()
set.seed(123)

cat("\n====================================================\n")
cat("  3-01_wui.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

pkgs <- c("sf", "dplyr")
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
# 2. Load WUI shapefile ----
# *****************************************************

ts("Loading WUI shapefile...")

wui_raw <- sf::st_read(paste0(data_wui, "CA_wui_block_1990_2020_change_v4.shp"))

cat(sprintf("  Rows: %s\n",   formatC(nrow(wui_raw), format = "d", big.mark = ",")))
cat(sprintf("  Cols: %d\n",   ncol(wui_raw)))
cat(sprintf("  CRS:  EPSG:%s  |  %s\n",
            sf::st_crs(wui_raw)$epsg, sf::st_crs(wui_raw)$input))

cat("\n--- WUI class distribution (2020) ---\n")
print(table(wui_raw$WUICLASS_2, useNA = "ifany"))

cat("\n--- WUI flag distribution (2020) ---\n")
print(table(wui_raw$WUIFLAG202, useNA = "ifany"))


# *****************************************************
# 3. CRS check and reproject to CA Albers ----
# *****************************************************
# WUI file came in as NAD_1983_Albers -- close to EPSG:3310 but not identical.
# Force transform to EPSG:3310 to match 1-05 / 2-04 spatial join standard.

ts("Reprojecting to CA Albers EPSG:3310...")

wui_3310 <- sf::st_transform(wui_raw, crs = 3310)
cat(sprintf("  Reprojected CRS: EPSG:%s\n", sf::st_crs(wui_3310)$epsg))

bb <- sf::st_bbox(wui_3310)
cat(sprintf("  Bbox xmin: %.0f  xmax: %.0f  width: %.0f\n",
            bb["xmin"], bb["xmax"], bb["xmax"] - bb["xmin"]))
cat(sprintf("  Bbox ymin: %.0f  ymax: %.0f\n", bb["ymin"], bb["ymax"]))

if (abs(bb["xmax"] - bb["xmin"]) < 1000)
  stop("Bounding box too small -- CRS transform likely failed.")
cat("  Bounding box confirmed in metres.\n")

rm(wui_raw, bb)


# *****************************************************
# 4. Clean and slim WUI columns ----
# *****************************************************

ts("Cleaning and slimming WUI columns...")

extract_density <- function(x) {
  dplyr::case_when(
    grepl("High_Dens",     x) ~ "High",
    grepl("Med_Dens",      x) ~ "Medium",
    grepl("Very_Low_Dens", x) ~ "Very Low",   # must come before Low_Dens
    grepl("Low_Dens",      x) ~ "Low",
    grepl("Uninhabited",   x) ~ "Uninhabited",
    grepl("Water",         x) ~ "Water",
    TRUE                      ~ NA_character_
  )
}

extract_type <- function(x) {
  dplyr::case_when(
    grepl("Interface", x) ~ "Interface",
    grepl("Intermix",  x) ~ "Intermix",
    grepl("NoVeg",     x) ~ "NoVeg",     # must come before Veg
    grepl("Veg",       x) ~ "Veg",
    grepl("Water",     x) ~ "Water",
    TRUE                  ~ NA_character_
  )
}

wui <- wui_3310 %>%
  mutate(
    blk20           = BLK20,
    hu2020          = HU2020,
    hu2010          = HU2010,
    wuiclass_2020   = WUICLASS_2,
    wuiflag_2020    = WUIFLAG202,
    vegpc_2019      = VEG2019PC,
    wuiclass_2010   = WUICLASS_1,
    wuiflag_2010    = WUIFLAG201,
    vegpc_2011      = VEG2011PC,
    density_2020    = extract_density(WUICLASS_2),
    density_2010    = extract_density(WUICLASS_1),
    wuitype_2020    = extract_type(WUICLASS_2),
    wuitype_2010    = extract_type(WUICLASS_1),
    density_changed = density_2020 != density_2010,
    flag_changed    = wuiflag_2020 != wuiflag_2010
  ) %>%
  select(blk20, hu2020, hu2010,
         wuiclass_2020, wuiflag_2020, vegpc_2019,
         wuiclass_2010, wuiflag_2010, vegpc_2011,
         density_2020, density_2010, wuitype_2020, wuitype_2010,
         density_changed, flag_changed)

cat(sprintf("  WUI slim: %s rows x %d cols\n",
            formatC(nrow(wui), format = "d", big.mark = ","), ncol(wui)))

cat("\n--- Density 2020 distribution ---\n")
print(table(wui$density_2020, useNA = "ifany"))

cat("\n--- Density changed 2010->2020 ---\n")
print(table(wui$density_changed, useNA = "ifany"))

cat("\n--- WUI flag changed 2010->2020 ---\n")
print(table(wui$flag_changed, useNA = "ifany"))

rm(wui_3310)


# transition matrix - how many blocks changed flag
table(wui$wuiflag_2010, wui$wuiflag_2020, dnn = c("flag_2010", "flag_2020"))

# same but HU-weighted
density_order <- c("Uninhabited" = 0, "Very Low" = 1, "Low" = 2, 
                   "Medium" = 3, "High" = 4, "Water" = NA_real_)

wui %>%
  mutate(
    d2010 = density_order[density_2010],
    d2020 = density_order[density_2020],
    density_direction = case_when(
      is.na(d2010) | is.na(d2020) ~ "Unclassified",
      d2020 > d2010               ~ "Increased",
      d2020 < d2010               ~ "Decreased",
      TRUE                        ~ "No change"
    ),
    flag_direction = case_when(
      is.na(wuiflag_2010) | is.na(wuiflag_2020) ~ "Unclassified",
      wuiflag_2020 > wuiflag_2010                ~ "Increased",
      wuiflag_2020 < wuiflag_2010                ~ "Decreased",
      TRUE                                       ~ "No change"
    )
  ) %>%
  {list(
    density = table(.$density_direction),
    flag    = table(.$flag_direction)
  )}

# density transitions
table(wui$density_2010, wui$density_2020)

with(wui, table(density_2020, density_2010, useNA = "always"))



# *****************************************************
# 5. Save cleaned WUI ----
# *****************************************************

ts("Saving cleaned WUI object...")

out_wui <- paste0(data_derived, "3-01_wui_3310.rds")
saveRDS(wui, out_wui)
cat(sprintf("  wui_3310 -> %s  (%s rows x %d cols)\n",
            out_wui,
            formatC(nrow(wui), format = "d", big.mark = ","),
            ncol(wui)))


# # *****************************************************
# # 6. Load analytic and prop_wide ----
# # *****************************************************
# 
# ts("Loading 2-04_analytic.rds and 2-04_prop_wide.rds...")
# 
# analytic  <- readRDS(paste0(data_output_s, "2-04_analytic.rds"))
# prop_wide <- readRDS(paste0(data_output_s, "2-04_prop_wide.rds"))
# 
# cat(sprintf("  analytic:  %s rows x %d cols\n",
#             formatC(nrow(analytic),  format = "d", big.mark = ","), ncol(analytic)))
# cat(sprintf("  prop_wide: %s rows x %d cols\n",
#             formatC(nrow(prop_wide), format = "d", big.mark = ","), ncol(prop_wide)))
# 
# stopifnot("lat" %in% names(analytic),  "lon" %in% names(analytic))
# stopifnot("lat" %in% names(prop_wide), "lon" %in% names(prop_wide))


# *****************************************************
# 7. Helper: convert lat/lon to sf points in EPSG:3310 ----
# *****************************************************
# Property lat/lon are WGS84 (EPSG:4326), confirmed in 1-05.
# Convert to CA Albers to match WUI polygons.

make_points_3310 <- function(df) {
  df_valid <- df[!is.na(df$lat) & !is.na(df$lon), ]
  pts <- sf::st_as_sf(df_valid,
                      coords = c("lon", "lat"),
                      crs    = 4326,
                      remove = FALSE)
  sf::st_transform(pts, crs = 3310)
}

# Slim WUI to columns to attach -- drop geometry will be handled by st_join
wui_cols <- c("blk20", "wuiclass_2020", "wuiflag_2020", "vegpc_2019",
              "wuiclass_2010", "wuiflag_2010", "vegpc_2011",
              "density_2020", "density_2010", "wuitype_2020", "wuitype_2010",
              "density_changed", "flag_changed")
wui_slim <- wui[, c(wui_cols, "geometry")]


# *****************************************************
# 8. Spatial join: analytic ----
# *****************************************************

ts("Spatial join: analytic -> WUI blocks...")

analytic_pts  <- make_points_3310(analytic)
analytic_join <- sf::st_join(analytic_pts, wui_slim,
                             join = sf::st_within, left = TRUE)
analytic_wui  <- sf::st_drop_geometry(analytic_join)

n_total   <- nrow(analytic)
n_valid   <- sum(!is.na(analytic$lat) & !is.na(analytic$lon))
n_matched <- sum(!is.na(analytic_wui$blk20))
n_no_ll   <- n_total - n_valid

cat(sprintf("  Total rows:         %s\n", formatC(n_total,   format = "d", big.mark = ",")))
cat(sprintf("  With lat/lon:       %s\n", formatC(n_valid,   format = "d", big.mark = ",")))
cat(sprintf("  Matched to WUI blk: %s (%.2f%% of valid lat/lon)\n",
            formatC(n_matched, format = "d", big.mark = ","),
            100 * n_matched / max(n_valid, 1)))
cat(sprintf("  Unmatched (no blk): %s\n",
            formatC(n_valid - n_matched, format = "d", big.mark = ",")))

rm(analytic_pts, analytic_join)


# *****************************************************
# 9. Spatial join: prop_wide ----
# *****************************************************

ts("Spatial join: prop_wide -> WUI blocks...")

prop_pts  <- make_points_3310(prop_wide)
prop_join <- sf::st_join(prop_pts, wui_slim,
                         join = sf::st_within, left = TRUE)
prop_wui  <- sf::st_drop_geometry(prop_join)

n_total_p   <- nrow(prop_wide)
n_valid_p   <- sum(!is.na(prop_wide$lat) & !is.na(prop_wide$lon))
n_matched_p <- sum(!is.na(prop_wui$blk20))

cat(sprintf("  Total rows:         %s\n", formatC(n_total_p,   format = "d", big.mark = ",")))
cat(sprintf("  With lat/lon:       %s\n", formatC(n_valid_p,   format = "d", big.mark = ",")))
cat(sprintf("  Matched to WUI blk: %s (%.2f%% of valid lat/lon)\n",
            formatC(n_matched_p, format = "d", big.mark = ","),
            100 * n_matched_p / max(n_valid_p, 1)))
cat(sprintf("  Unmatched (no blk): %s\n",
            formatC(n_valid_p - n_matched_p, format = "d", big.mark = ",")))

rm(prop_pts, prop_join)


# *****************************************************
# 10. Post-join distribution checks ----
# *****************************************************

ts("Post-join distribution checks...")

cat("\n--- WUI class 2020 distribution (analytic) ---\n")
print(table(analytic_wui$wuiclass_2020, useNA = "ifany"))

cat("\n--- Density 2020 distribution (analytic) ---\n")
print(table(analytic_wui$density_2020, useNA = "ifany"))

cat("\n--- WUI flag 2020 distribution (analytic) ---\n")
print(table(analytic_wui$wuiflag_2020, useNA = "ifany"))

cat("\n--- Density changed 2010->2020 (analytic) ---\n")
print(table(analytic_wui$density_changed, useNA = "ifany"))


# *****************************************************
# 11. Wilcoxon + Cohen's d by buy1_corp ----
# *****************************************************
# Compare WUI exposure between corporate and non-corporate buyers.
# vegpc_2019 is continuous; wuiflag_2020 is ordinal (0/1/2).
# Both suitable for Wilcoxon rank-sum.

ts("Wilcoxon + Cohen's d: WUI vars by corporate buyer status...")

test_vars <- c("vegpc_2019", "wuiflag_2020", "vegpc_2011", "wuiflag_2010")

corp_df <- analytic_wui %>%
  filter(!is.na(buy1_corp)) %>%
  mutate(across(all_of(test_vars), as.numeric)) %>%
  select(buy1_corp, all_of(test_vars))

n_corp    <- sum(corp_df$buy1_corp == 1L)
n_noncorp <- sum(corp_df$buy1_corp == 0L)

cat(sprintf("\n  Corporate:     %s txns\n", formatC(n_corp,    format = "d", big.mark = ",")))
cat(sprintf("  Non-corporate: %s txns\n",  formatC(n_noncorp, format = "d", big.mark = ",")))

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
  wt        <- suppressWarnings(wilcox.test(corp_vals, noncorp_vals, exact = FALSE))
  
  stars <- dplyr::case_when(
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

# WUI flag: cross-tab corporate vs non-corporate by flag category
cat("\n--- WUI flag 2020: % corporate by flag category ---\n")
analytic_wui %>%
  filter(!is.na(buy1_corp), !is.na(wuiflag_2020)) %>%
  group_by(wuiflag_2020) %>%
  summarise(
    n          = n(),
    pct_corp   = round(mean(buy1_corp == 1) * 100, 2),
    .groups = "drop"
  ) %>%
  mutate(flag_label = case_when(
    wuiflag_2020 == 0 ~ "0 = Non-WUI",
    wuiflag_2020 == 1 ~ "1 = Intermix",
    wuiflag_2020 == 2 ~ "2 = Interface"
  )) %>%
  print()

# Density: cross-tab corporate vs non-corporate by density category
cat("\n--- Density 2020: % corporate by density category ---\n")
analytic_wui %>%
  filter(!is.na(buy1_corp), !is.na(density_2020)) %>%
  group_by(density_2020) %>%
  summarise(
    n        = n(),
    pct_corp = round(mean(buy1_corp == 1) * 100, 2),
    .groups = "drop"
  ) %>%
  arrange(desc(pct_corp)) %>%
  print()

rm(corp_df, n_corp, n_noncorp)

analytic_wui$sale_yr <- year(as.Date(analytic_wui$sale_date))

analytic_wui %>%
  filter(!is.na(buy1_corp), !is.na(wuiclass_2020)) %>%
  group_by(sale_yr, wuiclass_2020) %>%
  summarise(pct_corp = mean(buy1_corp == 1) * 100, n = n(), .groups = "drop") %>%
  arrange(wuiclass_2020, sale_yr) %>%
  print(n = 100)


analytic_wui %>%
  filter(!is.na(buy1_corp), !is.na(wuiflag_2020)) %>%
  group_by(sale_yr, wuiflag_2020) %>%
  summarise(pct_corp = mean(buy1_corp == 1) * 100, n = n(), .groups = "drop") %>%
  arrange(wuiflag_2020, sale_yr) %>%
  print(n = 100)


analytic_wui %>%
  filter(!is.na(buy1_corp), !is.na(wuiclass_2020), sale_yr <= 2025,
         !wuiclass_2020 %in% c("Uninhabited_NoVeg", "Uninhabited_Veg", "Water")) %>%
  mutate(
    density = case_when(
      grepl("High_Dens",     wuiclass_2020) ~ "High Density",
      grepl("Med_Dens",      wuiclass_2020) ~ "Medium Density",
      grepl("Very_Low_Dens", wuiclass_2020) ~ "Very Low Density",
      grepl("Low_Dens",      wuiclass_2020) ~ "Low Density"
    ),
    wui_type = case_when(
      grepl("Interface", wuiclass_2020) ~ "Interface",
      grepl("Intermix",  wuiclass_2020) ~ "Intermix",
      grepl("NoVeg",     wuiclass_2020) ~ "NoVeg",
      grepl("Veg",       wuiclass_2020) ~ "Veg"
    ),
    density = factor(density, levels = c("High Density", "Medium Density",
                                         "Low Density", "Very Low Density"))
  ) %>%
  group_by(sale_yr, density, wui_type) %>%
  summarise(pct_corp = mean(buy1_corp == 1) * 100, n = n(), .groups = "drop") %>%
  group_by(density, wui_type) %>%
  mutate(indexed = pct_corp / pct_corp[sale_yr == 2019] * 100) %>%
  ungroup() %>%
  ggplot(aes(x = sale_yr, y = indexed, color = wui_type, group = wui_type)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2) +
  geom_hline(yintercept = 100, linetype = "dashed", color = "grey50") +
  facet_wrap(~ density, ncol = 2) +
  scale_x_continuous(breaks = 2019:2025) +
  labs(
    title    = "Corporate buyer share indexed to 2019 by WUI class",
    subtitle = "100 = 2019 baseline. Lines above 100 = grown faster than baseline.",
    x = NULL, y = "Index (2019 = 100)", color = "WUI type"
  ) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "bottom")

# *****************************************************
# 12. Save ----
# *****************************************************

ts("Saving outputs...")

out_analytic  <- paste0(data_output_s, "3-01_analytic.rds")
out_prop_wide <- paste0(data_output_s, "3-01_prop_wide.rds")

saveRDS(analytic_wui, out_analytic)
cat(sprintf("  (A) analytic  -> %s rows x %d cols\n",
            formatC(nrow(analytic_wui), format = "d", big.mark = ","),
            ncol(analytic_wui)))

saveRDS(prop_wui, out_prop_wide)
cat(sprintf("  (B) prop_wide -> %s rows x %d cols\n",
            formatC(nrow(prop_wui), format = "d", big.mark = ","),
            ncol(prop_wui)))


# *****************************************************
# 13. Close out ----
# *****************************************************

cat("\n\n")
cat(strrep("=", 70), "\n")
cat("  WUI variables added:\n")
cat("    blk20           -- census block GEOID (15-char)\n")
cat("    wuiclass_2020   -- full WUI class string, 2020\n")
cat("    wuiflag_2020    -- WUI flag (0=non-WUI, 1=intermix, 2=interface)\n")
cat("    vegpc_2019      -- % vegetation cover, 2019\n")
cat("    wuiclass_2010   -- full WUI class string, 2010\n")
cat("    wuiflag_2010    -- WUI flag, 2010\n")
cat("    vegpc_2011      -- % vegetation cover, 2011\n")
cat("    density_2020    -- density extracted from wuiclass_2020\n")
cat("    density_2010    -- density extracted from wuiclass_2010\n")
cat("    wuitype_2020    -- type extracted from wuiclass_2020\n")
cat("    wuitype_2010    -- type extracted from wuiclass_2010\n")
cat("    density_changed -- density_2020 != density_2010\n")
cat("    flag_changed    -- wuiflag_2020 != wuiflag_2010\n")
cat("\n  Outputs:\n")
cat(sprintf("    3-01_wui_3310.rds  -- cleaned WUI sf, EPSG:3310\n"))
cat(sprintf("    3-01_analytic.rds  -- analytic + WUI vars\n"))
cat(sprintf("    3-01_prop_wide.rds -- prop_wide + WUI vars\n"))
cat(strrep("=", 70), "\n")

message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")

savehistory(paste0(secure, "Process/Fire Investment/Logs/3-01_history_", timestamp, ".txt"))
sink()


