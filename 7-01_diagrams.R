
# **************************************************
#                     DETAILS
#
# Purpose:   Generate presentation figures for WUI paper
#            - FAIR Plan market share trend (statewide + counties)
#            - Corporate purchase share by nonrenewal rate
#            - Corporate purchase share by FAIR Plan share
#            - Corporate purchases rising across all WUI zones
#            - Case study maps: Sacramento metro + Lake Tahoe
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2026-05-01
# Started:   2026-05-01
# Updated:   MM/DD/YYYY
# **************************************************


# *************
# 1. Setup ----
# *************

user    <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root    <- paste0(user, "Fire Investment/")
secure  <- "Y:/Fire Investment/"

data_input   <- paste0(root, "Data/Source/")
data_output  <- paste0(root, "Data/Derived/")
img_out      <- paste0(root, "Process/Figures/")

fema <- paste0(user, "Relocation Capacity/Data/Source/FEMA Footprints/CA_Structures.gdb")

if (!dir.exists(img_out)) dir.create(img_out, recursive = TRUE)

timestamp  <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file   <- paste0(root, "Process/Logs/7-01_log_", timestamp, ".txt")
sink(log_file, split = TRUE)
start_time <- Sys.time()

ts <- function(msg) cat("\n[", format(Sys.time(), "%H:%M:%S"), "]", msg, "\n")

pkgs <- c("dplyr", "tidyr", "ggplot2", "sf", "data.table",
          "scales", "lubridate", "ggnewscale", "beepr")
invisible(lapply(pkgs, function(p) {
  if (!require(p, character.only = TRUE)) { install.packages(p); library(p, character.only = TRUE) }
}))
rm(pkgs)

sf::sf_use_s2(FALSE)


# *************
# 2. Load data ----
# *************

ts("Loading analytic data...")

# Main analytic file (from 1-04 or 3-01 output)
analytic <- readRDS(paste0(data_output, "3-01_analytic.rds"))

# Insurance data (CDI / SB824) — county-year
# Expected columns: county_fips, year, fair_share, nonrenewal_rate
ins <- readRDS(paste0(data_output, "2-01_insurance_county.rds"))

# WUI block data
wui <- readRDS(paste0(data_output, "3-01_wui_blocks.rds"))

# CA counties boundary
ca_counties <- tigris::counties(state = "CA", cb = TRUE, year = 2020, class = "sf") %>%
  sf::st_transform(4326)


# *************
# 3. Prep transaction points for maps ----
# *************

ts("Prepping transaction points...")

# Corporate purchases only, SFR, with valid coordinates
txn_pts <- analytic %>%
  filter(
    corp_buyer == 1,
    prop_type  == "SFR",
    !is.na(longitude), !is.na(latitude),
    sale_year >= 2019, sale_year <= 2025
  ) %>%
  select(longitude, latitude, sale_year) %>%
  sf::st_as_sf(coords = c("longitude", "latitude"), crs = 4326)


# *************
# 4. Map helper function ----
# *************

make_zoom_map <- function(title, subtitle,
                          lat_min, lat_max, lon_min, lon_max,
                          txn_pts, wui_map, ca_counties,
                          fema_layer_path,
                          output_path,
                          pt_size    = 0.08,
                          pt_alpha   = 0.6,
                          fema_sample = 300000,
                          dpi = 1200,
                          width = 13, height = 9) {
  
  bbox <- sf::st_bbox(c(xmin = lon_min, xmax = lon_max,
                        ymin = lat_min, ymax = lat_max),
                      crs = 4326)
  bbox_poly <- sf::st_as_sfc(bbox)
  
  # clip WUI
  wui_clip <- sf::st_crop(wui_map, bbox)
  
  # clip counties
  counties_clip <- sf::st_crop(ca_counties, bbox)
  
  # load + clip building footprints
  ts("  Loading building footprints...")
  fp_raw <- sf::st_read(fema_layer_path, quiet = TRUE) %>%
    sf::st_transform(4326)
  fp_clip <- sf::st_crop(fp_raw, bbox)
  rm(fp_raw); gc()
  
  if (nrow(fp_clip) > fema_sample) {
    fp_clip <- fp_clip[sample(nrow(fp_clip), fema_sample), ]
  }
  
  # clip transaction points
  txn_clip <- sf::st_crop(txn_pts, bbox)
  
  # year → color scale (light to dark green for corporate)
  year_range <- 2019:2025
  corp_colors <- scales::seq_gradient_pal("#c7e9c0", "#006d2c")(
    (year_range - min(year_range)) / (max(year_range) - min(year_range))
  )
  names(corp_colors) <- as.character(year_range)
  
  txn_clip <- txn_clip %>%
    mutate(year_chr = as.character(sale_year))
  
  p <- ggplot() +
    # WUI zones
    geom_sf(data = wui_clip, aes(fill = wui_class), color = NA, alpha = 0.35) +
    scale_fill_manual(
      values = c("Non-WUI"    = "#f7f7f7",
                 "Intermix"   = "#fdae61",
                 "Interface"  = "#d73027"),
      name = "WUI Zone", na.translate = FALSE
    ) +
    ggnewscale::new_scale_fill() +
    # building footprints
    geom_sf(data = fp_clip, fill = "#4d4d4d", color = NA, alpha = 0.6) +
    # corporate purchase dots only
    geom_sf(data = txn_clip,
            aes(color = year_chr),
            size  = pt_size,
            alpha = pt_alpha,
            shape = 16) +
    scale_color_manual(values = corp_colors, name = "Purchase year") +
    # county outlines
    geom_sf(data = counties_clip, fill = NA, color = "black", linewidth = 0.4) +
    coord_sf(xlim = c(lon_min, lon_max), ylim = c(lat_min, lat_max), expand = FALSE) +
    labs(title = title, subtitle = subtitle,
         caption = "Sources: Cotality (2019–2025), USFS WUI (2020), FEMA building footprints, TIGER/Line") +
    theme_void(base_size = 11) +
    theme(
      plot.title    = element_text(face = "bold", size = 13),
      plot.subtitle = element_text(size = 9, color = "gray40"),
      plot.caption  = element_text(size = 7, color = "gray60"),
      legend.position = "right"
    )
  
  ggsave(output_path, plot = p, width = width, height = height, dpi = dpi)
  ts(paste("  Saved:", output_path))
}


# *************
# 5. Fig 1: FAIR Plan market share — statewide + key counties ----
# *************

ts("Fig 1: FAIR Plan market share trend...")

# Highlight counties: Riverside, San Bernardino, San Diego
highlight_counties <- c("Riverside", "San Bernardino", "San Diego")

fair_trend <- ins %>%
  mutate(group = case_when(
    county_name %in% highlight_counties ~ county_name,
    is.na(county_name)                  ~ "Statewide",
    TRUE                                ~ NA_character_
  )) %>%
  filter(!is.na(group))

p1 <- ggplot(fair_trend, aes(x = year, y = fair_share, color = group, linewidth = group)) +
  geom_line() +
  scale_color_manual(
    values = c(
      "Statewide"      = "black",
      "Riverside"      = "#d7301f",
      "San Bernardino" = "#fc8d59",
      "San Diego"      = "#fdcc8a"
    ),
    name = NULL
  ) +
  scale_linewidth_manual(
    values = c("Statewide" = 1.2, "Riverside" = 0.9,
               "San Bernardino" = 0.9, "San Diego" = 0.9),
    guide = "none"
  ) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
  scale_x_continuous(breaks = pretty(fair_trend$year)) +
  labs(
    title   = "FAIR Plan Market Share Rising in High-Risk Counties",
    subtitle = "Share of residential policies covered by FAIR Plan",
    x = NULL, y = "FAIR Plan market share",
    caption = "Source: California Department of Insurance (SB 824)"
  ) +
  theme_minimal(base_size = 12) +
  theme(legend.position = "bottom",
        plot.title = element_text(face = "bold"))

ggsave(paste0(img_out, "7-01_fig1_fairplan_trend.png"),
       plot = p1, width = 9, height = 5.5, dpi = 300)


# *************
# 6. Fig 2: Corporate purchases rising across all WUI zones ----
# *************

ts("Fig 2: Corporate purchases by WUI zone over time...")

corp_by_wui_year <- analytic %>%
  filter(prop_type == "SFR", sale_year >= 2019, sale_year <= 2025) %>%
  group_by(sale_year, wui_class) %>%
  summarise(
    n_corp  = sum(corp_buyer == 1, na.rm = TRUE),
    n_total = n(),
    .groups = "drop"
  ) %>%
  mutate(corp_share = n_corp / n_total)

p2 <- ggplot(corp_by_wui_year,
             aes(x = sale_year, y = n_corp, color = wui_class, group = wui_class)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2) +
  scale_color_manual(
    values = c("Non-WUI"   = "#4393c3",
               "Intermix"  = "#f4a582",
               "Interface" = "#d6604d"),
    name = "WUI Zone"
  ) +
  scale_y_continuous(labels = scales::comma) +
  scale_x_continuous(breaks = 2019:2025) +
  labs(
    title    = "Corporate SFR Purchases Increasing Across All WUI Zones",
    subtitle = "Annual count of corporate purchases by WUI designation, 2019–2025",
    x = NULL, y = "Corporate purchases (n)",
    caption  = "Source: Cotality (2019–2025); USFS WUI (2020)"
  ) +
  theme_minimal(base_size = 12) +
  theme(legend.position = "bottom",
        plot.title = element_text(face = "bold"))

ggsave(paste0(img_out, "7-01_fig2_corp_by_wui_zone.png"),
       plot = p2, width = 9, height = 5.5, dpi = 300)


# *************
# 7. Fig 3: Corporate purchase share by nonrenewal rate (county-year) ----
# *************

ts("Fig 3: Corporate purchase share vs nonrenewal rate...")

corp_county_year <- analytic %>%
  filter(prop_type == "SFR", sale_year >= 2019, sale_year <= 2023) %>%
  group_by(county_fips, sale_year) %>%
  summarise(corp_share = mean(corp_buyer == 1, na.rm = TRUE), .groups = "drop")

scatter_nr <- corp_county_year %>%
  left_join(ins %>% select(county_fips, year, nonrenewal_rate),
            by = c("county_fips", "sale_year" = "year")) %>%
  filter(!is.na(nonrenewal_rate))

p3 <- ggplot(scatter_nr, aes(x = nonrenewal_rate, y = corp_share)) +
  geom_point(alpha = 0.45, size = 1.8, color = "#2166ac") +
  geom_smooth(method = "lm", se = TRUE, color = "#d6604d", linewidth = 1) +
  scale_x_continuous(labels = scales::percent_format(accuracy = 0.1)) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
  labs(
    title    = "Corporate Purchase Share by County Nonrenewal Rate",
    subtitle = "Each point = one county-year (2019–2023)",
    x = "Insurer-initiated nonrenewal rate",
    y = "Corporate share of SFR purchases",
    caption = "Sources: Cotality (2019–2023); CDI (SB 824)"
  ) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold"))

ggsave(paste0(img_out, "7-01_fig3_corp_share_nonrenewal.png"),
       plot = p3, width = 8, height = 5.5, dpi = 300)


# *************
# 8. Fig 4: Corporate purchase share by FAIR Plan share (county-year) ----
# *************

ts("Fig 4: Corporate purchase share vs FAIR Plan share...")

scatter_fair <- corp_county_year %>%
  left_join(ins %>% select(county_fips, year, fair_share),
            by = c("county_fips", "sale_year" = "year")) %>%
  filter(!is.na(fair_share))

p4 <- ggplot(scatter_fair, aes(x = fair_share, y = corp_share)) +
  geom_point(alpha = 0.45, size = 1.8, color = "#2166ac") +
  geom_smooth(method = "lm", se = TRUE, color = "#d6604d", linewidth = 1) +
  scale_x_continuous(labels = scales::percent_format(accuracy = 0.1)) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
  labs(
    title    = "Corporate Purchase Share by County FAIR Plan Market Share",
    subtitle = "Each point = one county-year (2019–2023)",
    x = "FAIR Plan market share",
    y = "Corporate share of SFR purchases",
    caption = "Sources: Cotality (2019–2023); CDI (SB 824)"
  ) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold"))

ggsave(paste0(img_out, "7-01_fig4_corp_share_fairplan.png"),
       plot = p4, width = 8, height = 5.5, dpi = 300)


# *************
# 9. Fig 5: Sacramento metro map ----
# *************

ts("Fig 5: Sacramento metro map...")

make_zoom_map(
  title    = "Sacramento Metro: Corporate SFR Purchases (2019–2025)",
  subtitle = "WUI designation underlaid. Charcoal = building footprints. Green dots = corporate purchases (light→dark: 2019→2025).",
  lat_min  = 38.1,  lat_max = 38.9,
  lon_min  = -121.9, lon_max = -120.8,
  txn_pts     = txn_pts,
  wui_map     = wui,
  ca_counties = ca_counties,
  fema_layer_path = fema,
  output_path = paste0(img_out, "7-01_fig5_sacramento.png"),
  pt_size  = 0.08,
  pt_alpha = 0.6,
  fema_sample = 500000,
  dpi = 1200
)


# *************
# 10. Fig 6: Lake Tahoe map ----
# *************

ts("Fig 6: Lake Tahoe map...")

make_zoom_map(
  title    = "South Lake Tahoe: Corporate SFR Purchases (2019–2025)",
  subtitle = "WUI designation underlaid. Charcoal = building footprints. Green dots = corporate purchases (light→dark: 2019→2025).",
  lat_min  = 38.91, lat_max = 38.97,
  lon_min  = -120.08, lon_max = -119.94,
  txn_pts     = txn_pts,
  wui_map     = wui,
  ca_counties = ca_counties,
  fema_layer_path = fema,
  output_path = paste0(img_out, "7-01_fig6_laketahoe.png"),
  pt_size  = 0.25,
  pt_alpha = 0.8,
  fema_sample = 300000,
  dpi = 1200
)


# *************
# 11. Close out ----
# *************

beepr::beep(3)
message("\nElapsed: ", round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")
savehistory(paste0(root, "Process/Logs/7-01_history_", timestamp, ".txt"))
sink()


# ==============================================================================
# RECORD / SCRATCH — everything below was used in development but is not
# part of the main output. Kept as reference.
# ==============================================================================

# # Indexed FAIR Plan trend (relative to 2019 baseline)
# fair_indexed <- ins %>%
#   filter(!is.na(county_name) | is.na(county_name)) %>%
#   group_by(group) %>%
#   mutate(idx = fair_share / fair_share[year == 2019]) %>%
#   ungroup()
#
# ggplot(fair_indexed, aes(x = year, y = idx, color = group)) +
#   geom_line() + geom_hline(yintercept = 1, linetype = "dashed") +
#   scale_y_continuous(labels = scales::percent_format()) +
#   labs(title = "Indexed FAIR Plan market share (2019 = 100%)")

# # Wilcoxon tests — corporate vs non-corporate by WUI zone
# # (moved to 6-01_descriptive.R)
# wui_zones <- c("Non-WUI", "Intermix", "Interface")
# wilcox_results <- lapply(wui_zones, function(z) {
#   sub <- analytic %>% filter(wui_class == z, prop_type == "SFR")
#   wilcox.test(sale_price ~ corp_buyer, data = sub)
# })

# # Corporate purchase share by WUI zone (prop table)
# prop.table(table(analytic$wui_class, analytic$corp_buyer), margin = 1)

# # Earlier map versions with both corporate + non-corporate dots
# # (dropped — non-corporate dots too dense, obscures WUI underneath)
# # Blue = non-corporate, Green = corporate — removed per 2026-05-01 revision.

# # Sacramento v1–v3 iterations:
# # v1: pt_size = 0.25, dpi = 600
# # v2: pt_size = 0.25, dpi = 1200
# # v3: pt_size = 0.08, dpi = 1200 (current)

# # Lake Tahoe bbox iterations:
# # v1: lat 38.88–38.98, lon -120.1 to -119.9
# # v2: lat 38.89–38.97, lon -120.09 to -119.93
# # v3: lat 38.91–38.97, lon -120.08 to -119.94 (current, tighter on community)
# # v4: same as v3, pt_size bumped to 0.25 for Tahoe density