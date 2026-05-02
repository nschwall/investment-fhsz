
# **************************************************
#                     DETAILS
#
# Purpose:   Generate figures and tables for UAA conference
#            presentation handout on corporate investor activity
#            in WUI and insurance-stressed housing markets in CA.
#
# ---- FIGURES PRODUCED ----
#
#   Context (statewide):
#     Fig 1a: FAIR Plan market share trend, statewide + top WUI counties
#     Fig 1b: Nonrenewal rate trend, statewide + top WUI counties
#
#   Introductory findings:
#     Fig 2:  Wilcoxon test results table (corp vs non-corp, WUI + insurance vars)
#     Fig 3:  Corporate share by WUI flag category, stacked bar
#
#   Maps:
#     Fig 4:  Statewide WUI map (county outlines, WUI block colors)
#     Fig 5:  Sacramento metro zoom (WUI + transactions)
#     Fig 6:  Lake Tahoe / South Lake Tahoe zoom (same)
#
# ---- INPUTS ----
#
#   3-01_analytic.rds     (secure drive) -- analytic_wui, full analytic file
#   3-01_wui_3310.rds     (dropbox derived) -- WUI sf blocks, EPSG:3310
#   2-04_ins_county_wide.rds (secure drive) -- county insurance panel
#
# ---- OUTPUTS ----
#
#   7-01_fig1a_fair_trend.png
#   7-01_fig1b_nonrenew_trend.png
#   7-01_fig2_wilcoxon_table.png
#   7-01_fig3_corp_wui_share.png
#   7-01_fig4_statewide_wui.png
#   7-01_fig5_sacramento.png
#   7-01_fig6_laketahoe.png
#
# Author:    Nora Schwaller
# Assisted:  Claude Sonnet 4.6 (Anthropic), claude.ai, 2026-04-30
# Started:   MM/DD/YYYY
# **************************************************


# *************
# 1. Setup ----

user   <- "C:/Users/Nora Schwaller/Dropbox (Personal)/"
root   <- paste0(user, "Fire Investment/")
secure <- "Y:/Institutional Investment/"

data_derived  <- paste0(root,   "Data/Derived/WUI/")
data_output_s <- paste0(secure, "Data/Derived/Fire Investment/")
img_out       <- paste0(root,   "Process/Images/Presentation/")

dir.create(img_out, showWarnings = FALSE, recursive = TRUE)

start_time <- Sys.time()
set.seed(123)

cat("\n====================================================\n")
cat("  7-01_presentation.R  |  Started:", format(start_time), "\n")
cat("====================================================\n\n")

pkgs <- c("sf", "dplyr", "tidyr", "purrr", "ggplot2", "scales",
          "tidycensus", "stringr", "lubridate", "readr")
invisible(lapply(pkgs, function(p) {
  if (!require(p, character.only = TRUE)) {
    install.packages(p); library(p, character.only = TRUE)
  }
}))
rm(pkgs)

ts <- function(label = "") {
  cat(sprintf("\n[%s]  %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), label))
}

# Presentation theme -- clean, minimal, suitable for print
theme_pres <- function(base_size = 12) {
  theme_minimal(base_size = base_size) +
    theme(
      plot.title      = element_text(face = "bold", size = base_size + 2),
      plot.subtitle   = element_text(color = "grey40", size = base_size - 1),
      legend.position = "bottom",
      panel.grid.minor = element_blank(),
      strip.text      = element_text(face = "bold")
    )
}

# WUI colors -- desaturated, print-safe
wui_colors <- c(
  "0" = "#d9d9d9",   # light gray: non-WUI
  "1" = "#e8c49a",   # soft orange: intermix
  "2" = "#d4938a"    # desaturated red: interface
)

cohens_d_ranks <- function(x, g) {
  r  <- rank(x, na.last = "keep")
  m1 <- mean(r[g == 1], na.rm = TRUE); m0 <- mean(r[g == 0], na.rm = TRUE)
  s1 <- sd(r[g == 1],   na.rm = TRUE); s0 <- sd(r[g == 0],   na.rm = TRUE)
  n1 <- sum(!is.na(r[g == 1]));        n0 <- sum(!is.na(r[g == 0]))
  sp <- sqrt(((n1 - 1) * s1^2 + (n0 - 1) * s0^2) / (n1 + n0 - 2))
  (m1 - m0) / sp
}


# *****************************************************
# 2. Load data ----
# *****************************************************

ts("Loading analytic_wui, WUI sf, and insurance panel...")

analytic_wui <- readRDS(paste0(data_output_s, "3-01_analytic.rds"))
wui          <- readRDS(paste0(data_derived,  "3-01_wui_3310.rds"))
ins_county   <- readRDS(paste0(data_output_s, "2-04_ins_county_panel.rds"))

cat(sprintf("  analytic_wui: %s rows x %d cols\n",
            formatC(nrow(analytic_wui), format = "d", big.mark = ","), ncol(analytic_wui)))
cat(sprintf("  wui:          %s rows\n",
            formatC(nrow(wui), format = "d", big.mark = ",")))
cat(sprintf("  ins_county:   %s rows\n",
            formatC(nrow(ins_county), format = "d", big.mark = ",")))

# Derive sale_yr if not present
if (!"sale_yr" %in% names(analytic_wui)) {
  analytic_wui <- analytic_wui %>%
    mutate(sale_yr = lubridate::year(as.Date(sale_date)))
}


# *****************************************************
# 3. Identify top WUI-gaining counties ----
# *****************************************************
# Counties with most census blocks that gained WUI flag 2010->2020.
# Used to select counties for context graphs.

ts("Identifying top WUI-gaining counties...")

# Pull county FIPS from tidycensus for name lookup
county_fips <- tidycensus::get_acs(
  geography = "county", variables = "B01003_001",
  state = "CA", year = 2020, survey = "acs5", geometry = FALSE
) %>%
  transmute(
    county_fips = GEOID,
    county_name = str_remove(NAME, " County, California$") %>% str_to_title()
  )

LA_FIPS <- "06037"   # hard-exclude LA -- atypical fire season distorts trends

top_wui_counties <- wui %>%
  sf::st_drop_geometry() %>%
  mutate(county_fips = substr(blk20, 1, 5)) %>%
  filter(wuiflag_change == "More", county_fips != LA_FIPS) %>%
  count(county_fips, sort = TRUE) %>%
  left_join(county_fips, by = "county_fips") %>%
  head(3)

cat("\n--- Top 3 counties by WUI flag increase (blocks) ---\n")
print(top_wui_counties)

top_fips  <- top_wui_counties$county_fips
top_names <- top_wui_counties$county_name

# Fixed county set for Fig 1 duplicates: statewide + El Dorado + Sacramento
fixed_fips  <- c("06017", "06067")   # El Dorado, Sacramento
fixed_names <- c("El Dorado", "Sacramento")


# *****************************************************
# 4. Context graphs: FAIR Plan and nonrenewal trends ----
# *****************************************************

ts("Fig 1a/1b/1c/1d: FAIR Plan and nonrenewal trends...")

# Helper: build statewide + specified counties trend df
build_trend_df <- function(county_fips_vec, county_names_vec) {
  statewide <- ins_county %>%
    filter(!is.na(fair_share), !is.na(nonrenew_rate), year >= 2015) %>%
    group_by(year) %>%
    summarise(
      fair_share    = sum(fair_new + fair_renew, na.rm = TRUE) /
        sum(vol_new + vol_renew + fair_new + fair_renew, na.rm = TRUE),
      nonrenew_rate = sum(vol_non, na.rm = TRUE) /
        sum(vol_renew_lag, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(county_name = "Statewide", county_fips = "CA", is_state = TRUE)
  
  county_trends <- ins_county %>%
    filter(county_fips %in% county_fips_vec, year >= 2015) %>%
    left_join(
      tibble(county_fips = county_fips_vec, county_name = county_names_vec),
      by = "county_fips"
    ) %>%
    mutate(is_state = FALSE) %>%
    select(year, county_name, county_fips, fair_share, nonrenew_rate, is_state)
  
  bind_rows(statewide, county_trends) %>%
    mutate(
      label_type = if_else(is_state, "Statewide", county_name),
      line_size  = if_else(is_state, 1.4, 0.9),
      line_alpha = if_else(is_state, 1.0, 0.8)
    )
}

# Helper: build and save Fig 1a/1b pair
save_trend_pair <- function(trend_df, color_vec, suffix) {
  p_fair <- ggplot(trend_df,
                   aes(x = year, y = fair_share * 100,
                       color = label_type, group = label_type,
                       linewidth = I(line_size), alpha = I(line_alpha))) +
    geom_line() +
    geom_point(size = 2) +
    scale_color_manual(values = color_vec, name = NULL) +
    scale_x_continuous(breaks = 2015:2023) +
    scale_y_continuous(labels = function(x) paste0(x, "%")) +
    labs(
      title    = "FAIR Plan market share rising statewide",
      subtitle = "Share of all residential policies held by California FAIR Plan",
      x = NULL, y = "FAIR Plan share (%)"
    ) +
    theme_pres()
  
  fname_fair <- paste0(img_out, "7-01_fig1a_fair_trend", suffix, ".png")
  ggsave(fname_fair, plot = p_fair, width = 8, height = 5, dpi = 200)
  cat(sprintf("  Saved: %s\n", basename(fname_fair)))
  
  p_nonrenew <- ggplot(trend_df,
                       aes(x = year, y = nonrenew_rate * 100,
                           color = label_type, group = label_type,
                           linewidth = I(line_size), alpha = I(line_alpha))) +
    geom_line() +
    geom_point(size = 2) +
    scale_color_manual(values = color_vec, name = NULL) +
    scale_x_continuous(breaks = 2015:2023) +
    scale_y_continuous(labels = function(x) paste0(x, "%")) +
    labs(
      title    = "Nonrenewal rates elevated in high-risk counties",
      subtitle = "Total nonrenewals as share of prior-year renewals (county level)",
      x = NULL, y = "Nonrenewal rate (%)"
    ) +
    theme_pres()
  
  fname_nonrenew <- paste0(img_out, "7-01_fig1b_nonrenew_trend", suffix, ".png")
  ggsave(fname_nonrenew, plot = p_nonrenew, width = 8, height = 5, dpi = 200)
  cat(sprintf("  Saved: %s\n", basename(fname_nonrenew)))
  
  invisible(list(fair = p_fair, nonrenew = p_nonrenew))
}

# --- Version A: top WUI-gaining counties (dynamic, LA excluded) ---
trend_topwui <- build_trend_df(top_fips, top_names)
colors_topwui <- c("Statewide" = "black",
                   setNames(c("#e07b54", "#5b8db8", "#6aaa7a"), top_names))
save_trend_pair(trend_topwui, colors_topwui, suffix = "_topwui")

# --- Version B: El Dorado + Sacramento (fixed) ---
trend_fixed <- build_trend_df(fixed_fips, fixed_names)
colors_fixed <- c("Statewide"  = "black",
                  "El Dorado"  = "#9b59b6",
                  "Sacramento" = "#27ae60")
save_trend_pair(trend_fixed, colors_fixed, suffix = "_eldorado_sac")

rm(trend_topwui, trend_fixed)


# *****************************************************
# 5. Wilcoxon results table -- saved as CSV ----
# *****************************************************

ts("Fig 2 (CSV): Wilcoxon results table...")

test_vars <- c("wuiflag_2020", "vegpc_2019", "nonrenew_saleyr", "fair_saleyr")
var_labels <- c(
  wuiflag_2020    = "WUI flag 2020 (0-2)",
  vegpc_2019      = "Vegetation cover % (2019)",
  nonrenew_saleyr = "Nonrenewal rate (sale year)",
  fair_saleyr     = "FAIR Plan share (sale year)"
)

corp_df <- analytic_wui %>%
  filter(!is.na(buy1_corp)) %>%
  mutate(across(all_of(test_vars), as.numeric))

n_corp    <- sum(corp_df$buy1_corp == 1L)
n_noncorp <- sum(corp_df$buy1_corp == 0L)

cat(sprintf("\n  N corporate: %s  |  N non-corporate: %s\n",
            formatC(n_corp, format = "d", big.mark = ","),
            formatC(n_noncorp, format = "d", big.mark = ",")))

wilcox_results <- purrr::map_dfr(test_vars, function(v) {
  cv <- corp_df[[v]][corp_df$buy1_corp == 1L]
  nv <- corp_df[[v]][corp_df$buy1_corp == 0L]
  wt <- suppressWarnings(wilcox.test(cv, nv, exact = FALSE))
  tibble(
    variable   = var_labels[v],
    n_corp     = sum(!is.na(cv)),
    n_noncorp  = sum(!is.na(nv)),
    mean_corp  = round(mean(cv, na.rm = TRUE), 4),
    mean_non   = round(mean(nv, na.rm = TRUE), 4),
    diff_pct   = round(100 * (mean(cv, na.rm = TRUE) - mean(nv, na.rm = TRUE)) /
                         abs(mean(nv, na.rm = TRUE)), 2),
    cohens_d   = round(cohens_d_ranks(corp_df[[v]], corp_df$buy1_corp), 4),
    p_value    = wt$p.value,
    sig        = case_when(
      wt$p.value < 0.001 ~ "***",
      wt$p.value < 0.01  ~ "**",
      wt$p.value < 0.05  ~ "*",
      TRUE               ~ ""
    )
  )
})

print(wilcox_results)

readr::write_csv(wilcox_results,
                 paste0(img_out, "7-01_fig2_wilcoxon_results.csv"))
cat("  Saved: 7-01_fig2_wilcoxon_results.csv\n")

rm(corp_df, wilcox_results)


# *****************************************************
# 6. Corporate share by WUI zone + year ----
# *****************************************************

ts("Fig 3: Corporate share by WUI flag zone and year...")

p3 <- analytic_wui %>%
  filter(!is.na(buy1_corp), !is.na(wuiflag_2020), sale_yr <= 2025) %>%
  mutate(
    wui_label = factor(wuiflag_2020,
                       levels = c(0, 1, 2),
                       labels = c("Non-WUI (0)", "Intermix (1)", "Interface (2)"))
  ) %>%
  group_by(sale_yr, wui_label) %>%
  summarise(
    pct_corp = mean(buy1_corp == 1) * 100,
    n        = n(),
    .groups  = "drop"
  ) %>%
  ggplot(aes(x = sale_yr, y = pct_corp, color = wui_label, group = wui_label)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2.5) +
  scale_color_manual(
    values = c("Non-WUI (0)" = "#6b9ab8",
               "Intermix (1)" = "#c8854a",
               "Interface (2)" = "#b85c54"),
    name = "WUI designation"
  ) +
  scale_x_continuous(breaks = 2019:2025) +
  scale_y_continuous(labels = function(x) paste0(x, "%"),
                     limits = c(0, NA)) +
  labs(
    title    = "Corporate purchases rising across all WUI zones",
    subtitle = "Non-WUI areas show higher corporate share than WUI-designated zones",
    x = NULL, y = "% corporate purchases"
  ) +
  theme_pres()

ggsave(paste0(img_out, "7-01_fig3_corp_wui_share.png"),
       plot = p3, width = 8, height = 5, dpi = 200)
cat("  Saved: 7-01_fig3_corp_wui_share.png\n")
rm(p3)


# *****************************************************
# 7. Statewide WUI map ----
# *****************************************************

ts("Fig 4: Statewide WUI map...")

# County boundaries from tidycensus
ca_counties <- tidycensus::get_acs(
  geography = "county", variables = "B01003_001",
  state = "CA", year = 2020, survey = "acs5", geometry = TRUE
) %>%
  sf::st_transform(crs = 3310)

# WUI blocks -- drop geometry-heavy objects we don't need
wui_map <- wui %>%
  mutate(wui_flag_f = factor(as.character(wuiflag_2020),
                             levels = c("0", "1", "2")))

p4 <- ggplot() +
  geom_sf(data = wui_map, aes(fill = wui_flag_f),
          color = NA, lwd = 0) +
  geom_sf(data = ca_counties, fill = NA,
          color = "white", linewidth = 0.4) +
  scale_fill_manual(
    values = wui_colors,
    labels = c("0" = "Non-WUI", "1" = "Intermix", "2" = "Interface"),
    name   = "WUI designation (2020)",
    na.value = "grey90"
  ) +
  labs(
    title    = "Wildland-Urban Interface Designation, California (2020)",
    subtitle = "Census block level. County boundaries in white."
  ) +
  theme_void(base_size = 11) +
  theme(
    plot.title    = element_text(face = "bold", size = 13),
    plot.subtitle = element_text(color = "grey40", size = 10),
    legend.position = "bottom"
  )

ggsave(paste0(img_out, "7-01_fig4_statewide_wui.png"),
       plot = p4, width = 8, height = 10, dpi = 200)
cat("  Saved: 7-01_fig4_statewide_wui.png\n")
rm(p4)


# *****************************************************
# 8. Transaction point setup ----
# *****************************************************
# Corporate: red (#cc2222) on all maps
# Non-corporate: black on all maps
# No year gradient.

txn_pts <- analytic_wui %>%
  filter(!is.na(lat), !is.na(lon), !is.na(buy1_corp)) %>%
  mutate(buyer_type = if_else(buy1_corp == 1, "Corporate", "Non-corporate")) %>%
  select(lat, lon, buy1_corp, buyer_type)

# Flat dot colors for WUI-designation maps (Sacramento, San Diego, Tahoe)
dot_colors_desig <- c("Corporate" = "#1a6fbd", "Non-corporate" = "black")

# Flat dot colors for WUI-change maps (county maps)
dot_colors_change <- c("Corporate" = "#cc2222", "Non-corporate" = "black")


# *****************************************************
# 9. Helper: zoom map function ----
# *****************************************************

make_zoom_map <- function(title, subtitle,
                          lat_min, lat_max, lon_min, lon_max,
                          txn_pts, wui_map, ca_counties,
                          output_path,
                          pt_size = 0.8, pt_alpha = 0.6,
                          dpi = 600) {
  
  # Bounding box in WGS84 for clipping transactions
  bbox_wgs84 <- sf::st_bbox(c(xmin = lon_min, ymin = lat_min,
                              xmax = lon_max, ymax = lat_max),
                            crs = sf::st_crs(4326))
  
  # Transactions in bbox
  txn_sf <- txn_pts %>%
    filter(lat >= lat_min, lat <= lat_max,
           lon >= lon_min, lon <= lon_max) %>%
    sf::st_as_sf(coords = c("lon", "lat"), crs = 4326) %>%
    sf::st_transform(crs = 3310)
  
  cat(sprintf("    Transactions in bbox: %s\n",
              formatC(nrow(txn_sf), format = "d", big.mark = ",")))
  
  # Reproject bbox to 3310 for spatial clipping
  bbox_3310 <- sf::st_as_sfc(bbox_wgs84) %>%
    sf::st_transform(crs = 3310) %>%
    sf::st_bbox()
  
  # Clip WUI to bbox
  wui_clip <- sf::st_crop(wui_map, bbox_3310)
  
  # Clip counties to bbox
  county_clip <- sf::st_crop(ca_counties, bbox_3310)
  
  # Plot
  p <- ggplot() +
    # WUI blocks by designation
    geom_sf(data = wui_clip, aes(fill = wui_flag_f),
            color = NA, lwd = 0) +
    scale_fill_manual(
      values = wui_colors,
      labels = c("0" = "Non-WUI", "1" = "Intermix", "2" = "Interface"),
      name   = "WUI (2020)", guide = guide_legend(order = 1)
    ) +
    # County outlines
    geom_sf(data = county_clip, fill = NA,
            color = "white", linewidth = 0.5) +
    # Non-corporate transactions (drawn first, underneath)
    geom_sf(
      data  = txn_sf %>% filter(buy1_corp == 0),
      color = "black",
      size  = pt_size, alpha = pt_alpha, shape = 16
    ) +
    # Corporate transactions (on top)
    geom_sf(
      data  = txn_sf %>% filter(buy1_corp == 1),
      color = "#cc2222",
      size  = pt_size, alpha = pt_alpha, shape = 16
    ) +
    coord_sf(
      xlim = c(bbox_3310["xmin"], bbox_3310["xmax"]),
      ylim = c(bbox_3310["ymin"], bbox_3310["ymax"]),
      expand = FALSE
    ) +
    labs(title = title, subtitle = subtitle) +
    theme_void(base_size = 11) +
    theme(
      plot.title      = element_text(face = "bold", size = 13),
      plot.subtitle   = element_text(color = "grey40", size = 10),
      legend.position = "right",
      legend.text     = element_text(size = 8)
    )
  
  ggsave(output_path, plot = p, width = 13, height = 9, dpi = dpi)
  cat(sprintf("    Saved: %s\n", output_path))
  invisible(p)
}


# *****************************************************
# 10. Sacramento metro map ----
# *****************************************************

ts("Fig 5: Sacramento metro map (two point sizes)...")

for (ps in c(0.01, 0.005)) {
  ver <- if (ps == 0.01) "v3a" else "v3b"
  make_zoom_map(
    title    = "Sacramento Metro: Corporate and Non-Corporate SFR Purchases (2019-2025)",
    subtitle = "WUI underlaid. Red = corporate, Black = non-corporate.",
    lat_min  = 38.1,  lat_max = 38.9,
    lon_min  = -121.9, lon_max = -120.8,
    txn_pts     = txn_pts,
    wui_map     = wui_map,
    ca_counties = ca_counties,
    output_path = paste0(img_out, "7-01_fig5_sacramento_", ver, ".png"),
    pt_size  = ps,
    pt_alpha = 0.6,
    dpi      = 1600
  )
}


# *****************************************************
# 11. Lake Tahoe / South Lake Tahoe map ----
# *****************************************************

ts("Fig 6: Lake Tahoe map...")

make_zoom_map(
  title    = "South Lake Tahoe: Corporate and Non-Corporate SFR Purchases (2019-2025)",
  subtitle = "WUI underlaid. Red = corporate, Black = non-corporate.",
  lat_min  = 38.91, lat_max = 38.97,
  lon_min  = -120.08, lon_max = -119.94,
  txn_pts     = txn_pts,
  wui_map     = wui_map,
  ca_counties = ca_counties,
  output_path = paste0(img_out, "7-01_fig6_laketahoe_v4.png"),
  pt_size  = 0.25,
  pt_alpha = 0.8,
  dpi      = 600
)


# *****************************************************
# 12. San Diego metro map ----
# *****************************************************

ts("Fig 7: San Diego metro map (two point sizes)...")

for (ps in c(0.01, 0.005)) {
  ver <- if (ps == 0.01) "v1a" else "v1b"
  make_zoom_map(
    title    = "San Diego Metro: Corporate and Non-Corporate SFR Purchases (2019-2025)",
    subtitle = "WUI underlaid. Red = corporate, Black = non-corporate.",
    lat_min  = 32.5, lat_max = 33.3,
    lon_min  = -117.6, lon_max = -116.5,
    txn_pts     = txn_pts,
    wui_map     = wui_map,
    ca_counties = ca_counties,
    output_path = paste0(img_out, "7-01_fig7_sandiego_", ver, ".png"),
    pt_size  = ps,
    pt_alpha = 0.6,
    dpi      = 1600
  )
}



# *****************************************************
# 12b. Mutate WUI change category onto wui_map ----
# *****************************************************
# Done once here so both the county maps and any future maps
# can use wui_change_cat directly from wui_map.

wui_map <- wui_map %>%
  mutate(
    wui_change_cat = case_when(
      wuiflag_2010 == 0 & wuiflag_2020 > 0 ~ "gained_wui",        # non-WUI -> WUI (dark blue)
      wuiflag_2010 > 0  & wuiflag_2020 == 0 ~ "lost_wui",          # WUI -> non-WUI (dark green)
      wuiflag_2010 < wuiflag_2020 & wuiflag_2010 > 0 ~ "more_wui", # moved up within WUI (light blue)
      wuiflag_2010 > wuiflag_2020 & wuiflag_2020 > 0 ~ "less_wui", # moved down within WUI (light green)
      TRUE ~ "same"
    ),
    wui_change_cat = factor(wui_change_cat,
                            levels = c("gained_wui", "more_wui",
                                       "same",
                                       "less_wui", "lost_wui"))
  )

wui_change_colors <- c(
  "gained_wui" = "#0a3d8f",   # dark blue:  non-WUI -> WUI
  "more_wui"   = "#74b9ff",   # light blue: moved up within WUI
  "same"       = "#d0d0d0",   # gray:       no change
  "less_wui"   = "#55efc4",   # light green: moved down within WUI
  "lost_wui"   = "#006400"    # dark green:  WUI -> non-WUI
)

wui_change_labels <- c(
  "gained_wui" = "Gained WUI (non→WUI)",
  "more_wui"   = "More WUI (within)",
  "same"       = "No change",
  "less_wui"   = "Less WUI (within)",
  "lost_wui"   = "Lost WUI (WUI→non)"
)


# *****************************************************
# 12c. Helper: WUI-change county map function ----
# *****************************************************

make_wui_change_map <- function(county_fips_code,
                                county_name_label,
                                txn_pts, wui_map, ca_counties,
                                img_out,
                                fig_num,
                                pt_size  = 0.005,
                                pt_alpha = 0.6,
                                dpi = 1600) {
  
  cat(sprintf("\n  County: %s  (FIPS: %s)\n", county_name_label, county_fips_code))
  
  # Derive bbox from county polygon
  county_sf  <- ca_counties %>% filter(GEOID == county_fips_code)
  bbox_3310  <- sf::st_bbox(county_sf)
  bbox_wgs84 <- sf::st_bbox(
    sf::st_transform(sf::st_as_sfc(bbox_3310), crs = 4326)
  )
  
  # Clip transaction points using bbox_wgs84 directly
  txn_sf <- txn_pts %>%
    filter(lat >= bbox_wgs84["ymin"], lat <= bbox_wgs84["ymax"],
           lon >= bbox_wgs84["xmin"], lon <= bbox_wgs84["xmax"]) %>%
    sf::st_as_sf(coords = c("lon", "lat"), crs = 4326) %>%
    sf::st_transform(crs = 3310)
  
  cat(sprintf("    Transactions in bbox: %s\n",
              formatC(nrow(txn_sf), format = "d", big.mark = ",")))
  
  # Clip layers -- make valid and drop empties to avoid NA geometry errors
  wui_clip <- sf::st_crop(wui_map, bbox_3310) %>%
    sf::st_make_valid() %>%
    filter(!sf::st_is_empty(geometry))
  
  county_clip <- sf::st_crop(ca_counties, bbox_3310) %>%
    sf::st_make_valid() %>%
    filter(!sf::st_is_empty(geometry))
  
  county_slug <- tolower(gsub(" ", "_", county_name_label))
  output_path <- paste0(img_out, sprintf("7-01_%s_%s.png", fig_num, county_slug))
  
  p <- ggplot() +
    # WUI blocks colored by change category
    geom_sf(data = wui_clip, aes(fill = wui_change_cat),
            color = NA, lwd = 0) +
    scale_fill_manual(
      values = wui_change_colors,
      labels = wui_change_labels,
      name   = "WUI change 2010–2020",
      na.value = "#ebebeb",
      guide  = guide_legend(order = 1)
    ) +
    # County outlines
    geom_sf(data = county_clip, fill = NA,
            color = "white", linewidth = 0.5) +
    # Non-corporate (black, drawn first)
    geom_sf(
      data  = txn_sf %>% filter(buy1_corp == 0),
      color = "black",
      size  = pt_size, alpha = pt_alpha, shape = 16
    ) +
    # Corporate (red, on top)
    geom_sf(
      data  = txn_sf %>% filter(buy1_corp == 1),
      color = "#cc2222",
      size  = pt_size, alpha = pt_alpha, shape = 16
    ) +
    coord_sf(
      xlim   = c(bbox_3310["xmin"], bbox_3310["xmax"]),
      ylim   = c(bbox_3310["ymin"], bbox_3310["ymax"]),
      expand = FALSE
    ) +
    labs(
      title    = paste0(county_name_label, " County: WUI Change and SFR Purchases (2019-2025)"),
      subtitle = "Block color = WUI flag change 2010–2020. Red = corporate, Black = non-corporate."
    ) +
    theme_void(base_size = 11) +
    theme(
      plot.title      = element_text(face = "bold", size = 13),
      plot.subtitle   = element_text(color = "grey40", size = 10),
      legend.position = "right",
      legend.key.size = unit(0.9, "lines"),
      legend.text     = element_text(size = 7),
      legend.title    = element_text(size = 8, face = "bold")
    )
  
  ggsave(output_path, plot = p, width = 13, height = 9, dpi = dpi)
  cat(sprintf("    Saved: %s\n", basename(output_path)))
  invisible(p)
}


# *****************************************************
# 12d. WUI-change county maps ----
# *****************************************************

ts("Fig 9: WUI-change county maps (San Diego, Riverside, San Bernardino, Placer, Sonoma)...")

wui_change_counties <- tibble(
  fips = c("06073", "06065", "06071", "06061", "06097"),
  name = c("San Diego", "Riverside", "San Bernardino", "Placer", "Sonoma")
)

for (i in seq_len(nrow(wui_change_counties))) {
  fig_num <- sprintf("fig9%s", letters[i])   # fig9a, fig9b, ...
  make_wui_change_map(
    county_fips_code  = wui_change_counties$fips[i],
    county_name_label = wui_change_counties$name[i],
    txn_pts     = txn_pts,
    wui_map     = wui_map,
    ca_counties = ca_counties,
    img_out     = img_out,
    fig_num     = fig_num,
    pt_size     = 0.005,
    pt_alpha    = 0.6,
    dpi         = 1600
  )
}


# *****************************************************
# 13. WUI change: new block groups entering WUI ----
# *****************************************************

ts("Fig 8: WUI change graphs and corporate investment in newly WUI blocks...")

# --- 13a. How many blocks/block groups are newly WUI? ---
# wuiflag_change levels: "Same", "More", "Less"
# flag_changed: logical

wui_change_summary <- analytic_wui %>%
  filter(!is.na(wuiflag_change)) %>%
  distinct(geoid, wuiflag_change, wuiflag_2010, wuiflag_2020) %>%
  group_by(wuiflag_change) %>%
  summarise(n_blockgroups = n(), .groups = "drop") %>%
  mutate(pct = round(100 * n_blockgroups / sum(n_blockgroups), 1))

cat("\n  WUI change summary (block groups represented in analytic file):\n")
print(wui_change_summary)

# Bar chart: block groups by WUI change status
p8a <- ggplot(wui_change_summary,
              aes(x = reorder(wuiflag_change, -n_blockgroups),
                  y = n_blockgroups,
                  fill = wuiflag_change)) +
  geom_col(width = 0.6) +
  geom_text(aes(label = paste0(formatC(n_blockgroups, format = "d", big.mark = ","),
                               "\n(", pct, "%)")),
            vjust = -0.4, size = 3.2) +
  scale_fill_manual(
    values = c("More" = "#d4504a", "Same" = "#8eacc2", "Less" = "#6aaa7a"),
    guide  = "none"
  ) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.15))) +
  labs(
    title    = "Block groups by WUI flag change, 2010 to 2020",
    subtitle = "Among block groups with at least one SFR transaction 2019–2025",
    x = NULL, y = "Number of block groups"
  ) +
  theme_pres()

ggsave(paste0(img_out, "7-01_fig8a_wui_change_bars.png"),
       plot = p8a, width = 7, height = 5, dpi = 200)
cat("  Saved: 7-01_fig8a_wui_change_bars.png\n")

# --- 13b. WUI transition matrix: where are blocks moving? ---
wui_transition <- analytic_wui %>%
  filter(!is.na(wuiflag_2010), !is.na(wuiflag_2020)) %>%
  distinct(geoid, wuiflag_2010, wuiflag_2020) %>%
  mutate(
    flag_2010 = factor(wuiflag_2010, levels = c(0, 1, 2),
                       labels = c("Non-WUI", "Intermix", "Interface")),
    flag_2020 = factor(wuiflag_2020, levels = c(0, 1, 2),
                       labels = c("Non-WUI", "Intermix", "Interface"))
  ) %>%
  count(flag_2010, flag_2020)

p8b <- ggplot(wui_transition,
              aes(x = flag_2010, y = flag_2020, fill = n)) +
  geom_tile(color = "white", linewidth = 0.8) +
  geom_text(aes(label = formatC(n, format = "d", big.mark = ",")),
            size = 3.5, fontface = "bold") +
  scale_fill_gradient(low = "#f0f4f8", high = "#2c5f8a",
                      name = "Block\ngroups") +
  labs(
    title    = "WUI classification transitions, 2010 to 2020",
    subtitle = "Block groups with SFR transactions. Diagonal = no change.",
    x = "WUI flag 2010", y = "WUI flag 2020"
  ) +
  theme_pres() +
  theme(legend.position = "right")

ggsave(paste0(img_out, "7-01_fig8b_wui_transition_matrix.png"),
       plot = p8b, width = 6, height = 5, dpi = 200)
cat("  Saved: 7-01_fig8b_wui_transition_matrix.png\n")

# --- 13c. Corporate share by year, newly WUI vs stable ---
wui_change_corp <- analytic_wui %>%
  filter(!is.na(buy1_corp), !is.na(wuiflag_change), sale_yr <= 2025) %>%
  mutate(
    change_label = case_when(
      wuiflag_change == "More" ~ "Newly WUI (gained flag)",
      wuiflag_change == "Less" ~ "Lost WUI flag",
      TRUE                     ~ "Stable WUI status"
    )
  ) %>%
  group_by(sale_yr, change_label) %>%
  summarise(
    pct_corp = mean(buy1_corp == 1) * 100,
    n        = n(),
    .groups  = "drop"
  )

p8c <- ggplot(wui_change_corp,
              aes(x = sale_yr, y = pct_corp,
                  color = change_label, group = change_label)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2.5) +
  scale_color_manual(
    values = c("Newly WUI (gained flag)" = "#d4504a",
               "Stable WUI status"       = "#8eacc2",
               "Lost WUI flag"           = "#6aaa7a"),
    name = NULL
  ) +
  scale_x_continuous(breaks = 2019:2025) +
  scale_y_continuous(labels = function(x) paste0(x, "%"),
                     limits = c(0, NA)) +
  labs(
    title    = "Corporate purchase share by WUI change status",
    subtitle = "Are corporate buyers concentrating in newly WUI block groups?",
    x = NULL, y = "% corporate purchases"
  ) +
  theme_pres()

ggsave(paste0(img_out, "7-01_fig8c_corp_by_wui_change.png"),
       plot = p8c, width = 8, height = 5, dpi = 200)
cat("  Saved: 7-01_fig8c_corp_by_wui_change.png\n")

# --- 13d. Top counties with most "newly WUI" block groups ---
newly_wui_counties <- analytic_wui %>%
  filter(wuiflag_change == "More") %>%
  distinct(geoid, county, county_fips) %>%
  count(county, county_fips, sort = TRUE) %>%
  head(10) %>%
  mutate(county = reorder(county, n))

p8d <- ggplot(newly_wui_counties,
              aes(x = county, y = n)) +
  geom_col(fill = "#d4504a", width = 0.7) +
  geom_text(aes(label = n), hjust = -0.2, size = 3.2) +
  coord_flip() +
  scale_y_continuous(expand = expansion(mult = c(0, 0.15))) +
  labs(
    title    = "Counties with most block groups gaining WUI flag (2010–2020)",
    subtitle = "Among block groups with SFR transactions 2019–2025",
    x = NULL, y = "Block groups gaining WUI flag"
  ) +
  theme_pres()

ggsave(paste0(img_out, "7-01_fig8d_newly_wui_counties.png"),
       plot = p8d, width = 8, height = 5, dpi = 200)
cat("  Saved: 7-01_fig8d_newly_wui_counties.png\n")

# --- Save newly WUI block group list as CSV for reference ---
newly_wui_bg_list <- analytic_wui %>%
  filter(wuiflag_change == "More") %>%
  distinct(geoid, bg_name, county, county_fips,
           wuiflag_2010, wuiflag_2020, wuiclass_2010, wuiclass_2020,
           vegpc_2019) %>%
  arrange(county, geoid)

readr::write_csv(newly_wui_bg_list,
                 paste0(img_out, "7-01_newly_wui_blockgroups.csv"))
cat(sprintf("  Saved: 7-01_newly_wui_blockgroups.csv  (%s block groups)\n",
            nrow(newly_wui_bg_list)))

rm(wui_change_summary, wui_transition, wui_change_corp,
   newly_wui_counties, newly_wui_bg_list, p8a, p8b, p8c, p8d)


# --- 13e. Top counties losing WUI flag ---
losing_wui_counties <- analytic_wui %>%
  filter(wuiflag_change == "Less") %>%
  distinct(geoid, county, county_fips) %>%
  count(county, county_fips, sort = TRUE) %>%
  head(10) %>%
  mutate(county = reorder(county, n))

p8e <- ggplot(losing_wui_counties,
              aes(x = county, y = n)) +
  geom_col(fill = "#006400", width = 0.7) +
  geom_text(aes(label = n), hjust = -0.2, size = 3.2) +
  coord_flip() +
  scale_y_continuous(expand = expansion(mult = c(0, 0.15))) +
  labs(
    title    = "Counties with most block groups losing WUI flag (2010–2020)",
    subtitle = "Among block groups with SFR transactions 2019–2025",
    x = NULL, y = "Block groups losing WUI flag"
  ) +
  theme_pres()

ggsave(paste0(img_out, "7-01_fig8e_losing_wui_counties.png"),
       plot = p8e, width = 8, height = 5, dpi = 200)
cat("  Saved: 7-01_fig8e_losing_wui_counties.png\n")

rm(losing_wui_counties, p8e)


# *****************************************************
# 14. Close out ----
# *****************************************************

cat("\n\n")
cat(strrep("=", 70), "\n")
cat("  Figures saved to:", img_out, "\n\n")
cat("    7-01_fig1a_fair_trend_topwui.png            -- FAIR share: top WUI-gaining counties (LA excluded)\n")
cat("    7-01_fig1b_nonrenew_trend_topwui.png        -- Nonrenewal: top WUI-gaining counties (LA excluded)\n")
cat("    7-01_fig1a_fair_trend_eldorado_sac.png      -- FAIR share: El Dorado + Sacramento\n")
cat("    7-01_fig1b_nonrenew_trend_eldorado_sac.png  -- Nonrenewal: El Dorado + Sacramento\n")
cat("    7-01_fig2_wilcoxon_results.csv              -- Wilcoxon test results (CSV)\n")
cat("    7-01_fig3_corp_wui_share.png                -- Corporate share by WUI zone over time\n")
cat("    7-01_fig4_statewide_wui.png                 -- Statewide WUI map\n")
cat("    7-01_fig5_sacramento_v3a.png                -- Sacramento metro (pt=0.01, 1600dpi)\n")
cat("    7-01_fig5_sacramento_v3b.png                -- Sacramento metro (pt=0.005, 1600dpi)\n")
cat("    7-01_fig6_laketahoe_v4.png                  -- Lake Tahoe zoom (600dpi)\n")
cat("    7-01_fig7_sandiego_v1a.png                  -- San Diego metro (pt=0.01, 1600dpi)\n")
cat("    7-01_fig7_sandiego_v1b.png                  -- San Diego metro (pt=0.005, 1600dpi)\n")
cat("    7-01_fig9a_san_diego.png                    -- San Diego county WUI-change map (1600dpi)\n")
cat("    7-01_fig9b_riverside.png                    -- Riverside county WUI-change map (1600dpi)\n")
cat("    7-01_fig9c_san_bernardino.png               -- San Bernardino county WUI-change map (1600dpi)\n")
cat("    7-01_fig9d_placer.png                       -- Placer county WUI-change map (1600dpi)\n")
cat("    7-01_fig9e_sonoma.png                       -- Sonoma county WUI-change map (1600dpi)\n")
cat("    7-01_fig8a_wui_change_bars.png              -- Block groups by WUI change status\n")
cat("    7-01_fig8b_wui_transition_matrix.png        -- WUI flag transition matrix 2010-2020\n")
cat("    7-01_fig8c_corp_by_wui_change.png           -- Corporate share: newly WUI vs stable\n")
cat("    7-01_fig8d_newly_wui_counties.png           -- Top counties gaining WUI block groups\n")
cat("    7-01_fig8e_losing_wui_counties.png          -- Top counties losing WUI block groups\n")
cat("    7-01_newly_wui_blockgroups.csv              -- Block group reference list (newly WUI)\n")
cat(sprintf("\n  Top WUI-gaining counties (LA excluded): %s\n",
            paste(top_names, collapse = ", ")))
cat(strrep("=", 70), "\n")

message("Total elapsed: ",
        round(difftime(Sys.time(), start_time, units = "mins"), 2), " minutes")