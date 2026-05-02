

df <- readRDS(paste0(data_output_s, "2-04_analytic.rds"))


options(scipen = 999)

summary(analytic_ins$nonrenew_saleyr)
analytic_ins$nonrenew_saleyr <- analytic_ins$nonrenew_saleyr * 100
summary(analytic_ins$nonrenew_saleyr)

summary(analytic_ins$fair_saleyr)
analytic_ins$fair_saleyr <- analytic_ins$fair_saleyr * 100
summary(analytic_ins$fair_saleyr)



owner_runsample <- analytic_ins %>%
  mutate(
    house_age = scale(house_age),
    tenure = scale(preharv_tenure),
    appr_201610k = scale(appr_201610k),
    bld_ar_2016 = scale(bld_ar_2016),
    yr_built_2016 = scale(yr_built_2016),
    # min_flood_hu10 = scale(min_flood_hu10),
    mfaminc_acs2012_2016 = scale(mfaminc_acs2012_2016),
    med_hval_acs2012_2016 = scale(med_hval_acs2012_2016),
    med_yrblt_acs2012_2016 = scale(med_yrblt_acs2012_2016),
    pc_latino_acs2012_2016 = scale(pc_latino_acs2012_2016),
    pc_white_acs2012_2016 = scale(pc_white_acs2012_2016),
    pc_black_acs2012_2016 = scale(pc_black_acs2012_2016),
    pc_college_acs2012_2016 = scale(pc_college_acs2012_2016),
    pc_husocc_own_acs2012_2016 = scale(pc_husocc_own_acs2012_2016),
    pc_nonmove_1yr_acs2012_2016 = scale(pc_nonmove_1yr_acs2012_2016),
    pc_pov_acs2012_2016 = scale(pc_pov_acs2012_2016),
    prob_hispanic_2016 = scale(prob_hispanic_2016),
    prob_white_2016 = scale(prob_white_2016),
    prob_black_2016 = scale(prob_black_2016),
    pc_cosold = scale(pc_cosold)
  )

output3 <- glm(buy1_corp ~ nonrenew_saleyr + fair_saleyr +
                 log1p(sale_amt) + log1p(building_gross_area_square_feet) +
                 year_built + total_number_of_acres + 
                 median_hh_income + pct_renter + pop_density +
                 factor(sale_yr),
               data = analytic_ins, family = binomial) |> summary()



# baseline
glm(buy1_corp ~ nonrenew_saleyr,
    data = analytic_ins, family = binomial) |> summary()

# baseline
glm(buy1_corp ~ fair_saleyr,
    data = analytic_ins, family = binomial) |> summary()

# baseline
glm(buy1_corp ~ nonrenew_saleyr + fair_saleyr,
    data = analytic_ins, family = binomial) |> summary()



# might prefer
fit <- glm(buy1_corp ~ nonrenew_saleyr + fair_saleyr + ..., 
           data = analytic_ins, family = binomial)

summary(fit)          # log-odds coefficients
exp(coef(fit))        # odds ratios
exp(confint(fit))     # odds ratio confidence intervals




# add property characteristics
output1 <- glm(buy1_corp ~ nonrenew_saleyr + fair_saleyr +
      log1p(sale_amt) + log1p(sqft_living) +
      yr_built + acres ,
    data = analytic_ins, family = binomial)

summary(output1)          # log-odds coefficients
exp(coef(output1))        # odds ratios
#exp(confint(output1))     # odds ratio confidence intervals



# add BG context
output2 <- glm(buy1_corp ~ nonrenew_saleyr + fair_saleyr +
      log1p(sale_amt) + log1p(sqft_living) +
      yr_built + acres +
      median_hh_income + pct_renter + pop_density,
    data = analytic_ins, family = binomial) 

summary(output2)          # log-odds coefficients
exp(coef(output2))        # odds ratios
#exp(confint(output2))     # odds ratio confidence intervals



# add year FE
output3 <- glm(buy1_corp ~ nonrenew_saleyr + fair_saleyr +
      log1p(sale_amt) + log1p(sqft_living) +
        yr_built + acres + 
      median_hh_income + pct_renter + pop_density +
      factor(sale_yr),
    data = analytic_ins, family = binomial)

summary(output3)          # log-odds coefficients
exp(coef(output3))        # odds ratios
#exp(confint(output3))     # odds ratio confidence intervals





# add year FE
output3 <- glm(buy1_corp ~ nonrenew_saleyr + fair_saleyr + wuitype_2020 + wuiflag_change +
                 log1p(sale_amt) + log1p(sqft_living) +
                 yr_built + acres + 
                 median_hh_income + pct_renter + pop_density +
                 factor(sale_yr),
               data = analytic_full, family = binomial) |> summary()

summary(output3)          # log-odds coefficients
exp(coef(output3))        # odds ratios
#exp(confint(output3))     # odds ratio confidence intervals



library(lmtest)
library(sandwich)

# Scale continuous predictors
analytic_model <- analytic_wui %>%
  filter(!is.na(buy1_corp), !is.na(nonrenew_saleyr), !is.na(fair_saleyr),
         !is.na(sale_amt), !is.na(sqft_living), !is.na(yr_built),
         !is.na(acres), !is.na(median_hh_income), !is.na(pct_renter),
         !is.na(pop_density), !is.na(wuitype_2020)) %>%
  mutate(
    nonrenew_sc    = scale(nonrenew_saleyr),
    fair_sc        = scale(fair_saleyr),
    log_sale_amt   = scale(log1p(sale_amt)),
    log_sqft       = scale(log1p(sqft_living)),
    yr_built_sc    = scale(yr_built),
    acres_sc       = scale(acres),
    hh_income_sc   = scale(median_hh_income),
    pct_renter_sc  = scale(pct_renter),
    pop_density_sc = scale(pop_density)
  )

cat(sprintf("  Model N: %s\n", formatC(nrow(analytic_model), format = "d", big.mark = ",")))

# Fit model
output3 <- glm(
  buy1_corp ~ nonrenew_sc + fair_sc + wuitype_2020 +
    log_sale_amt + log_sqft + yr_built_sc + acres_sc +
    median_hh_income + pct_renter_sc + pop_density_sc +
    factor(sale_yr),
  data   = analytic_model,
  family = binomial
)

# Clustered SEs by block group
se_clustered <- coeftest(output3, vcov = vcovCL(output3, cluster = ~geoid))

cat("\n--- Coefficients with block-group clustered SEs ---\n")
print(se_clustered)

cat("\n--- Odds ratios (clustered SEs) ---\n")
or_df <- data.frame(
  term     = rownames(se_clustered),
  OR       = exp(se_clustered[, "Estimate"]),
  CI_low   = exp(se_clustered[, "Estimate"] - 1.96 * se_clustered[, "Std. Error"]),
  CI_high  = exp(se_clustered[, "Estimate"] + 1.96 * se_clustered[, "Std. Error"]),
  p_value  = se_clustered[, "Pr(>|z|)"],
  sig      = case_when(
    se_clustered[, "Pr(>|z|)"] < 0.001 ~ "***",
    se_clustered[, "Pr(>|z|)"] < 0.01  ~ "**",
    se_clustered[, "Pr(>|z|)"] < 0.05  ~ "*",
    TRUE                                ~ ""
  )
) %>%
  mutate(across(c(OR, CI_low, CI_high), ~round(., 3)),
         p_value = round(p_value, 4))

print(or_df)

readr::write_csv(or_df, paste0(img_out, "7-01_output3_logit_clustered.csv"))
cat("  Saved: 7-01_output3_logit_clustered.csv\n")

rm(analytic_model)


analytic_model <- analytic_wui %>%
  filter(!is.na(buy1_corp), !is.na(nonrenew_saleyr), !is.na(fair_saleyr),
         !is.na(sale_amt), !is.na(sqft_living), !is.na(yr_built),
         !is.na(acres), !is.na(median_hh_income), !is.na(pct_renter),
         !is.na(pop_density), !is.na(wuiflag_2020)) %>%
  mutate(
    nonrenew_sc    = scale(nonrenew_saleyr),
    fair_sc        = scale(fair_saleyr),
    log_sale_amt   = scale(log1p(sale_amt)),
    log_sqft       = scale(log1p(sqft_living)),
    yr_built_sc    = scale(yr_built),
    acres_sc       = scale(acres),
    hh_income_sc   = scale(median_hh_income),
    pct_renter_sc  = scale(pct_renter),
    pop_density_sc = scale(pop_density),
    wui_f          = factor(wuiflag_2020, levels = c(0, 1, 2),
                            labels = c("Non-WUI", "Intermix", "Interface"))
  )

cat(sprintf("  Model N: %s\n", formatC(nrow(analytic_model), format = "d", big.mark = ",")))

output4 <- glm(
  buy1_corp ~ nonrenew_sc + fair_sc + wui_f +
    log_sale_amt + log_sqft + yr_built_sc + acres_sc +
    hh_income_sc + pct_renter_sc + pop_density_sc +
    factor(sale_yr),
  data   = analytic_model,
  family = binomial
)

se_clustered <- coeftest(output4, vcov = vcovCL(output4, cluster = ~geoid))

or_df <- data.frame(
  term    = rownames(se_clustered),
  OR      = exp(se_clustered[, "Estimate"]),
  CI_low  = exp(se_clustered[, "Estimate"] - 1.96 * se_clustered[, "Std. Error"]),
  CI_high = exp(se_clustered[, "Estimate"] + 1.96 * se_clustered[, "Std. Error"]),
  p_value = se_clustered[, "Pr(>|z|)"],
  sig     = case_when(
    se_clustered[, "Pr(>|z|)"] < 0.001 ~ "***",
    se_clustered[, "Pr(>|z|)"] < 0.01  ~ "**",
    se_clustered[, "Pr(>|z|)"] < 0.05  ~ "*",
    TRUE                                ~ ""
  )
) %>%
  mutate(across(c(OR, CI_low, CI_high), ~round(., 3)),
         p_value = round(p_value, 4))

print(or_df)

readr::write_csv(or_df, paste0(img_out, "7-01_output4_logit_wuiflag.csv"))
cat("  Saved: 7-01_output4_logit_wuiflag.csv\n")

rm(analytic_model)