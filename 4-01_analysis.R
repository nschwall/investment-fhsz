

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




