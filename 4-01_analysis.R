

df <- readRDS(paste0(data_output_s, "2-04_analytic.rds"))



# baseline
glm(buy1_corp ~ nonrenew_saleyr,
    data = df, family = binomial) |> summary()

# baseline
glm(buy1_corp ~ fair_saleyr,
    data = df, family = binomial) |> summary()

# baseline
glm(buy1_corp ~ nonrenew_saleyr + fair_saleyr,
    data = df, family = binomial) |> summary()



# might prefer
fit <- glm(buy1_corp ~ nonrenew_saleyr + fair_saleyr + ..., 
           data = df, family = binomial)

summary(fit)          # log-odds coefficients
exp(coef(fit))        # odds ratios
exp(confint(fit))     # odds ratio confidence intervals




# add property characteristics
glm(buy1_corp ~ nonrenew_saleyr + fair_saleyr +
      log1p(calculated_total_value) + log1p(building_gross_area_square_feet) +
      year_built + total_number_of_acres ,
    data = df, family = binomial) |> summary()

# add BG context
glm(buy1_corp ~ nonrenew_saleyr + fair_saleyr +
      log1p(calculated_total_value) + log1p(building_gross_area_square_feet) +
      year_built + total_number_of_acres +
      median_hh_income + pct_renter + pop_density,
    data = df, family = binomial) |> summary()

# add year FE
glm(buy1_corp ~ nonrenew_saleyr + fair_saleyr +
      log1p(calculated_total_value) + log1p(building_gross_area_square_feet) +
      year_built + total_number_of_acres + 
      median_hh_income + pct_renter + pop_density +
      factor(sale_yr),
    data = df, family = binomial) |> summary()