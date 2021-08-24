# LIBRARIES & SETUP ----

# Time Series ML
library(tidymodels)
library(modeltime)
library(modeltime.ensemble)

# Timing & Parallel Processing
library(tictoc)
library(future)
library(doFuture)

# Core 
library(tidyquant)
library(tidyverse)
library(timetk)

# * Parallel Processing ----

registerDoFuture()
n_cores <- parallel::detectCores()
plan(
  strategy = cluster,
  workers  = parallel::makeCluster(n_cores)
)

# plan(sequential)

# 1.0 DATA ----


# * Tickets Data ----
quota_tickets <- read.csv("01_RDQuota/00_quota_tickets_6_mo.csv") %>% 
  mutate(CreatedDateKey = as.Date(paste0(substr(CreatedDateKey, 1, 4), "-", 
                                 substr(CreatedDateKey, 5, 6), "-", 
                                 substr(CreatedDateKey, 7, 8))))

quota_tickets <- quota_tickets %>%
  mutate(AZ_Region = paste0(TicketType)) %>% 
  filter()
#AZ-Enablement + Region

#SubscriptionGUID


#Select GTS Category
quota_tickets <- quota_tickets %>% 
  rename(GTS_Category = AZ_Region)

# Filter case with less than 12 observations
quota_tickets_by_Type <- quota_tickets %>% 
  group_by(CreatedDateKey,GTS_Category) %>% 
  summarize(n = n()) %>% 
  rename(date = CreatedDateKey) %>% ungroup() %>%
  group_by(GTS_Category) %>% filter(n() >= 12) %>% 
  ungroup()



quota_tickets_by_Type  %>%
  group_by(GTS_Category) %>% 
  plot_time_series(
    date, n,
    .facet_ncol = 4,
    .smooth = FALSE,
    .interactive = FALSE
  )

# * Full Data Quota ----

Full_Quota_tbl <- quota_tickets_by_Type %>%
  
  select(date, GTS_Category, n) %>%
  group_by(GTS_Category) %>%
  pad_by_time(date, .by = "day", .pad_value = 0)%>%
  ungroup() %>%

  #Global Features / Transformations / Joins
  
  mutate(n = log1p(n)) %>%
  
  # Group-Wise Feature Transformations  
  group_by(GTS_Category) %>%
  future_frame(date, .length_out = 28, .bind_data = TRUE ) %>%
  ungroup() %>%
  
  #Lags & Rolling Features / Fourier
  mutate(GTS_Category = as_factor(GTS_Category)) %>% 
  group_by(GTS_Category) %>%
  group_split() %>%
  map(.f = function(df){
    df %>% 
      arrange(date) %>%
      tk_augment_fourier(date, .periods = c(14, 28)) %>%
      tk_augment_lags(n, .lags = 28) %>%
      tk_augment_slidify(
        n_lag28, 
        .f = ~mean(.x, na.rm = TRUE),
        .period = c(7, 28, 28*2),
        .partial = TRUE,
        .align = "center"
      )
  }) %>% 
  bind_rows() %>%
  
  rowid_to_column(var = "rowid")


# * Data Quota Prepared ----

Quota_prepared_tbl <- Full_Quota_tbl  %>%
  filter(!is.na(n)) %>% 
  drop_na()

Quota_prepared_tbl

# * Future  Quota Data ----
quota_future_tbl <- Full_Quota_tbl %>%
  filter(is.na(n)) 

quota_future_tbl %>% filter(is.nan(n_lag28_roll_28))

quota_future_tbl <- quota_future_tbl %>%
  mutate(across(.cols = contains("_lag"), 
                .fns = ~ ifelse(is.nan(.x), NA, .x))) %>%
  fill(contains("_lag"), .direction = "up")

quota_future_tbl %>% filter(is.na(n_lag28_roll_28))


# 2.0 TIME SPLIT Quota ----

quota_splits <- Quota_prepared_tbl %>%
  time_series_split(date, assess = 28, cumulative = TRUE)

quota_splits %>%
  tk_time_series_cv_plan() %>% 
  plot_time_series_cv_plan(date, n)


# 3.0 Quota RECIPE ----

# * Clean Training Set ----
# - With Panel Data, need to do this outside of a recipe
# - Transformation happens by group 

quota_train_cleaned <- training(quota_splits) %>% 
  group_by(GTS_Category) %>%
  mutate(n= ts_clean_vec(n, period = 7)) %>%
  ungroup()

#training(splits) %>%
quota_train_cleaned %>%
  group_by(GTS_Category) %>%
  plot_time_series(
    date, n,
    .facet_ncol = 4,
    .interactive = TRUE
  )

# * Recipe Specification ----

quota_recipe_spec <- recipe(n ~ ., data = quota_train_cleaned) %>%
  update_role(rowid, new_role = "indicator") %>%
  step_timeseries_signature(date) %>%
  step_rm(matches("(.xts$)|(.iso$)|(hour)|(minute)|(second)|(am.pm)")) %>%
  step_normalize(date_index.num, date_year) %>%
  step_other(GTS_Category) %>%
  step_dummy(all_nominal(), one_hot = TRUE)

quota_recipe_spec %>% prep() %>% juice() %>% glimpse()



train_cleaned <- quota_train_cleaned
recipe_spec <- quota_recipe_spec
splits <- quota_splits


recipe_spec %>% prep() %>% juice() %>% glimpse()

# 4.0 MODELS ----

# * PROPHET ----

wflw_fit_prophet <- workflow() %>%
  add_model(
    spec = prophet_reg() %>% set_engine("prophet")
  ) %>%
  add_recipe(recipe_spec) %>%
  fit(train_cleaned)


# * XGBOOST ----

wflw_fit_xgboost <- workflow() %>%
  add_model(
    spec = boost_tree(mode = "regression") %>% set_engine("xgboost")
  ) %>%
  add_recipe(recipe_spec %>% update_role(date, new_role = "indicator")) %>%
  fit(train_cleaned)


# * PROPHET BOOST ----

wflw_fit_prophet_boost <- workflow() %>%
  add_model(
    spec = prophet_boost(
      seasonality_daily  = FALSE, 
      seasonality_weekly = FALSE, 
      seasonality_yearly = FALSE
    ) %>% 
      set_engine("prophet_xgboost")
  ) %>%
  add_recipe(recipe_spec) %>%
  fit(train_cleaned)



# * SVM ----

wflw_fit_svm <- workflow() %>%
  add_model(
    spec = svm_rbf(mode = "regression") %>% set_engine("kernlab")
  ) %>%
  add_recipe(recipe_spec %>% update_role(date, new_role = "indicator")) %>%
  fit(train_cleaned)



# * RANDOM FOREST ----

wflw_fit_rf <- workflow() %>%
  add_model(
    spec = rand_forest(mode = "regression") %>% set_engine("ranger")
  ) %>%
  add_recipe(recipe_spec %>% update_role(date, new_role = "indicator")) %>%
  fit(train_cleaned)

# * NNET ----

wflw_fit_nnet <- workflow() %>%
  add_model(
    spec = mlp(mode = "regression") %>% set_engine("nnet")
  ) %>%
  add_recipe(recipe_spec %>% update_role(date, new_role = "indicator")) %>%
  fit(train_cleaned)

# * MARS ----

wflw_fit_mars <- workflow() %>%
  add_model(
    spec = mars(mode = "regression") %>% set_engine("earth")
  ) %>%
  add_recipe(recipe_spec %>% update_role(date, new_role = "indicator")) %>%
  fit(train_cleaned)


# * ACCURACY CHECK ----

submodels_1_tbl <- modeltime_table(
  wflw_fit_prophet,
  wflw_fit_xgboost,
  wflw_fit_prophet_boost,
  wflw_fit_svm,
  wflw_fit_rf,
  wflw_fit_nnet,
  wflw_fit_mars
)

submodels_1_tbl %>%
  modeltime_accuracy(testing(splits)) %>%
  arrange(rmse)


# 5.0 HYPER PARAMETER TUNING ---- 

set.seed(123)
resamples_kfold <- train_cleaned %>%
  vfold_cv(v = 5)

resamples_kfold %>%
    tk_time_series_cv_plan() %>%
    plot_time_series_cv_plan(date, n, .facet_ncol = 2)


# XGBOOST TUNE ___

model_spec_xgboost_tune <- boost_tree(
  mode = "regression",
  mtry = tune(),
  trees = tune(),
  min_n = tune(),
  tree_depth = tune(),
  learn_rate = tune(),
  loss_reduction = tune()
) %>%
  set_engine("xgboost")

wflw_spec_xgboost_tune <- workflow() %>%
  add_model(model_spec_xgboost_tune) %>%
  add_recipe(recipe_spec %>% update_role(date, new_role = "indicator"))

# Tuning

tic()
set.seed(123)
tune_results_xgboost <- wflw_spec_xgboost_tune %>%
  tune_grid(
    resamples = resamples_kfold,
    param_info = parameters(wflw_spec_xgboost_tune) %>%
      update(
        learn_rate = learn_rate(c(0.001, 0.400), trans = NULL)
        ),
      grid = 10,
      control = control_grid(verbose = TRUE, allow_par = TRUE)
  )
toc()


## xgboost tune

tune_results_xgboost %>% show_best("rmse", n = Inf)

# ** finalize

wflw_fit_xgboost_tuned <- wflw_spec_xgboost_tune %>%
  finalize_workflow(select_best(tune_results_xgboost, "rmse")) %>%
  fit(train_cleaned)

wflw_fit_xgboost_tuned

##Ranger Tune

model_spec_rf_tune <- rand_forest(
  mode = "regression",
  mtry = tune(),
  trees = tune(),
  min_n = tune()
) %>%
  set_engine("ranger")

wflw_spec_rf_tune <- workflow() %>%
  add_model(model_spec_rf_tune) %>%
  add_recipe(recipe_spec %>% update_role(date, new_role = "indicator"))

# ** Tuning

tic()
set.seed(123)
tune_results_rf <- wflw_spec_rf_tune %>%
  tune_grid(
    resamples = resamples_kfold,
    grid = 5,
    control = control_grid(verbose = TRUE, allow_par = TRUE)
  )
toc()

# Results
tune_results_rf %>% show_best("rmse", n = Inf)

#Finalize

wflw_fit_rf_tuned <- wflw_spec_rf_tune %>%
  finalize_workflow(select_best(tune_results_rf, "rmse")) %>%
  fit(train_cleaned)

wflw_fit_rf_tuned

# * Turn OFF Parallel Backend
plan(sequential)

#Earth Tune:

model_spec_earth_tune <- mars(
  mode = "regression", 
  num_terms = tune(),
  prod_degree = tune()
) %>%
  set_engine("earth")

wflw_spec_earth_tune <- workflow() %>%
  add_model(model_spec_earth_tune) %>%
  add_recipe(recipe_spec %>% 
               update_role(date, new_role = "indicator"))


# ** Tuning
tic()
set.seed(123)
tune_results_earth <- wflw_spec_earth_tune %>%
  tune_grid(
    resamples = resamples_kfold,
    grid = 10,
    control = control_grid(allow_par = TRUE, verbose = TRUE)
  )
toc()

tune_results_earth %>%
  show_best("rmse")

#Finalize
wflw_fit_earth_tuned <- wflw_spec_earth_tune %>%
  finalize_workflow(tune_results_earth %>% select_best("rmse")) %>%
  fit(train_cleaned)



#Tuned Models evaluation, 6.0
submodels_2_tbl <- modeltime_table(
  wflw_fit_xgboost_tuned,
  wflw_fit_rf_tuned,
  wflw_fit_earth_tuned
) %>% 
  update_model_description(1, "XGBOOST - Tuned") %>%
  update_model_description(2, "RANGER - Tuned") %>%
  update_model_description(3, "EARTH - Tuned") %>%
  combine_modeltime_tables(submodels_1_tbl)

#Calibration
calibration_tbl <- submodels_2_tbl %>%
  modeltime_calibrate(testing(splits))

#Accuracy
calibration_tbl %>%
  modeltime_accuracy() %>%
  arrange(rmse)

#Forecast Test -----


calibration_tbl %>%
  modeltime_forecast(
    new_data = testing(splits),
    actual_data = Quota_prepared_tbl,
    keep_data = TRUE
  ) %>%
  group_by(GTS_Category) %>%
  plot_modeltime_forecast(
    .facet_ncol = 4,
    .conf_interval_show = FALSE,
    .interactive = TRUE
  )


# 7.0 Resampling 

resamples_tcsv <- quota_train_cleaned %>%
  time_series_cv(
    assess =     28, 
    skip =       28, 
    cumulative = TRUE,
    slice_limit = 4
  )

resamples_tcsv %>%
  tk_time_series_cv_plan() %>%
  plot_time_series_cv_plan(date, n)

# * Fitting Resamples -------------------
model_tbl_tuned_resamples <- submodels_2_tbl %>%
  modeltime_fit_resamples(
    resamples = resamples_tcsv,
    control = control_resamples(verbose = TRUE, allow_par = TRUE)
  )

#Resampling Accuracy Table

model_tbl_tuned_resamples %>%
  modeltime_resample_accuracy(
    metric_set = metric_set(rmse), 
    summary_fns = list(mean = mean, sd = sd)
  ) %>%
  arrange(rmse_mean)

model_tbl_tuned_resamples %>%
  plot_modeltime_resamples(
    .metric_set = metric_set(mae, rmse, rsq),
    .point_size = 4,
    .point_alpha = 0.8, 
    .facet_ncol = 1
  )
  
# 8.0 Ensemble Panel Models

# * Average Ensemble------

submodels_2_ids_to_keep <- c(5,1,6)

ensemble_fit <- submodels_2_tbl %>% 
  filter(.model_id %in% submodels_2_ids_to_keep) %>%
  ensemble_average(type = "median")

model_ensemble_tbl <- modeltime_table(
  ensemble_fit
)

# * Accuracy

model_ensemble_tbl %>%
  modeltime_accuracy(testing(splits))


#* Forecast-----------------

forecast_ensemble_test_tbl <- model_ensemble_tbl %>% 
  modeltime_forecast(
    new_data = testing(splits),
    actual_data = Quota_prepared_tbl,
    keep_data = TRUE
  ) %>%
  mutate(
    across(.cols = c(.value, n), .fns = expm1)
  )

forecast_ensemble_test_tbl %>%
  group_by(GTS_Category) %>%
  plot_modeltime_forecast(
    .facet_ncol = 4
  )

forecast_ensemble_test_tbl %>%
  filter(.key == "prediction") %>%
  select(GTS_Category, .value, n) %>%
  group_by(GTS_Category) %>%
  summarize_accuracy_metrics(
    truth = n,
    estimate = .value,
    metric_set = metric_set(mae, rmse, rsq)
  )


# * Refit -------
data_prepared_tbl_cleaned <- Quota_prepared_tbl %>%
  group_by(GTS_Category) %>%
  mutate(n = ts_clean_vec(n, period = 7)) %>%
  ungroup()

model_ensemble_refit_tbl <- model_ensemble_tbl %>%
  modeltime_refit(data_prepared_tbl_cleaned)



output <- model_ensemble_refit_tbl %>%
  modeltime_forecast(
    new_data = quota_future_tbl,
    actual_data = data_prepared_tbl_cleaned ,
    keep_data = TRUE
  ) %>%
  mutate(
    .value = expm1(.value),
    n = expm1(n)
  ) %>%
  group_by(GTS_Category)

output %>%
  plot_modeltime_forecast(
    .facet_ncol = 4,
    .y_intercept = 0
  )


write.csv(output, "./time_series_QuotaType.csv")