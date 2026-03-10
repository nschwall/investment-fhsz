


# **************************************************
#                     DETAILS
#
# Purpose:   Briefly state what this script does
# Author:    Your Name
# Started:   MM/DD/YYYY
# Updated:   MM/DD/YYYY
# **************************************************


# *************
# 1. Setup ----


# UPDATE THESE PATHS TO YOUR LOCAL MACHINE
# set user
user <- "C:/Users/YourName/"  
root <- paste0(user, "Institutional Investment/")  

# define folders 
data_input    <- paste0(root, "Data/Source/")
data_output   <- paste0(root, "Data/Derived/")


# UPDATE AA-BB TO MATCH THIS SCRIPT'S NUMBER
# logging
timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M")
log_file <- paste0(root, "Process/Logs/AA-BB_log_", timestamp, ".txt")  # UPDATE AA-BB
sink(log_file, split = TRUE)

start_time <- Sys.time()
set.seed(123) 


#load packages - add packages needed for script, installs if needed
pkgs <- c("dplyr", "tidyverse")  
invisible(lapply(pkgs, function(p) {
  if (!require(p, character.only = TRUE)) {
    install.packages(p)
    library(p, character.only = TRUE)
  }
}))
rm(pkgs)


# *******************
# 2. Load data ----





# ******************************
# X. Save & Close out


# save file
saveRDS(x, file = paste0(data_output, "AA-BB_description.rds"))
save(x, file = paste0(data_output, "AA-BB_description.RData"))

# time spent
message("Elapsed: ", round(difftime(Sys.time(), start_time, units = "mins"), 2),
        " minutes")


# UPDATE AA-BB TO MATCH THIS SCRIPT'S NUMBER
# record session info
savehistory(paste0(root, "Process/Logs/AA-BB_history_", timestamp, ".txt")) 
sink() #closes log
