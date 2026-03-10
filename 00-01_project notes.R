

# ****************************
# FOLDER STRUCTURE

# Project root:
#   EDQ_disaster_econ_impact/
#
#   ├── EDQ_disaster_econ_impact.Rproj    # RStudio project
#   ├── README.md                         # high‑level overview & conventions
#   ├── Data/
#   │   ├── Source/      # immutable raw data
#   │   └── Derived/     # cleaned or constructed datasets
#   ├── Process/
#   │   ├── Drop List/   # records of dropped obs
#   │   └── Logs/        # script run histories
#   └── Output/
#       ├── Images/      # figures for final paper
#       └── Tables/      # tables for final paper





# ****************************
# NAMING CONVENTIONS

# Use:
#   • two-digit prefix for script category  
#   • two-digit sub-step numbering for ordering  
#   • descriptive name  

# Naming Structure
#    AA-BB_Description.R      # code scripts
#
#    • AA = phase  
#       00 = notes & templates  
#       01 = data cleaning  
#       02 = analysis  
#       etc.(add more if needed)
#   • BB = sub‑step (01, 02, just sequential numbering)  
#   • Description = whatever makes sense 

# Example:
#   00-01_project notes.R
#   00-02_script template.R
#   01-01_import raw data.R
#   01-02_clean demographics.R
#   02-01_explore summary stats.R
#   02-02_fit models.R


