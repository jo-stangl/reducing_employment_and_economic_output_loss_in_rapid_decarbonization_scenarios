############################################
################## Setup ###################
############################################


library(data.table)
library(Matrix)
library(parallel)
library(fastcascade)


# Load data
# Load ESRI data
esri <- readRDS(path_to_dir + "ESRI_ess_ihs_all_nonownership_size_20220629_243339.rds")

# Assign employment weighted ESRI to firm_list
esri_ew_vector <- esri$ESRI[, 7] # 7 is the employment weighted ESRI
firm_list[, esri_ew := esri_ew_vector]

# Assign out-strength weighted ESRI to firm_list
esri_ow_vector <- esri$ESRI[, 1] # 1 is the out_strength weighted ESRI
firm_list[, esri_ow := esri_ow_vector]

# Load defaulting firms data
defaulting_firms <- fread(path_to_dir + "ETS_companies_HUNGARY_emissions_2019.csv",
                          integer64 = 'double',
                          encoding = "UTF-8")


# add attributes of firm_list to defaulting firms
# omitted for data protection reasons


############################################
################ Rankings ##################
############################################

# CO2 emissions rank
setorder(defaulting_firms, -emissions)
defaulting_firms[, co2_rank := .I]

# Employees rank
setorder(defaulting_firms, employees)
defaulting_firms[, employment_rank := .I]

# Out-strength rank
setorder(defaulting_firms, out_strength)
defaulting_firms[, out_strength_rank := .I]

# Employment weighted ESRI rank
setorder(defaulting_firms, esri_ew_19)
defaulting_firms[, esri_ew_rank := .I]

# Out-strength weighted ESRI rank
setorder(defaulting_firms, esri_ow_19)
defaulting_firms[, esri_ow_rank := .I]

# CO2 to Employees ratio rank
setorder(defaulting_firms, -emission_employees_ratio)
defaulting_firms[, emission_employees_rank := .I]

# CO2 to Out-strength ratio rank
setorder(defaulting_firms, -emission_out_strength_ratio)
defaulting_firms[, emission_out_strength_rank := .I]

# CO2 to Employment weighted ESRI ratio rank
setorder(defaulting_firms, -emission_esri_ew_ratio)
defaulting_firms[, emission_esri_ew_rank := .I]

# CO2 to Out-strength weighted ESRI ratio rank
setorder(defaulting_firms, -emission_esri_ow_ratio)
defaulting_firms[, emission_esri_ow_rank := .I]

# CO2 to Employment and Out-strength weighted ESRI ratio rank
setorder(defaulting_firms, -emission_esri_ew_esri_ow_ratio)
defaulting_firms[, emission_esri_ew_esri_ow_rank := .I]

# CO2 to Employment and Out-strength weighted ESRI ratio rank (arithmetic mean)
setorder(defaulting_firms, -emission_esri_ew_esri_ow_ratio_arithmetic)
defaulting_firms[, emission_esri_ew_esri_ow_arithmetic_rank := .I]

# CO2 to Employment and Out-strength weighted ESRI ratio rank (geometric mean)
setorder(defaulting_firms, -emission_esri_ew_esri_ow_ratio_geometric)
defaulting_firms[, emission_esri_ew_esri_ow_geometric_rank := .I]

############################################
################ Scenarios #################
############################################


###################### CO2 ranking ########################

setorder(defaulting_firms, co2_rank)
def_firms <- unique(defaulting_firms$[ID]) # [ID] is a placeholder

psi_mat_co2_rank_I <- sapply(1:length(def_firms), function(x) def_firms[x]==(1:243339))
summary(psi_mat_co2_rank_I[psi_mat_co2_rank_I!=0])
psi_mat <- Matrix(psi_mat_co2_rank_I[,1])

for(i in 2:length(def_firms)){
  psi_mat <- cbind(psi_mat,  rowSums(psi_mat_co2_rank_I[,1:i]) )
}
colSums(psi_mat)
summary(psi_mat[psi_mat!=0])
psi_mat_co2_rank_II <- psi_mat


################# Employees ranking ########################

setorder(defaulting_firms, employment_rank)
def_firms <- unique(defaulting_firms[employees != 0,[ID]]) 

psi_mat_emp_rank_I <- sapply(1:length(def_firms), function(x) def_firms[x]==(1:243339))
summary(psi_mat_emp_rank_I[psi_mat_emp_rank_I!=0])
psi_mat <- Matrix(psi_mat_emp_rank_I[,1])

for(i in 2:length(def_firms)){
  psi_mat <- cbind(psi_mat,  rowSums(psi_mat_emp_rank_I[,1:i]) )
}
colSums(psi_mat)
summary(psi_mat[psi_mat!=0])
psi_mat_emp_rank_II <- psi_mat


################# Out-strength ranking ########################

setorder(defaulting_firms, out_strength_rank)
def_firms <- unique(defaulting_firms$[ID])

psi_mat_out_s_rank_I <- sapply(1:length(def_firms), function(x) def_firms[x]==(1:243339))
summary(psi_mat_out_s_rank_I[psi_mat_out_s_rank_I!=0])
psi_mat <- Matrix(psi_mat_out_s_rank_I[,1])

for(i in 2:length(def_firms)){
  psi_mat <- cbind(psi_mat,  rowSums(psi_mat_out_s_rank_I[,1:i]) )
}
colSums(psi_mat)
summary(psi_mat[psi_mat!=0])
psi_mat_out_s_rank_II <- psi_mat


##################### ESRI-EW ranking ########################

setorder(defaulting_firms, esri_ew_rank)
def_firms <- unique(defaulting_firms$[ID])

psi_mat_esri_ew_rank_I <- sapply(1:length(def_firms), function(x) def_firms[x]==(1:243339))
summary(psi_mat_esri_ew_rank_I[psi_mat_esri_ew_rank_I!=0])
psi_mat <- Matrix(psi_mat_esri_ew_rank_I[,1])

for(i in 2:length(def_firms)){
  psi_mat <- cbind(psi_mat,  rowSums(psi_mat_esri_ew_rank_I[,1:i]) )
}
colSums(psi_mat)
summary(psi_mat[psi_mat!=0])
psi_mat_esri_ew_rank_II <- psi_mat


##################### ESRI-OW ranking ########################

setorder(defaulting_firms, esri_ow_rank)
def_firms <- unique(defaulting_firms$[ID])

psi_mat_esri_ow_rank_I <- sapply(1:length(def_firms), function(x) def_firms[x]==(1:243339))
summary(psi_mat_esri_ow_rank_I[psi_mat_esri_ow_rank_I!=0])
psi_mat <- Matrix(psi_mat_esri_ow_rank_I[,1])

for(i in 2:length(def_firms)){
  psi_mat <- cbind(psi_mat,  rowSums(psi_mat_esri_ow_rank_I[,1:i]) )
}
colSums(psi_mat)
summary(psi_mat[psi_mat!=0])
psi_mat_esri_ow_rank_II <- psi_mat


############# CO2--Employment ratio ranking ####################

setorder(defaulting_firms, emission_employees_rank)
def_firms <- unique(defaulting_firms[employees != 0,[ID]])

psi_mat_emp_ratio_rank_I <- sapply(1:length(def_firms), function(x) def_firms[x]==(1:243339))
summary(psi_mat_emp_ratio_rank_I[psi_mat_emp_ratio_rank_I!=0])
psi_mat <- Matrix(psi_mat_emp_ratio_rank_I[,1])

for(i in 2:length(def_firms)){
  psi_mat <- cbind(psi_mat,  rowSums(psi_mat_emp_ratio_rank_I[,1:i]) )
}
colSums(psi_mat)
summary(psi_mat[psi_mat!=0])
psi_mat_ratio_emp_rank_II <- psi_mat


############# CO2--Out-strength ratio ranking ####################

setorder(defaulting_firms, emission_out_strength_rank)
def_firms <- unique(defaulting_firms$[ID])

psi_mat_out_s_ratio_rank_I <- sapply(1:length(def_firms), function(x) def_firms[x]==(1:243339))
summary(psi_mat_out_s_ratio_rank_I[psi_mat_out_s_ratio_rank_I!=0])
psi_mat <- Matrix(psi_mat_out_s_ratio_rank_I[,1])

for(i in 2:length(def_firms)){
  psi_mat <- cbind(psi_mat,  rowSums(psi_mat_out_s_ratio_rank_I[,1:i]) )
}
colSums(psi_mat)
summary(psi_mat[psi_mat!=0])
psi_mat_ratio_out_s_rank_II <- psi_mat


############# CO2--EW-ESRI ratio ranking ####################

setorder(defaulting_firms, emission_esri_ew_rank)
def_firms <- unique(defaulting_firms$[ID])

psi_mat_esri_ew_ratio_rank_I <- sapply(1:length(def_firms), function(x) def_firms[x]==(1:243339))
summary(psi_mat_esri_ew_ratio_rank_I[psi_mat_esri_ew_ratio_rank_I!=0])
psi_mat <- Matrix(psi_mat_esri_ew_ratio_rank_I[,1])

for(i in 2:length(def_firms)){
  psi_mat <- cbind(psi_mat,  rowSums(psi_mat_esri_ew_ratio_rank_I[,1:i]) )
}
colSums(psi_mat)
summary(psi_mat[psi_mat!=0])
psi_mat_ratio_esri_ew_rank_II <- psi_mat


############# CO2--EW-ESRI ratio ranking ####################

setorder(defaulting_firms, emission_esri_ow_rank)
def_firms <- unique(defaulting_firms$[ID])

psi_mat_esri_ow_ratio_rank_I <- sapply(1:length(def_firms), function(x) def_firms[x]==(1:243339))
summary(psi_mat_esri_ow_ratio_rank_I[psi_mat_esri_ow_ratio_rank_I!=0])
psi_mat <- Matrix(psi_mat_esri_ow_ratio_rank_I[,1])

for(i in 2:length(def_firms)){
  psi_mat <- cbind(psi_mat,  rowSums(psi_mat_esri_ow_ratio_rank_I[,1:i]) )
}
colSums(psi_mat)
summary(psi_mat[psi_mat!=0])
psi_mat_ratio_esri_ow_rank_II <- psi_mat


############# CO2--EW-ESRI--OW-ESRI ratio ranking ####################

setorder(defaulting_firms, emission_esri_ew_esri_ow_rank)
def_firms <- unique(defaulting_firms$[ID])

psi_mat_esri_ew_esri_ow_ratio_rank_I <- sapply(1:length(def_firms), function(x) def_firms[x]==(1:243339))
summary(psi_mat_esri_ew_esri_ow_ratio_rank_I[psi_mat_esri_ew_esri_ow_ratio_rank_I!=0])
psi_mat <- Matrix(psi_mat_esri_ew_esri_ow_ratio_rank_I[,1])

for(i in 2:length(def_firms)){
  psi_mat <- cbind(psi_mat,  rowSums(psi_mat_esri_ew_esri_ow_ratio_rank_I[,1:i]) )
}
colSums(psi_mat)
summary(psi_mat[psi_mat!=0])
psi_mat_ratio_esri_ew_esri_ow_rank_II <- psi_mat


############# CO2--EW-ESRI--OW-ESRI ratio arithmetic ranking ####################

setorder(defaulting_firms, emission_esri_ew_esri_ow_arithmetic_rank)
def_firms <- unique(defaulting_firms$[ID])

psi_mat_esri_ew_esri_ow_ratio_arithmetic_rank_I <- sapply(1:length(def_firms), function(x) def_firms[x]==(1:243339))
summary(psi_mat_esri_ew_esri_ow_ratio_arithmetic_rank_I[psi_mat_esri_ew_esri_ow_ratio_arithmetic_rank_I!=0])
psi_mat <- Matrix(psi_mat_esri_ew_esri_ow_ratio_arithmetic_rank_I[,1])

for(i in 2:length(def_firms)){
  psi_mat <- cbind(psi_mat,  rowSums(psi_mat_esri_ew_esri_ow_ratio_arithmetic_rank_I[,1:i]) )
}
colSums(psi_mat)
summary(psi_mat[psi_mat!=0])
psi_mat_ratio_esri_ew_esri_ow_arithmetic_rank_II <- psi_mat


############# CO2--EW-ESRI--OW-ESRI ratio geometric ranking ####################

setorder(defaulting_firms, emission_esri_ew_esri_ow_geometric_rank)
def_firms <- unique(defaulting_firms$[ID])

psi_mat_esri_ew_esri_ow_ratio_geometric_rank_I <- sapply(1:length(def_firms), function(x) def_firms[x]==(1:243339))
summary(psi_mat_esri_ew_esri_ow_ratio_geometric_rank_I[psi_mat_esri_ew_esri_ow_ratio_geometric_rank_I!=0])
psi_mat <- Matrix(psi_mat_esri_ew_esri_ow_ratio_geometric_rank_I[,1])

for(i in 2:length(def_firms)){
  psi_mat <- cbind(psi_mat,  rowSums(psi_mat_esri_ew_esri_ow_ratio_geometric_rank_I[,1:i]) )
}
colSums(psi_mat)
summary(psi_mat[psi_mat!=0])
psi_mat_ratio_esri_ew_esri_ow_geometric_rank_II <- psi_mat



######################## Psi_mat #########################

psi_mat <- cbind(psi_mat_co2_rank_II,
                 psi_mat_emp_rank_II,
                 psi_mat_out_s_rank_II,
                 psi_mat_esri_ew_rank_II,
                 psi_mat_esri_ow_rank_II,
                 psi_mat_ratio_emp_rank_II,
                 psi_mat_ratio_out_s_rank_II,
                 psi_mat_ratio_esri_ew_rank_II,
                 psi_mat_ratio_esri_ow_rank_II,
                 psi_mat_ratio_esri_ew_esri_ow_rank_II,
                 psi_mat_ratio_esri_ew_esri_ow_arithmetic_rank_II,
                 psi_mat_ratio_esri_ew_esri_ow_geometric_rank_II
                 )

############################################
########## Running the cascade #############
############################################
  
# set directory for saving the results
wd <- setwd(path_to_dir)

# Run cascade model with specified parameters and scenarios
scenario_results <- GL_cascade(W,
                           p,
                           p_market = p_market,
                           p_sec_impacts = p,   
                           ess_mat_sec = ess_mat_sec_n4,
                           h_weights = h_weights,       
                           sec_aggr_weights = FALSE, 
                           psi_mat = psi_mat,
                           revenue = revenue,
                           costs = costs,
                           track_h = FALSE,
                           track_sector_impacts = TRUE, 
                           track_conv = TRUE,
                           conv_type = 1,
                           eps = 10^-2,
                           use_rcpp = TRUE,
                           ncores = 28,
                           run_id = "esri_co2_cascade",
                           load_balance = TRUE,
                           prodfun_sets = FALSE,
                           serv_supplier_sets = FALSE
)
    
saveRDS(scenario_results, paste0("scenario_results", "_size_", length(p), ".rds"))