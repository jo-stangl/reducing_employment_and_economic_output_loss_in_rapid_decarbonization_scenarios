# Import necessary libraries
import pandas as pd
import psycopg2
import numpy as np  # Add numpy for data manipulation

# Establish connection to the PostgreSQL database
conn = psycopg2.connect(
    user=USER_NAME, 
    host=HOST_NAME, 
    database=DATABASE_NAME, 
    password=PASSWORD,
    port=PORT
)

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Query and process data for 'account' table
query_account = "select * from account"
cursor.execute(query_account)
data_account = list(cursor.fetchall())
df_account = pd.DataFrame(data_account, columns=[desc[0] for desc in cursor.description])

# Query and process data for 'account_holder' table
query_account_holder = "select * from account_holder"
cursor.execute(query_account_holder)
data_account_holder = list(cursor.fetchall())
df_account_holder = pd.DataFrame(data_account_holder, columns=[desc[0] for desc in cursor.description])

# Query and process data for 'installation' table
query_installation = "select * from installation"
cursor.execute(query_installation)
data_installation = list(cursor.fetchall())
df_installation = pd.DataFrame(data_installation, columns=[desc[0] for desc in cursor.description])

# Query and process data for 'compliance' table
query_compliance = "select * from compliance"
cursor.execute(query_compliance)
data_compliance = list(cursor.fetchall())
df_compliance = pd.DataFrame(data_compliance, columns=[desc[0] for desc in cursor.description])

# Query and process data for 'nace_code' table
query_nace = "select * from nace_code"
cursor.execute(query_nace)
data_nace = list(cursor.fetchall())
df_nace = pd.DataFrame(data_nace, columns=[desc[0] for desc in cursor.description])

# Create dictionaries for nace_code data
nace_desc_dict = dict(zip(df_nace["id"], df_nace["description"]))
nace_parent_dict = dict(zip(df_nace["id"], df_nace["parent_id"]))

# Extract relevant columns and filter data for 'installation' table
columns = ['installation_id', 'installation_name', 'nace_id']
df_inst = df_installation[(df_installation.registry_id == "HU")]
df_inst = df_inst.rename(columns={"id": "installation_id", "name": "installation_name"})
df_inst = df_inst[columns]

# Filter data for 'compliance' table
df_comp = df_compliance[(df_compliance.year == 2019) & (df_compliance.installation_id.str.contains("HU"))]

# Process and merge data for 'account' and 'account_holder' tables
df_acc = df_account[["accountHolder_id", "isOpen", "companyRegistrationNumber", "installation_id"]]
df_acc_holder = df_account_holder.rename(columns={"id": "accountHolder_id"})
df_acc_final = pd.merge(df_acc, df_acc_holder, on="accountHolder_id")
df_acc_final = df_acc_final.drop_duplicates()
df_acc_final = df_acc_final[df_acc_final.country_id == "HU"]

# Create dictionaries for company data
companyName_dict = dict(zip(df_acc_final["accountHolder_id"], df_acc_final["name"]))

# Read CSV file for VAT data
df_vat = pd.read_csv("produced_files/companies_Hungary_VAT_FINAL.csv")
name_vat_dict = pd.Series(df_vat.vat_number.values, df_vat.company_name).to_dict()
vat_name_dict = {v: k for k, v in name_vat_dict.items()}

# Merge and process data for Hungarian companies
df_hu = pd.merge(df_inst, df_comp[["installation_id", "verified"]], on="installation_id", how="left")
df_hu = pd.merge(df_hu, df_acc_final[["installation_id", "name", "accountHolder_id"]], on="installation_id", how="inner")
df_hu = df_hu.rename(columns={"verified": "emissions"})
df_hu = df_hu.drop_duplicates()
df_hu = df_hu.drop_duplicates("installation_id")
df_hu = df_hu[df_hu.emissions.notna()]
df_hu["company_name"] = df_hu["accountHolder_id"].map(companyName_dict)

# Update nace_code dictionary with known mappings
nace_dict = pd.Series(df_hu.nace_id.values, df_hu.company_name).to_dict()
nace_unknowns_dict = {"WIZZ AIR HUNGARY LTD": "51.10",
                      "SWISS KRONO Kft.": "16.21",
                      "BC-KC Formalin Kft.": "20.14",
                      "FGSZ Földgázszállító Zrt.": "49.50",
                      "Smartwings Hungary Kft.": "51.10",
                      "PANNONGREEN Megújuló Energia Termelő és Szolgáltató Kft.": "35.11",
                      "HF. Formula Kft.": "35.30",
                      "KALL Ingredients Kft.": "10.62",
                      "VIRESOL KFT.": "10.62",
                      "SK Battery Hungary Kft.": "27.20"}
nace_dict.update(nace_unknowns_dict)

# Group and aggregate data by accountHolder_id
df_hu = df_hu.groupby("accountHolder_id").sum().reset_index()
df_hu["company_name"] = df_hu["accountHolder_id"].map(companyName_dict)

# Group data by company name and add VAT number
df_hu = df_hu.groupby("company_name").sum()
df_hu = df_hu.reset_index()
df_hu["vat_number"] = df_hu.company_name.map(name_vat_dict)

# Group data by VAT number and add company name
df_hu = df_hu.groupby("vat_number").sum().reset_index()
df_hu["company_name"] = df_hu.vat_number.map(vat_name_dict)

# Add nace_id on nace_2 and nace_4 level
df_hu["nace_id"] = df_hu["company_name"].map(nace_dict)

# Process nace_id to get 2-digit representation and descriptions
two_digit = []
for i in range(len(df_hu)):
    try:
        two_digit.append(df_hu.nace_id.iloc[i].split(".")[0])
    except:
        two_digit.append(df_hu.nace_id.iloc[i])
df_hu["nace_id_2_digit"] = pd.Series(two_digit)
df_hu["nace_desc_2_digit"] = df_hu["nace_id_2_digit"].map(nace_desc_dict)

# Map parent nace_ids to get color codes
parent_list = []
for i in df_hu.nace_id_2_digit:
    try:
        parent_list.append(nace_parent_dict[i])
    except:
        parent_list.append(np.nan)
df_hu["parent_id"] = pd.Series(parent_list)
df_hu["parent_id_desc"] = df_hu["parent_id"].map(nace_desc_dict)

# Define color codes for parent_ids
color_dict_parent_ids = {"A":"tab:green",
                         "B":"tab:pink",
                         "C":"tab:cyan",
                         "D":"tab:orange",
                         "H":"tab:red",
                         np.nan:"tab:gray"}

# Map color codes to parent_ids
df_hu["color_parent_id"] = df_hu["parent_id"].map(color_dict_parent_ids)

# Export processed data to CSV
df_hu.to_csv(path_to_dir + "ETS_companies_HUNGARY_emissions_2019.csv", index=False)
