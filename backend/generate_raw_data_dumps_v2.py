import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# ==========================================
# CONFIGURATION
# ==========================================
INPUT_FHTC_STATE = "FunctionalHouseholdTapConnection_FHTCDataTable.csv"
INPUT_FHTC_DISTRICT = "households_with_tap_water_connection_in_districts.csv"
INPUT_HGJ_VILLAGE = "HGJ_DataTable.csv"

# Output files
OUTPUT_IMIS_TAP_STATUS = "raw_imis_tap_water_status.csv"       # State Level
OUTPUT_IMIS_SCHEME_MASTER = "raw_imis_scheme_master.csv"       # Scheme Level (NEW)
OUTPUT_ZP_SCHEME_PROGRESS = "raw_zp_scheme_progress.csv"       # Scheme Level
OUTPUT_MJP_FINANCIAL = "raw_mjp_financial_report.csv"          # Financials
OUTPUT_GSDA_WQMIS = "raw_gsda_water_quality.csv"               # District Level (UPDATED)
OUTPUT_PGRS_GRIEVANCE = "raw_pgrs_grievance.csv"               # Grievances

def random_date(start_year=2024, fmt="%Y-%m-%d"):
    start = datetime(start_year, 1, 1)
    end = datetime(start_year, 12, 31)
    delta = end - start
    random_days = random.randrange(delta.days)
    return (start + timedelta(days=random_days)).strftime(fmt)

def generate_messy_data():
    print("--- STARTING RAW DATA GENERATION (V2) ---")
    
    try:
        df_state = pd.read_csv(INPUT_FHTC_STATE)
        df_district = pd.read_csv(INPUT_FHTC_DISTRICT)
        print("✅ Input files loaded successfully.")
    except Exception as e:
        print(f"❌ Error loading inputs: {e}")
        return

    # Common list of districts for linking
    common_districts = df_district['District Name'].head(50).tolist()

    # ==============================================================================
    # 1. GENERATE IMIS TAP WATER STATUS (State Level)
    # Challenge 4: "A & N Islands" naming
    # ==============================================================================
    print("Generating IMIS Tap Water Status...")
    df_imis_state = df_state.copy()
    df_imis_state['State Name'] = df_imis_state['State Name'].str.strip()
    df_imis_state['Report_Date'] = [random_date(fmt="%Y-%m-%d") for _ in range(len(df_imis_state))] # ISO Format
    df_imis_state.to_csv(OUTPUT_IMIS_TAP_STATUS, index=False)
    print(f"   -> Generated {OUTPUT_IMIS_TAP_STATUS}")

    # ==============================================================================
    # 2. GENERATE IMIS SCHEME MASTER (Central Truth)
    # This file provides the "100%" status that conflicts with ZP's "0%"
    # ==============================================================================
    print("Generating IMIS Scheme Master...")
    imis_schemes = []
    
    # Generate matching records for ZP, plus the conflicting one
    for dist in common_districts:
        # Normal Record
        imis_schemes.append({
            "IMIS_ID": f"SCH-{random.randint(10000000, 99999999)}",
            "District": dist,
            "Scheme_Name": f"PWS {dist} Phase I",
            "Status": "Ongoing",
            "Completion_Date": ""
        })
    
    # --- INJECT COUNTERPART FOR CHALLENGE 2 ---
    imis_schemes.append({
        "IMIS_ID": "20118869", # Matches ZP ID
        "District": "Thane",
        "Scheme_Name": "Retrofitted PWS Thane",
        "Status": "Completed", # Conflict! ZP says Pending
        "Completion_Date": "2025-01-15"
    })

    df_imis_scheme = pd.DataFrame(imis_schemes)
    df_imis_scheme.to_csv(OUTPUT_IMIS_SCHEME_MASTER, index=False)
    print(f"   -> Generated {OUTPUT_IMIS_SCHEME_MASTER} (Contains 'Completed' status for 20118869)")

    # ==============================================================================
    # 3. GENERATE ZP SCHEME PROGRESS (Local Data)
    # Challenge 1: Physical 0% vs Financial > 0%
    # Challenge 2: Sync Conflict (ID 20118869 is Pending here)
    # Date Issue: Uses DD/MM/YYYY format (Different from IMIS)
    # ==============================================================================
    print("Generating ZP Scheme Progress...")
    zp_data = []
    
    for i, dist in enumerate(common_districts):
        scheme_id = f"SCH-{random.randint(10000000, 99999999)}"
        phy = random.randint(10, 100)
        fin = random.randint(10, 100)
        
        # Challenge 1 Injection
        if i % 10 == 0:
            phy = 0
            fin = 45 # Money spent, no work shown
            
        zp_data.append({
            "Scheme_ID": scheme_id,
            "District": dist,
            "Physical_Progress": phy,
            "Financial_Progress": fin,
            "Last_Updated": random_date(fmt="%d/%m/%Y") # UK Format
        })

    # Challenge 2 Injection
    zp_data.append({
        "Scheme_ID": "20118869",
        "District": "Thane",
        "Physical_Progress": 0,
        "Financial_Progress": 0,
        "Last_Updated": "10/01/2025" # ZP hasn't updated recently
    })
    
    pd.DataFrame(zp_data).to_csv(OUTPUT_ZP_SCHEME_PROGRESS, index=False)
    print(f"   -> Generated {OUTPUT_ZP_SCHEME_PROGRESS} (Contains Sync Conflict & UK Dates)")

    # ==============================================================================
    # 4. GENERATE MJP FINANCIAL REPORT
    # Challenge 3: Column Shift
    # Date Issue: Uses MM-DD-YYYY format (US Format)
    # ==============================================================================
    print("Generating MJP Financials...")
    mjp_data = []
    for i in range(30):
        actuals = random.randint(100000, 5000000)
        lakhs = round(actuals / 100000, 2)
        
        row = {
            "Scheme_Code": f"MJP-{random.randint(5000, 9000)}",
            "District": random.choice(common_districts),
            "Expenditure_Actuals": actuals,
            "Expenditure_Lakhs": lakhs,
            "Transaction_Date": random_date(fmt="%m-%d-%Y") # US Format
        }
        
        # Challenge 3 Injection (Swap columns)
        if i == 5 or i == 15:
            row["Expenditure_Actuals"] = lakhs
            row["Expenditure_Lakhs"] = actuals
            
        mjp_data.append(row)
        
    pd.DataFrame(mjp_data).to_csv(OUTPUT_MJP_FINANCIAL, index=False)
    print(f"   -> Generated {OUTPUT_MJP_FINANCIAL} (Contains Column Shift & US Dates)")

    # ==============================================================================
    # 5. GENERATE GSDA WATER QUALITY (District Level)
    # Challenge 4: "Andaman & Nicobar" naming (at State column)
    # Granularity: Now matches District level for joining
    # ==============================================================================
    print("Generating GSDA Water Quality...")
    gsda_data = []
    
    for dist in common_districts:
        # Simulate state lookup (simplified)
        state_name = "Maharashtra" 
        
        # Inject Challenge 4 Naming randomly into the 'State' column if we were processing multiple states
        # For simulation, we force one record to be the outlier
        if dist == common_districts[0]: 
            state_name = "Andaman & Nicobar Islands" # Conflict with IMIS "A & N"

        gsda_data.append({
            "State_Name": state_name,
            "District_Name": dist,
            "Samples_Tested": random.randint(500, 2000),
            "Contaminated_Samples": random.randint(0, 50),
            "Lab_Report_Date": random_date(fmt="%Y-%m-%d")
        })

    pd.DataFrame(gsda_data).to_csv(OUTPUT_GSDA_WQMIS, index=False)
    print(f"   -> Generated {OUTPUT_GSDA_WQMIS} (District Level with Naming Conflict)")

    # ==============================================================================
    # 6. GENERATE PGRS (Grievances)
    # New Issue: Logical Error (Resolved Date < Reported Date)
    # ==============================================================================
    print("Generating PGRS Grievances...")
    pgrs_data = []
    for dist in common_districts[:20]:
        report_date_str = random_date(fmt="%Y-%m-%d")
        report_date = datetime.strptime(report_date_str, "%Y-%m-%d")
        
        # Normal Case
        resolve_date = report_date + timedelta(days=5)
        
        # Logical Error Injection
        if random.random() < 0.1: # 10% chance
            resolve_date = report_date - timedelta(days=2) # Resolved before reported!
            
        pgrs_data.append({
            "Ticket_ID": f"TKT-{random.randint(1000,9999)}",
            "District": dist,
            "Issue": "No Water",
            "Date_Reported": report_date.strftime("%Y-%m-%d"),
            "Date_Resolved": resolve_date.strftime("%Y-%m-%d")
        })
        
    pd.DataFrame(pgrs_data).to_csv(OUTPUT_PGRS_GRIEVANCE, index=False)
    print(f"   -> Generated {OUTPUT_PGRS_GRIEVANCE} (Contains Logical Date Errors)")

    print("\n✅ SUCCESS: All 6 Raw Data Files Generated.")

if __name__ == "__main__":
    generate_messy_data()