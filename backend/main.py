from fastapi import FastAPI, UploadFile, File, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from typing import List, Dict, Any
import pandas as pd
import numpy as np
import io
import requests
import os
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert

app = FastAPI(title="JJM Cloud Backend")

# --- CORS: ALLOW FRONTEND TO CONNECT ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this to your Vercel Domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 1. SERVE STATIC FILES (REPLACES MOCK SERVER) ---
# This serves your CSVs directly from the backend
os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

# --- 2. DATABASE CONFIGURATION (CLOUD + LOCAL FALLBACK) ---
# Looks for Render's DB URL, else uses your local default
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:india4156@localhost/jjm_db")
if DB_URL and DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DB_URL)

# --- 3. DYNAMIC GOVT API URLs ---
# In Cloud, points to https://your-app.onrender.com. Locally, http://127.0.0.1:8000
BASE_URL = os.getenv("RENDER_EXTERNAL_URL", "http://127.0.0.1:8000")
GOVT_API_URLS = {
    "imis_tap": f"{BASE_URL}/static/raw_imis_tap_water_status.csv",
    "imis_schemes": f"{BASE_URL}/static/raw_imis_scheme_master.csv",
    "zp": f"{BASE_URL}/static/raw_zp_scheme_progress.csv",
    "mjp": f"{BASE_URL}/static/raw_mjp_financial_report.csv",
    "gsda": f"{BASE_URL}/static/raw_gsda_water_quality.csv",
    "pgrs": f"{BASE_URL}/static/raw_pgrs_grievance.csv"
}

# --- CONFIGURATION ---
STATE_MAPPING = {
    "A & N Islands": "Andaman & Nicobar Islands",
    "Andaman & Nicobar Islands": "Andaman & Nicobar Islands",
    "Maharashtra": "Maharashtra"
}

# --- UTILS ---
def clean_financials(row):
    try:
        act = float(row['Expenditure_Actuals']) if pd.notnull(row['Expenditure_Actuals']) else 0.0
        lakhs = float(row['Expenditure_Lakhs']) if pd.notnull(row['Expenditure_Lakhs']) else 0.0
    except: return 0.0, 0.0
    if lakhs > 1000 and act < 1000: return lakhs, act 
    return act, lakhs

# --- DATABASE HELPERS (SQLAlchemy 2.0 Robust) ---
def ensure_primary_key(table_name, pk_column):
    """Forces Primary Key constraint on PostgreSQL table"""
    try:
        with engine.connect() as conn:
            check_sql = text(f"SELECT constraint_name FROM information_schema.table_constraints WHERE table_name = '{table_name}' AND constraint_type = 'PRIMARY KEY'")
            if not conn.execute(check_sql).fetchone():
                conn.execute(text(f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ("{pk_column}")'))
                conn.commit()
    except Exception as e: print(f"PK Warning for {table_name}: {e}")

def perform_upsert(df, table_name, primary_key):
    """Upserts Dataframe to Postgres"""
    if df.empty: return
    ensure_primary_key(table_name, primary_key)
    metadata = MetaData()
    try:
        target_table = Table(table_name, metadata, autoload_with=engine)
    except: return 

    with engine.connect() as conn:
        data = df.to_dict(orient='records')
        stmt = insert(target_table).values(data)
        # Exclude PK from update
        update_cols = {col.name: stmt.excluded[col.name] for col in target_table.c if col.name != primary_key}
        
        if update_cols:
            on_conflict_stmt = stmt.on_conflict_do_update(index_elements=[primary_key], set_=update_cols)
            conn.execute(on_conflict_stmt)
        else:
            conn.execute(stmt.on_conflict_do_nothing(index_elements=[primary_key]))
        conn.commit()

# --- CORE PIPELINE (ROBUST UNIVERSAL LOGIC) ---
def run_etl_pipeline(dfs):
    anomalies = []

    # 1. ANOMALY DETECTION
    if 'gsda' in dfs:
        df_gsda = dfs['gsda']
        if 'State_Name' in df_gsda.columns:
            non_std = df_gsda[~df_gsda['State_Name'].isin(STATE_MAPPING.values())]
            for _, row in non_std.iterrows():
                anomalies.append({"Scheme_ID": "N/A", "Issue_Type": "Naming Convention", "Severity": "Medium", "Description": f"Non-standard State: '{row['State_Name']}'"})

    df_imis = dfs['imis_schemes'].copy()
    df_imis.rename(columns={'IMIS_ID': 'Scheme_ID'}, inplace=True) 
    
    if 'zp' in dfs:
        df_zp = dfs['zp'].copy()
        merged_check = pd.merge(df_imis, df_zp, on="Scheme_ID", how="outer", suffixes=('_IMIS', '_ZP'))
        for _, row in merged_check.iterrows():
            status = str(row['Status']).lower() if pd.notnull(row['Status']) else ""
            phy_prog = row['Physical_Progress'] if pd.notnull(row['Physical_Progress']) else 0
            fin_prog = row.get('Financial_Progress', 0)
            
            if status == 'completed' and phy_prog == 0:
                anomalies.append({"Scheme_ID": row['Scheme_ID'], "Issue_Type": "Sync Conflict", "Severity": "Critical", "Description": "IMIS Complete vs ZP Pending"})
            if phy_prog == 0 and fin_prog > 0:
                anomalies.append({"Scheme_ID": row['Scheme_ID'], "Issue_Type": "Ghost Asset", "Severity": "High", "Description": f"Fin Progress {fin_prog}% without Physical Progress"})

    if 'mjp' in dfs:
        df_mjp = dfs['mjp'].copy()
        cleaned_fins = df_mjp.apply(clean_financials, axis=1, result_type='expand')
        df_mjp['Cleaned_Expenditure_INR'] = cleaned_fins[0]
        for i, row in df_mjp.iterrows():
            if abs(float(row['Expenditure_Actuals']) - row['Cleaned_Expenditure_INR']) > 1.0: 
                anomalies.append({"Scheme_ID": row['Scheme_Code'], "Issue_Type": "Column Mismatch", "Severity": "Medium", "Description": "Financial Columns Swapped. Auto-corrected."})
        df_mjp.rename(columns={'Scheme_Code': 'Scheme_ID'}, inplace=True)
    else:
        df_mjp = pd.DataFrame(columns=['Scheme_ID', 'District', 'Cleaned_Expenditure_INR'])

    if 'pgrs' in dfs:
        for _, row in dfs['pgrs'].iterrows():
            try:
                if pd.to_datetime(row['Date_Resolved']) < pd.to_datetime(row['Date_Reported']):
                    anomalies.append({"Scheme_ID": row['Ticket_ID'], "Issue_Type": "Logical Data Error", "Severity": "Low", "Description": "Ticket Resolved before Reported."})
            except: pass

    # 2. REPO GENERATION (UNIVERSAL MERGE)
    src_imis = df_imis[['Scheme_ID', 'District', 'Scheme_Name', 'Status', 'Completion_Date']].copy()
    
    src_zp = pd.DataFrame()
    if 'zp' in dfs: 
        src_zp = dfs['zp'][['Scheme_ID', 'District', 'Physical_Progress', 'Financial_Progress', 'Last_Updated']].copy()
    
    src_mjp = pd.DataFrame()
    if 'mjp' in dfs: 
        src_mjp = df_mjp.groupby(['Scheme_ID', 'District'])['Cleaned_Expenditure_INR'].sum().reset_index()

    # The Universal Outer Join
    unified = src_imis.copy()
    if not src_zp.empty:
        unified = pd.merge(unified, src_zp, on='Scheme_ID', how='outer', suffixes=('', '_ZP'))
        unified['District'] = unified['District'].fillna(unified['District_ZP'])
        unified.drop(columns=['District_ZP'], inplace=True)
    if not src_mjp.empty:
        unified = pd.merge(unified, src_mjp, on='Scheme_ID', how='outer', suffixes=('', '_MJP'))
        if 'District_MJP' in unified.columns:
            unified['District'] = unified['District'].fillna(unified['District_MJP'])
            unified.drop(columns=['District_MJP'], inplace=True)

    # Clean Values
    for c in ['Physical_Progress', 'Financial_Progress', 'Cleaned_Expenditure_INR']:
        if c in unified.columns: unified[c] = unified[c].fillna(0)
    for c in ['Scheme_Name', 'Status', 'Completion_Date', 'Last_Updated', 'District']:
        if c in unified.columns: unified[c] = unified[c].fillna("-")

    # Status Inference Logic (Matches Screenshots)
    def determine_status(row):
        status = str(row.get('Status', '-')).lower()
        phy = row.get('Physical_Progress', 0)
        fin_mjp = row.get('Cleaned_Expenditure_INR', 0)
        
        if status == 'completed' and phy == 0: return "DATA CONFLICT"
        if status == '-' or status == 'nan':
            if phy > 90: return "Completed (ZP)"
            if phy > 0: return "Ongoing (ZP)"
            if fin_mjp > 0: return "Financial Only"
            return "Unknown"
        return row.get('Status', '-')

    unified['Unified_Status'] = unified.apply(determine_status, axis=1)
    unified['Scheme_Name'] = unified.apply(lambda r: f"Scheme {r['Scheme_ID']}" if r['Scheme_Name'] in ['-', 'nan', ''] else r['Scheme_Name'], axis=1)
    unified_schemes = unified

    # Districts Logic
    unique_districts = pd.DataFrame(unified_schemes['District'].unique(), columns=['District_Name'])
    unique_districts = unique_districts[unique_districts['District_Name'] != '-']
    
    if 'gsda' in dfs:
        df_gsda = dfs['gsda'].copy()
        gsda_grouped = df_gsda.groupby('District_Name')[['Samples_Tested', 'Contaminated_Samples']].sum().reset_index()
        unified_districts = pd.merge(unique_districts, gsda_grouped, on='District_Name', how='left')
    else:
        unified_districts['Samples_Tested'] = 0
        unified_districts['Contaminated_Samples'] = 0

    if 'pgrs' in dfs:
        df_pgrs = dfs['pgrs'].copy()
        df_pgrs.rename(columns={'District': 'District_Name'}, inplace=True)
        pgrs_grouped = df_pgrs.groupby('District_Name').size().reset_index(name='Total_Grievances')
        unified_districts = pd.merge(unified_districts, pgrs_grouped, on='District_Name', how='left')
    else:
        unified_districts['Total_Grievances'] = 0

    unified_districts = unified_districts.fillna(0)
    def calc_rate(row): return round((row['Contaminated_Samples'] / row['Samples_Tested']) * 100, 2) if row['Samples_Tested'] > 0 else 0.0
    unified_districts['Contamination_Rate'] = unified_districts.apply(calc_rate, axis=1)

    # Master Logic
    repo_2_join = unified_districts.rename(columns={'District_Name': 'District'})
    unified_master = pd.merge(unified_schemes, repo_2_join, on='District', how='left')
    unified_master = unified_master.replace({np.nan: 0, '': '-'})

    return {
        "status": "success", 
        "anomalies": anomalies, 
        "repo_schemes": unified_schemes.replace({np.nan: None}).to_dict(orient='records'),
        "repo_districts": unified_districts.replace({np.nan: None}).to_dict(orient='records'),
        "repo_master": unified_master.replace({np.nan: None}).to_dict(orient='records')
    }

# --- ROUTES ---

@app.post("/standardize")
async def standardize_data(files: List[UploadFile] = File(...)):
    dfs = {}
    for file in files:
        content = await file.read()
        filename = file.filename.lower()
        try:
            if "imis" in filename and "tap" in filename: dfs['imis_tap'] = pd.read_csv(io.BytesIO(content))
            elif "imis" in filename and "scheme" in filename: dfs['imis_schemes'] = pd.read_csv(io.BytesIO(content))
            elif "zp" in filename: dfs['zp'] = pd.read_csv(io.BytesIO(content))
            elif "mjp" in filename: dfs['mjp'] = pd.read_csv(io.BytesIO(content))
            elif "gsda" in filename: dfs['gsda'] = pd.read_csv(io.BytesIO(content))
            elif "pgrs" in filename: dfs['pgrs'] = pd.read_csv(io.BytesIO(content))
        except Exception as e: print(f"Error reading {filename}: {e}")
    if 'imis_schemes' not in dfs: raise HTTPException(status_code=400, detail="Missing Critical File: IMIS Scheme Master")
    return run_etl_pipeline(dfs)

@app.post("/fetch-from-api")
async def fetch_data_from_api():
    print("Initiating API Fetch...")
    dfs = {}
    for key, url in GOVT_API_URLS.items():
        try:
            response = requests.get(url)
            response.raise_for_status()
            dfs[key] = pd.read_csv(io.BytesIO(response.content))
        except Exception as e: print(f"Failed to fetch {key}: {e}")
    if 'imis_schemes' not in dfs: raise HTTPException(status_code=502, detail="Failed to fetch Critical Data")
    return run_etl_pipeline(dfs)

@app.post("/save-to-db")
async def save_to_db(payload: Dict[str, Any] = Body(...)):
    try:
        if 'repo_schemes' in payload:
            df = pd.DataFrame(payload['repo_schemes'])
            df.head(0).to_sql('table_schemes', engine, if_exists='append', index=False)
            perform_upsert(df, 'table_schemes', 'Scheme_ID')

        if 'repo_districts' in payload:
            df = pd.DataFrame(payload['repo_districts'])
            df.head(0).to_sql('table_districts', engine, if_exists='append', index=False)
            perform_upsert(df, 'table_districts', 'District_Name')

        if 'repo_master' in payload:
            df = pd.DataFrame(payload['repo_master'])
            df.head(0).to_sql('table_master', engine, if_exists='append', index=False)
            perform_upsert(df, 'table_master', 'Scheme_ID')
            
        if 'anomalies' in payload:
            df = pd.DataFrame(payload['anomalies'])
            df.to_sql('table_anomalies', engine, if_exists='replace', index=False)

        return {"status": "success", "message": "Data Upserted to PostgreSQL successfully."}
    except Exception as e:
        print(f"DB Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/load-from-db")
async def load_from_db():
    try:
        with engine.connect() as conn:
            try:
                schemes = pd.read_sql("SELECT * FROM table_schemes", conn).replace({np.nan: None}).to_dict(orient='records')
                districts = pd.read_sql("SELECT * FROM table_districts", conn).replace({np.nan: None}).to_dict(orient='records')
                master = pd.read_sql("SELECT * FROM table_master", conn).replace({np.nan: None}).to_dict(orient='records')
                anomalies = pd.read_sql("SELECT * FROM table_anomalies", conn).replace({np.nan: None}).to_dict(orient='records')
                return {"status": "success", "repo_schemes": schemes, "repo_districts": districts, "repo_master": master, "anomalies": anomalies}
            except Exception: return {"status": "empty", "message": "No data in database."}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))