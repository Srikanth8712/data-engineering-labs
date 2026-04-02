import sys
from pathlib import Path

ROOT = next(p for p in Path(__file__).resolve().parents if (p / "shared").exists())
sys.path.insert(0, str(ROOT))

import psycopg2
from shared.config import RDS_HOST, RDS_PORT, RDS_DB, RDS_USER, RDS_PASSWORD

conn = psycopg2.connect(
    host=RDS_HOST, port=RDS_PORT,
    dbname=RDS_DB, user=RDS_USER, password=RDS_PASSWORD
)
conn.autocommit = True
cur = conn.cursor()

# ── Insurance policies ────────────────────────────────────────────────
policies = [
    ("POL-789456", "John Smith",    "auto",     "2023-01-01", "2024-01-01", 1200.00, 50000.00,  "active"),
    ("POL-123987", "Sarah Johnson", "property", "2023-03-15", "2024-03-15", 950.00,  200000.00, "active"),
    ("POL-456321", "Mike Davis",    "liability","2022-06-01", "2024-06-01", 2100.00, 500000.00, "active"),
    ("POL-654789", "Emma Wilson",   "auto",     "2023-09-01", "2024-09-01", 1050.00, 50000.00,  "active"),
    ("POL-321654", "James Brown",   "property", "2023-01-15", "2024-01-15", 875.00,  150000.00, "active"),
]

cur.executemany("""
    INSERT INTO insurance.policies
        (policy_number, policy_holder, policy_type, start_date,
         end_date, premium_amount, coverage_limit, status)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (policy_number) DO NOTHING
""", policies)
print(f"Inserted {len(policies)} policies")

# ── Insurance claims ──────────────────────────────────────────────────
claims = [
    ("CLM-2024-001","POL-789456","John Smith",   "4521","2024-11-15","auto_collision",   8500.00, 7500.00, "approved",     "ADJ-042","Indianapolis, IN", 1200.00),
    ("CLM-2024-002","POL-123987","Sarah Johnson","8834","2024-11-18","property_damage",  15200.00,15200.00,"approved",     "ADJ-017","Carmel, IN",        0.00),
    ("CLM-2024-003","POL-456321","Mike Davis",   "2267","2024-11-20","liability",        45000.00,None,    "under_review", "ADJ-042","Tipton, IN",        22000.00),
    ("CLM-2024-004","POL-654789","Emma Wilson",  "9921","2024-11-22","auto_collision",   3200.00, 3200.00, "approved",     "ADJ-017","Fishers, IN",       0.00),
    ("CLM-2024-005","POL-321654","James Brown",  "1143","2024-11-25","property_damage",  28000.00,None,    "under_review", "ADJ-055","Noblesville, IN",   0.00),
    ("CLM-2024-006","POL-789456","John Smith",   "4521","2024-12-01","medical",          5500.00, None,    "pending",      "ADJ-019","Indianapolis, IN",  5500.00),
]

cur.executemany("""
    INSERT INTO insurance.claims
        (claim_id, policy_number, claimant_name, ssn_last4,
         claim_date, claim_type, amount_claimed, amount_approved,
         status, adjuster_id, incident_location, medical_expenses)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (claim_id) DO NOTHING
""", claims)
print(f"Inserted {len(claims)} claims")

# ── Pharma drugs ──────────────────────────────────────────────────────
drugs = [
    ("0093-1048-01", "Metformin",    "Teva",     "antidiabetic",    "1994-12-29"),
    ("0071-0156-23", "Atorvastatin", "Pfizer",   "statin",          "1996-12-17"),
    ("0093-3145-01", "Lisinopril",   "Teva",     "ACE inhibitor",   "1987-12-29"),
    ("0378-5085-01", "Omeprazole",   "Mylan",    "PPI",             "1989-09-14"),
    ("0069-2600-30", "Amlodipine",   "Pfizer",   "calcium blocker", "1992-07-31"),
]

cur.executemany("""
    INSERT INTO pharma.drugs
        (ndc_code, drug_name, manufacturer, drug_class, approval_date)
    VALUES (%s,%s,%s,%s,%s)
    ON CONFLICT (ndc_code) DO NOTHING
""", drugs)
print(f"Inserted {len(drugs)} drugs")

# ── Pharma adverse events ─────────────────────────────────────────────
adverse_events = [
    ("AE-2024-0441","2024-11-10","Metformin",    "0093-1048-01","45-64","F","gastrointestinal","moderate","recovered","physician",  "US","Nausea and diarrhea within 2 weeks of dose increase"),
    ("AE-2024-0442","2024-11-12","Atorvastatin", "0071-0156-23","65+",  "M","musculoskeletal", "severe",  "ongoing",  "pharmacist", "US","Muscle weakness and elevated CK levels after 3 months"),
    ("AE-2024-0443","2024-11-14","Lisinopril",   "0093-3145-01","35-44","F","cardiovascular",  "mild",    "recovered","consumer",   "US","Persistent dry cough within 1 week of starting therapy"),
    ("AE-2024-0444","2024-11-16","Omeprazole",   "0378-5085-01","55-64","M","hepatic",         "moderate","recovered","physician",  "US","Elevated liver enzymes after 6 weeks of treatment"),
    ("AE-2024-0445","2024-11-18","Amlodipine",   "0069-2600-30","45-54","F","cardiovascular",  "mild",    "recovered","physician",  "US","Peripheral edema in lower extremities after 2 months"),
    ("AE-2024-0446","2024-11-20","Metformin",    "0093-1048-01","65+",  "M","metabolic",       "severe",  "ongoing",  "physician",  "US","Lactic acidosis requiring hospitalization"),
    ("AE-2024-0447","2024-11-22","Atorvastatin", "0071-0156-23","35-44","F","musculoskeletal", "moderate","recovered","pharmacist", "US","Myalgia and joint pain after dose increase to 40mg"),
    ("AE-2024-0448","2024-11-24","Lisinopril",   "0093-3145-01","65+",  "M","renal",           "severe",  "ongoing",  "physician",  "US","Acute kidney injury in patient with pre-existing CKD"),
]

cur.executemany("""
    INSERT INTO pharma.adverse_events
        (event_id, report_date, drug_name, ndc_code, patient_age_group,
         patient_gender, adverse_event_type, severity, outcome,
         reporter_type, country, narrative)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (event_id) DO NOTHING
""", adverse_events)
print(f"Inserted {len(adverse_events)} adverse events")

# ── Verify counts ─────────────────────────────────────────────────────
print("\n── Row counts ──────────────────────────────")
for schema, table in [
    ("insurance", "policies"),
    ("insurance", "claims"),
    ("pharma",    "drugs"),
    ("pharma",    "adverse_events"),
]:
    cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
    count = cur.fetchone()[0]
    print(f"  {schema}.{table:20s} : {count} rows")
print("────────────────────────────────────────────\n")

cur.close()
conn.close()
print("Seeding complete.")