"""
Конфигурация путей и настроек проекта
"""
from pathlib import Path
import os

# Base paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

# Raw data (input)
RAW_DIR = DATA_DIR / "raw"
RAW_REFERENCE_DIR = RAW_DIR / "reference"
RAW_DOCUMENTS_DIR = RAW_DIR / "documents"

# Reference data subdirs
HCPCS_DIR = RAW_REFERENCE_DIR / "hcpcs"
NCCI_DIR = RAW_REFERENCE_DIR / "ncci"
ICD10_DIR = RAW_REFERENCE_DIR / "icd10"

# Document subdirs
GUIDELINES_DIR = RAW_DOCUMENTS_DIR / "guidelines"
POLICIES_DIR = RAW_DOCUMENTS_DIR / "policies"
CODEBOOKS_DIR = RAW_DOCUMENTS_DIR / "codebooks"

# Processed data (output)
PROCESSED_DIR = DATA_DIR / "processed"
REFERENCE_DB_PATH = PROCESSED_DIR / "reference.db"
DOCUMENTS_STORE_DIR = PROCESSED_DIR / "documents"
DOCUMENTS_INDEX_PATH = DOCUMENTS_STORE_DIR / "index.json"

# Rules output
RULES_DIR = BASE_DIR / "rules"
RULES_CODES_DIR = RULES_DIR / "codes"
RULES_PAYER_DIR = RULES_DIR / "payer"
RULES_CLAIMS_DIR = RULES_DIR / "claims"

# Expected file names (for validation)
EXPECTED_FILES = {
    "hcpcs": {
        "codes": "HCPC2026_JAN_ANWEB_12292025.xlsx",
        "notes": "proc_notes_JAN2026.txt",
    },
    "ncci": {
        "ptp_pra": "ccipra-v320r0-f1.xlsx",
        "mue_pra": "MCR_MUE_PractitionerServices_Eff_01-01-2026.csv",
        "mue_dme": "MCR_MUE_DMESupplierServices_Eff_10-01-2025.csv",
    },
    "icd10": {
        "codes": "icd10-codes.csv",
    },
}

# LLM Settings
GOOGLE_MODEL_NAME = os.getenv("GOOGLE_MODEL_NAME", "gemini-2.5-flash")
CHUNK_SIZE = 10  # Pages per chunk for PDF parsing
MAX_CONCURRENT_CHUNKS = 5

# Cache settings
CACHE_DIR = BASE_DIR / "cache"


def ensure_dirs():
    """Create all required directories"""
    dirs = [
        HCPCS_DIR, NCCI_DIR, ICD10_DIR,
        GUIDELINES_DIR, POLICIES_DIR, CODEBOOKS_DIR,
        PROCESSED_DIR, DOCUMENTS_STORE_DIR,
        RULES_CODES_DIR, RULES_PAYER_DIR, RULES_CLAIMS_DIR,
        CACHE_DIR,
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


def check_raw_files():
    """Check which raw files are present"""
    status = {}
    
    # HCPCS
    status["hcpcs_codes"] = (HCPCS_DIR / EXPECTED_FILES["hcpcs"]["codes"]).exists()
    status["hcpcs_notes"] = (HCPCS_DIR / EXPECTED_FILES["hcpcs"]["notes"]).exists()
    
    # NCCI
    status["ncci_ptp"] = (NCCI_DIR / EXPECTED_FILES["ncci"]["ptp_pra"]).exists()
    status["ncci_mue_pra"] = (NCCI_DIR / EXPECTED_FILES["ncci"]["mue_pra"]).exists()
    status["ncci_mue_dme"] = (NCCI_DIR / EXPECTED_FILES["ncci"]["mue_dme"]).exists()
    
    # ICD-10
    status["icd10_codes"] = (ICD10_DIR / EXPECTED_FILES["icd10"]["codes"]).exists()
    
    # Documents
    status["guidelines"] = list(GUIDELINES_DIR.glob("*.pdf"))
    status["policies"] = list(POLICIES_DIR.glob("*.pdf"))
    
    return status


if __name__ == "__main__":
    ensure_dirs()
    status = check_raw_files()
    
    print("=== Raw Files Status ===\n")
    print("Reference Data:")
    print(f"  HCPCS codes:    {'✅' if status['hcpcs_codes'] else '❌'} {EXPECTED_FILES['hcpcs']['codes']}")
    print(f"  HCPCS notes:    {'✅' if status['hcpcs_notes'] else '❌'} {EXPECTED_FILES['hcpcs']['notes']}")
    print(f"  NCCI PTP:       {'✅' if status['ncci_ptp'] else '❌'} {EXPECTED_FILES['ncci']['ptp_pra']}")
    print(f"  NCCI MUE (PRA): {'✅' if status['ncci_mue_pra'] else '❌'} {EXPECTED_FILES['ncci']['mue_pra']}")
    print(f"  NCCI MUE (DME): {'✅' if status['ncci_mue_dme'] else '❌'} {EXPECTED_FILES['ncci']['mue_dme']}")
    print(f"  ICD-10 codes:   {'✅' if status['icd10_codes'] else '❌'} {EXPECTED_FILES['icd10']['codes']}")
    
    print("\nDocuments:")
    print(f"  Guidelines:     {len(status['guidelines'])} PDF(s)")
    for p in status['guidelines']:
        print(f"                  - {p.name}")
    print(f"  Policies:       {len(status['policies'])} PDF(s)")
    for p in status['policies']:
        print(f"                  - {p.name}")
