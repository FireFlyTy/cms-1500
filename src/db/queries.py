"""
Database query functions for reference data lookups
"""
import sqlite3
from typing import Optional, List, Dict, Any
from pathlib import Path


class ReferenceDB:
    """Reference database query interface"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn = None
    
    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn
    
    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
    
    # === HCPCS Queries ===
    
    def get_hcpcs(self, code: str) -> Optional[Dict[str, Any]]:
        """Get HCPCS code info"""
        cur = self.conn.execute(
            "SELECT * FROM hcpcs WHERE code = ?", (code.upper(),)
        )
        row = cur.fetchone()
        return dict(row) if row else None
    
    def get_hcpcs_by_betos(self, betos_prefix: str) -> List[Dict[str, Any]]:
        """Get all HCPCS codes by BETOS category (e.g., 'D1' for diabetic)"""
        cur = self.conn.execute(
            "SELECT * FROM hcpcs WHERE betos LIKE ?", (f"{betos_prefix}%",)
        )
        return [dict(row) for row in cur.fetchall()]
    
    def get_hcpcs_note(self, note_id: str) -> Optional[str]:
        """Get processing note text"""
        cur = self.conn.execute(
            "SELECT note_text FROM hcpcs_notes WHERE note_id = ?", (note_id,)
        )
        row = cur.fetchone()
        return row["note_text"] if row else None
    
    # === NCCI PTP Queries ===
    
    def check_ptp_conflict(self, code1: str, code2: str) -> Optional[Dict[str, Any]]:
        """Check if two codes have PTP edit conflict"""
        cur = self.conn.execute("""
            SELECT * FROM ncci_ptp 
            WHERE (column1 = ? AND column2 = ?) 
               OR (column1 = ? AND column2 = ?)
            AND (deletion_date IS NULL OR deletion_date = '' OR deletion_date = '*')
        """, (code1, code2, code2, code1))
        row = cur.fetchone()
        if row:
            return {
                "conflict": True,
                "column1": row["column1"],
                "column2": row["column2"],
                "modifier_allowed": row["modifier_indicator"] == 1,
                "modifier_indicator": row["modifier_indicator"],
                "rationale": row["rationale"],
            }
        return None
    
    def get_ptp_edits_for_code(self, code: str) -> List[Dict[str, Any]]:
        """Get all PTP edits where code appears"""
        cur = self.conn.execute("""
            SELECT * FROM ncci_ptp 
            WHERE (column1 = ? OR column2 = ?)
            AND (deletion_date IS NULL OR deletion_date = '' OR deletion_date = '*')
        """, (code, code))
        return [dict(row) for row in cur.fetchall()]
    
    # === NCCI MUE Queries ===
    
    def get_mue(self, code: str) -> Optional[Dict[str, Any]]:
        """Get MUE limit for code (checks both PRA and DME)"""
        # Try practitioner first
        cur = self.conn.execute(
            "SELECT * FROM ncci_mue_pra WHERE code = ?", (code,)
        )
        row = cur.fetchone()
        if row:
            return {"source": "practitioner", **dict(row)}
        
        # Try DME
        cur = self.conn.execute(
            "SELECT * FROM ncci_mue_dme WHERE code = ?", (code,)
        )
        row = cur.fetchone()
        if row:
            return {"source": "dme", **dict(row)}
        
        return None
    
    def check_mue_exceeded(self, code: str, units: int) -> Dict[str, Any]:
        """Check if units exceed MUE limit"""
        mue = self.get_mue(code)
        if mue is None:
            return {"has_mue": False, "exceeded": False}
        
        return {
            "has_mue": True,
            "mue_limit": mue["mue_value"],
            "submitted_units": units,
            "exceeded": units > mue["mue_value"],
            "source": mue["source"],
            "rationale": mue["rationale"],
        }
    
    # === ICD-10 Queries ===
    
    def get_icd10(self, code: str) -> Optional[Dict[str, Any]]:
        """Get ICD-10 code info"""
        cur = self.conn.execute(
            "SELECT * FROM icd10 WHERE code = ?", (code.upper().replace(".", ""),)
        )
        row = cur.fetchone()
        return dict(row) if row else None
    
    def search_icd10(self, pattern: str) -> List[Dict[str, Any]]:
        """Search ICD-10 codes by pattern (e.g., 'E11%' for T2DM)"""
        cur = self.conn.execute(
            "SELECT * FROM icd10 WHERE code LIKE ?", (pattern,)
        )
        return [dict(row) for row in cur.fetchall()]
    
    # === Combined Validation ===
    
    def validate_code_pair(self, code1: str, code2: str) -> Dict[str, Any]:
        """Full validation of code pair"""
        result = {
            "code1": code1,
            "code2": code2,
            "ptp_conflict": None,
            "valid": True,
            "warnings": [],
            "errors": [],
        }
        
        # Check PTP
        ptp = self.check_ptp_conflict(code1, code2)
        if ptp:
            result["ptp_conflict"] = ptp
            if not ptp["modifier_allowed"]:
                result["valid"] = False
                result["errors"].append(
                    f"PTP conflict: {code1} and {code2} cannot be billed together"
                )
            else:
                result["warnings"].append(
                    f"PTP edit: {code1} and {code2} require modifier to bill together"
                )
        
        return result
    
    def enrich_code(self, code: str) -> Dict[str, Any]:
        """Get all available info for a code"""
        result = {"code": code, "found": False}
        
        # Try HCPCS
        hcpcs = self.get_hcpcs(code)
        if hcpcs:
            result["found"] = True
            result["type"] = "HCPCS"
            result["info"] = hcpcs
            if hcpcs.get("proc_note"):
                result["proc_note"] = self.get_hcpcs_note(hcpcs["proc_note"])
            result["mue"] = self.get_mue(code)
            return result
        
        # Try ICD-10
        icd10 = self.get_icd10(code)
        if icd10:
            result["found"] = True
            result["type"] = "ICD-10"
            result["info"] = icd10
            return result
        
        # Try MUE only (for CPT codes not in HCPCS)
        mue = self.get_mue(code)
        if mue:
            result["found"] = True
            result["type"] = "CPT"
            result["mue"] = mue
            return result
        
        return result
