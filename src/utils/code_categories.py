"""
Medical Code Categories

5 основных категорий для демо с маппингом ICD-10 и CPT/HCPCS кодов.
"""

from typing import Optional, Dict, List, Tuple


# =============================================================================
# IGNORED CODES (Modifiers, not real procedure/diagnosis codes)
# =============================================================================

# These are HCPCS modifiers, not actual billable codes
IGNORED_PATTERNS = [
    # Anatomical modifiers
    "E1", "E2", "E3", "E4",  # Eyelid modifiers
    "FA", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9",  # Finger modifiers
    "TA", "T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8", "T9",  # Toe modifiers
    "LT", "RT",  # Left/Right side
    "LC", "LD", "RC", "RD",  # Coronary artery modifiers
    # Service modifiers
    "25", "26", "59", "76", "77", "78", "79",
    "TC",  # Technical component
    "XE", "XP", "XS", "XU",  # Distinct service modifiers
    # Other common modifiers
    "GA", "GY", "GZ",
    "KX",
    "Q5", "Q6",
]

def is_ignored_code(code: str, code_type: str = None) -> bool:
    """Check if code should be ignored (modifiers, etc.)"""
    if not code:
        return True

    # Filter by type if provided
    if code_type and code_type.upper() == 'MODIFIER':
        return True

    code_upper = code.upper().strip()

    # Handle ranges like "E1-E4" - check first part
    if '-' in code_upper:
        first_part = code_upper.split('-')[0].strip()
        if first_part in IGNORED_PATTERNS or len(first_part) <= 2:
            return True

    # Exact match for short modifiers
    if code_upper in IGNORED_PATTERNS:
        return True

    # Also ignore if it's just 2 characters (likely a modifier)
    if len(code_upper) <= 2:
        return True

    return False


# =============================================================================
# CATEGORY DEFINITIONS
# =============================================================================

CATEGORIES = {
    "Diabetes & Metabolic": {
        "color": "#8B5CF6",  # purple
        "description": "Diabetes, metabolic disorders, related medications",
    },
    "Cardiovascular": {
        "color": "#EF4444",  # red
        "description": "Heart disease, hypertension, vascular conditions",
    },
    "Mental Health": {
        "color": "#3B82F6",  # blue
        "description": "Psychiatric conditions, substance use, behavioral health",
    },
    "Renal & Genitourinary": {
        "color": "#10B981",  # green
        "description": "Kidney disease, dialysis, urological conditions",
    },
    "Pain & Musculoskeletal": {
        "color": "#F59E0B",  # amber
        "description": "Chronic pain, arthritis, spine, physical therapy",
    },
}


# =============================================================================
# ICD-10 MAPPING
# =============================================================================

# Format: (prefix, category) - checked in order, first match wins
# More specific prefixes should come BEFORE broader ones
ICD10_PREFIX_MAP: List[Tuple[str, str]] = [
    # Diabetes & Metabolic - specific codes first
    ("E08", "Diabetes & Metabolic"),  # Diabetes due to underlying condition
    ("E09", "Diabetes & Metabolic"),  # Drug-induced diabetes
    ("E10", "Diabetes & Metabolic"),  # Type 1 diabetes
    ("E11", "Diabetes & Metabolic"),  # Type 2 diabetes
    ("E13", "Diabetes & Metabolic"),  # Other specified diabetes
    ("E66", "Diabetes & Metabolic"),  # Obesity
    ("E78", "Diabetes & Metabolic"),  # Hyperlipidemia
    ("Z79.4", "Diabetes & Metabolic"),  # Long-term insulin use
    ("Z79.84", "Diabetes & Metabolic"),  # Long-term oral hypoglycemic use
    ("Z86.32", "Diabetes & Metabolic"),  # History of gestational diabetes
    # Broader endocrine/metabolic patterns for ranges like E00-E89
    ("E0", "Diabetes & Metabolic"),   # E00-E07 thyroid, E08-E09 diabetes
    ("E1", "Diabetes & Metabolic"),   # E10-E14 diabetes
    ("E7", "Diabetes & Metabolic"),   # E70-E89 metabolic disorders

    # Cardiovascular
    ("I10", "Cardiovascular"),  # Essential hypertension
    ("I11", "Cardiovascular"),  # Hypertensive heart disease
    ("I12", "Cardiovascular"),  # Hypertensive CKD
    ("I13", "Cardiovascular"),  # Hypertensive heart and CKD
    ("I20", "Cardiovascular"),  # Angina pectoris
    ("I21", "Cardiovascular"),  # Acute MI
    ("I22", "Cardiovascular"),  # Subsequent MI
    ("I23", "Cardiovascular"),  # Complications of MI
    ("I24", "Cardiovascular"),  # Other acute ischemic heart
    ("I25", "Cardiovascular"),  # Chronic ischemic heart
    ("I26", "Cardiovascular"),  # Pulmonary embolism
    ("I27", "Cardiovascular"),  # Pulmonary heart disease
    ("I30", "Cardiovascular"),  # Pericarditis
    ("I31", "Cardiovascular"),  # Other pericardial diseases
    ("I42", "Cardiovascular"),  # Cardiomyopathy
    ("I44", "Cardiovascular"),  # AV block
    ("I45", "Cardiovascular"),  # Conduction disorders
    ("I46", "Cardiovascular"),  # Cardiac arrest
    ("I47", "Cardiovascular"),  # Paroxysmal tachycardia
    ("I48", "Cardiovascular"),  # Atrial fibrillation/flutter
    ("I49", "Cardiovascular"),  # Other cardiac arrhythmias
    ("I50", "Cardiovascular"),  # Heart failure
    ("I63", "Cardiovascular"),  # Cerebral infarction
    ("I65", "Cardiovascular"),  # Carotid artery occlusion
    ("I66", "Cardiovascular"),  # Cerebral artery occlusion
    ("I70", "Cardiovascular"),  # Atherosclerosis
    ("I71", "Cardiovascular"),  # Aortic aneurysm
    ("I73", "Cardiovascular"),  # Peripheral vascular disease
    ("Z95", "Cardiovascular"),  # Cardiac implant status
    ("Z86.7", "Cardiovascular"),  # History of circulatory disease
    # Broad pattern for ranges like I00-I99
    ("I", "Cardiovascular"),  # All cardiovascular codes

    # Mental Health
    ("F10", "Mental Health"),  # Alcohol use disorders
    ("F11", "Mental Health"),  # Opioid use disorders
    ("F12", "Mental Health"),  # Cannabis use disorders
    ("F13", "Mental Health"),  # Sedative use disorders
    ("F14", "Mental Health"),  # Cocaine use disorders
    ("F15", "Mental Health"),  # Stimulant use disorders
    ("F16", "Mental Health"),  # Hallucinogen use disorders
    ("F17", "Mental Health"),  # Nicotine dependence
    ("F18", "Mental Health"),  # Inhalant use disorders
    ("F19", "Mental Health"),  # Other psychoactive substance
    ("F20", "Mental Health"),  # Schizophrenia
    ("F21", "Mental Health"),  # Schizotypal disorder
    ("F22", "Mental Health"),  # Delusional disorders
    ("F23", "Mental Health"),  # Brief psychotic disorder
    ("F24", "Mental Health"),  # Shared psychotic disorder
    ("F25", "Mental Health"),  # Schizoaffective disorders
    ("F28", "Mental Health"),  # Other psychotic disorders
    ("F29", "Mental Health"),  # Unspecified psychosis
    ("F30", "Mental Health"),  # Manic episode
    ("F31", "Mental Health"),  # Bipolar disorder
    ("F32", "Mental Health"),  # Major depressive disorder, single
    ("F33", "Mental Health"),  # Major depressive disorder, recurrent
    ("F34", "Mental Health"),  # Persistent mood disorders
    ("F39", "Mental Health"),  # Unspecified mood disorder
    ("F40", "Mental Health"),  # Phobic anxiety disorders
    ("F41", "Mental Health"),  # Other anxiety disorders
    ("F42", "Mental Health"),  # OCD
    ("F43", "Mental Health"),  # Trauma/stressor disorders (PTSD)
    ("F44", "Mental Health"),  # Dissociative disorders
    ("F45", "Mental Health"),  # Somatic symptom disorders
    ("F48", "Mental Health"),  # Other nonpsychotic disorders
    ("F50", "Mental Health"),  # Eating disorders
    ("F60", "Mental Health"),  # Personality disorders
    ("F84", "Mental Health"),  # Autism spectrum
    ("F90", "Mental Health"),  # ADHD
    # Broad pattern for ranges like F01-F99
    ("F", "Mental Health"),  # All mental health codes

    # Renal & Genitourinary
    ("N17", "Renal & Genitourinary"),  # Acute kidney failure
    ("N18", "Renal & Genitourinary"),  # Chronic kidney disease
    ("N19", "Renal & Genitourinary"),  # Unspecified kidney failure
    ("N20", "Renal & Genitourinary"),  # Kidney stones
    ("N25", "Renal & Genitourinary"),  # Disorders from tubular function
    ("N26", "Renal & Genitourinary"),  # Unspecified contracted kidney
    ("N28", "Renal & Genitourinary"),  # Other kidney disorders
    ("N39", "Renal & Genitourinary"),  # Urinary tract disorders
    ("N40", "Renal & Genitourinary"),  # BPH
    ("Z94.0", "Renal & Genitourinary"),  # Kidney transplant status
    ("Z99.2", "Renal & Genitourinary"),  # Dialysis dependence
    ("Z49", "Renal & Genitourinary"),  # Dialysis encounter
    # Broad pattern for ranges like N00-N99
    ("N", "Renal & Genitourinary"),  # All genitourinary codes

    # Pain & Musculoskeletal
    ("M15", "Pain & Musculoskeletal"),  # Polyosteoarthritis
    ("M16", "Pain & Musculoskeletal"),  # Hip osteoarthritis
    ("M17", "Pain & Musculoskeletal"),  # Knee osteoarthritis
    ("M18", "Pain & Musculoskeletal"),  # Hand osteoarthritis
    ("M19", "Pain & Musculoskeletal"),  # Other osteoarthritis
    ("M25", "Pain & Musculoskeletal"),  # Joint disorders
    ("M43", "Pain & Musculoskeletal"),  # Spinal deformities
    ("M47", "Pain & Musculoskeletal"),  # Spondylosis
    ("M48", "Pain & Musculoskeletal"),  # Spinal stenosis
    ("M50", "Pain & Musculoskeletal"),  # Cervical disc disorders
    ("M51", "Pain & Musculoskeletal"),  # Thoracic/lumbar disc disorders
    ("M54", "Pain & Musculoskeletal"),  # Dorsalgia (back pain)
    ("M62", "Pain & Musculoskeletal"),  # Muscle disorders
    ("M79", "Pain & Musculoskeletal"),  # Soft tissue disorders
    ("G89", "Pain & Musculoskeletal"),  # Pain, not elsewhere classified
    ("R52", "Pain & Musculoskeletal"),  # Pain, unspecified
    # Broad pattern for ranges like M00-M99
    ("M", "Pain & Musculoskeletal"),  # All musculoskeletal codes
]

# Exact code matches (override prefix matching)
ICD10_EXACT_MAP: Dict[str, str] = {
    "Z79.4": "Diabetes & Metabolic",
    "Z79.84": "Diabetes & Metabolic",
    "Z86.32": "Diabetes & Metabolic",
    "Z94.0": "Renal & Genitourinary",
    "Z99.2": "Renal & Genitourinary",
}


# =============================================================================
# CPT/HCPCS MAPPING - PATTERNS (shorter prefix = broader match)
# =============================================================================

# CPT/HCPCS patterns - use 3-4 char prefixes for broader matching
CPT_PREFIX_MAP: List[Tuple[str, str]] = [
    # Diabetes & Metabolic - Lab tests, supplies, education
    ("8294", "Diabetes & Metabolic"),   # 82947-82952 glucose tests
    ("8295", "Diabetes & Metabolic"),   # 82950-82952 glucose
    ("8303", "Diabetes & Metabolic"),   # 83036 HbA1c
    ("9524", "Diabetes & Metabolic"),   # 95249-95251 CGM
    ("9525", "Diabetes & Metabolic"),   # CGM
    ("E078", "Diabetes & Metabolic"),   # E0784 insulin pump
    ("E210", "Diabetes & Metabolic"),   # E2100-E2103 glucose monitors
    ("A425", "Diabetes & Metabolic"),   # A4253-A4259 diabetes supplies
    ("S914", "Diabetes & Metabolic"),   # S9140-S9145 diabetes education
    ("G010", "Diabetes & Metabolic"),   # G0108-G0109 diabetes self-mgmt

    # Cardiovascular - EKG, Echo, Cath, Holter
    ("9300", "Cardiovascular"),  # 93000-93018 EKG, stress
    ("9301", "Cardiovascular"),  # stress test
    ("9322", "Cardiovascular"),  # 93224-93229 Holter
    ("9330", "Cardiovascular"),  # 93303-93352 echo
    ("9331", "Cardiovascular"),  # echo
    ("9335", "Cardiovascular"),  # echo stress
    ("9345", "Cardiovascular"),  # 93451-93462 cath
    ("9346", "Cardiovascular"),  # cath
    ("3336", "Cardiovascular"),  # 33361-33369 TAVR
    ("E047", "Cardiovascular"),  # E0470-E0472 CPAP/BiPAP

    # Mental Health - Psych eval, therapy, testing
    ("9079", "Mental Health"),   # 90791-90792 psych eval
    ("9083", "Mental Health"),   # 90832-90840 psychotherapy
    ("9084", "Mental Health"),   # 90845-90849 family therapy
    ("9085", "Mental Health"),   # 90853 group
    ("9086", "Mental Health"),   # 90867-90870 TMS, ECT
    ("9087", "Mental Health"),   # ECT
    ("9613", "Mental Health"),   # 96130-96139 psych testing
    ("H003", "Mental Health"),   # H0031-H0037 MH services
    ("H201", "Mental Health"),   # H2011-H2013 crisis

    # Renal & Genitourinary - Dialysis, transplant, access
    ("9093", "Renal & Genitourinary"),  # 90935-90937 hemodialysis
    ("9094", "Renal & Genitourinary"),  # 90945-90947 peritoneal
    ("9095", "Renal & Genitourinary"),  # 90951-90970 ESRD
    ("9096", "Renal & Genitourinary"),  # ESRD
    ("9097", "Renal & Genitourinary"),  # ESRD
    ("5036", "Renal & Genitourinary"),  # 50360-50365 transplant
    ("3680", "Renal & Genitourinary"),  # 36800-36833 AV access
    ("3681", "Renal & Genitourinary"),  # AV fistula
    ("3682", "Renal & Genitourinary"),  # AV fistula
    ("3683", "Renal & Genitourinary"),  # AV revision

    # Pain & Musculoskeletal - Injections, PT
    ("2055", "Pain & Musculoskeletal"),  # 20552-20553 trigger point
    ("6232", "Pain & Musculoskeletal"),  # 62320-62327 epidural
    ("6449", "Pain & Musculoskeletal"),  # 64490-64495 facet inj
    ("6463", "Pain & Musculoskeletal"),  # 64633-64636 facet destruction
    ("9711", "Pain & Musculoskeletal"),  # 97110-97116 exercises
    ("9714", "Pain & Musculoskeletal"),  # 97140 manual therapy
    ("9753", "Pain & Musculoskeletal"),  # 97530-97542 activities
    ("9776", "Pain & Musculoskeletal"),  # 97760-97763 orthotics
]


# =============================================================================
# CATEGORIZATION FUNCTIONS
# =============================================================================

def get_code_category(code: str) -> Dict:
    """
    Определяет категорию для кода (ICD-10 или CPT/HCPCS).
    Обрабатывает также диапазоны: E00-E89, 90832-90838

    Returns:
        {
            'category': 'Diabetes & Metabolic',
            'color': '#8B5CF6',
            'matched_by': 'prefix E11'
        }
    """
    if not code:
        return {'category': None, 'color': '#6B7280', 'matched_by': None}

    code = code.upper().strip()

    # Handle ranges like "E00-E89", "90832-90838"
    if '-' in code:
        parts = code.split('-')
        # Try first part of range
        code = parts[0].strip()

    # 1. Check exact matches first
    if code in ICD10_EXACT_MAP:
        category = ICD10_EXACT_MAP[code]
        return {
            'category': category,
            'color': CATEGORIES[category]['color'],
            'matched_by': f'exact {code}'
        }

    # 2. Check ICD-10 prefix matches
    for prefix, category in ICD10_PREFIX_MAP:
        if code.startswith(prefix):
            return {
                'category': category,
                'color': CATEGORIES[category]['color'],
                'matched_by': f'ICD-10 prefix {prefix}'
            }

    # 3. Check CPT/HCPCS matches
    for prefix, category in CPT_PREFIX_MAP:
        if code.startswith(prefix):
            return {
                'category': category,
                'color': CATEGORIES[category]['color'],
                'matched_by': f'CPT prefix {prefix}'
            }

    # 4. No match - return None category
    return {'category': None, 'color': '#6B7280', 'matched_by': None}


def get_all_categories() -> List[Dict]:
    """
    Возвращает список всех категорий.
    """
    return [
        {
            'name': name,
            'color': info['color'],
            'description': info['description']
        }
        for name, info in CATEGORIES.items()
    ]


def group_codes_by_category(codes: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Группирует коды по категориям.

    Input: [{'code': 'E11.9', 'type': 'ICD-10', ...}, ...]
    Output: {
        'Diabetes & Metabolic': [
            {'code': 'E11.9', 'category_info': {...}, ...}
        ],
        ...
    }

    Коды без категории и модификаторы НЕ включаются.
    """
    grouped = {}

    for code_info in codes:
        code = code_info.get('code', '')
        code_type = code_info.get('type', '')

        # Skip modifiers and ignored codes
        if is_ignored_code(code, code_type):
            continue

        cat_info = get_code_category(code)
        category = cat_info['category']

        # Skip codes without category (hide for demo)
        if not category:
            continue

        if category not in grouped:
            grouped[category] = []

        grouped[category].append({
            **code_info,
            'category_info': cat_info
        })

    return grouped


def is_code_in_demo_categories(code: str) -> bool:
    """
    Проверяет, входит ли код в одну из 5 демо-категорий.
    """
    cat_info = get_code_category(code)
    return cat_info['category'] is not None