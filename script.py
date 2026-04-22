
import os
import json
import re
import random
from datetime import datetime, timedelta, timezone
from faker import Faker
import psycopg2
from psycopg2.extras import RealDictCursor
import ollama

fake = Faker('en_IN')
IN_STATES = ['MH', 'DL', 'GJ', 'RJ', 'UP', 'TN', 'KA', 'WB', 'AP', 'PB']
random.seed(42)

# ─────────────────────────────────────────────────
# DB CONNECTION — adjust to your local setup
# ─────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     os.getenv("DB_PORT",     5432),
    "dbname":   os.getenv("DB_NAME",     "rfpilot"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "beauty"),
}

OLLAMA_MODEL = "qwen2.5:7b-instruct"

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def now_minus(days=0, hours=0):
    return datetime.now(timezone.utc) - timedelta(days=days, hours=hours)


# ═══════════════════════════════════════════════════════════════
# OLLAMA HELPERS
# ═══════════════════════════════════════════════════════════════

def call_ollama(prompt: str, max_retries: int = 3) -> list | dict:
    for attempt in range(max_retries):
        try:
            response = ollama.chat(
                model=OLLAMA_MODEL,
                messages=[{"role": "user", "content": prompt}],
                format="json",
            )
            raw = response["message"]["content"]
            raw = re.sub(r"^```json\\s*", "", raw.strip())
            raw = re.sub(r"\\s*```$",     "", raw.strip())
            data = json.loads(raw)
            # normalise: unwrap {"skus": [...]} → [...]
            if isinstance(data, dict):
                data = next((v for v in data.values() if isinstance(v, list)), data)
            return data
        except (json.JSONDecodeError, KeyError) as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            if attempt == max_retries - 1:
                raise
    return []


VALID_CONDUCTORS  = {"copper", "aluminium"}
VALID_INSULATIONS = {"XLPE", "PVC", "EPR", "HEPR"}
VALID_VOLTAGES    = {1.1, 3.3, 6.6, 11.0, 22.0, 33.0}
VALID_CORES       = {1, 2, 3, 4}
VALID_SECTIONS    = {10, 16, 25, 35, 50, 70, 95, 120, 150, 185, 240, 300}
FALLBACK_SKUS = [
    {"sku_code":"SKU-1001","product_name":"1.1kV Copper XLPE 35mm2 3-Core Armoured","voltage_kv":1.1,"conductor_material":"copper","cross_section_mm2":35,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":145.0,"stock_meters":18000,"lead_time_days":14},
    {"sku_code":"SKU-1002","product_name":"1.1kV Copper PVC 16mm2 4-Core Unarmoured","voltage_kv":1.1,"conductor_material":"copper","cross_section_mm2":16,"insulation_type":"PVC","armoured":False,"number_of_cores":4,"price_per_meter":98.0,"stock_meters":22000,"lead_time_days":10},
    {"sku_code":"SKU-1003","product_name":"1.1kV Aluminium XLPE 50mm2 3-Core Armoured","voltage_kv":1.1,"conductor_material":"aluminium","cross_section_mm2":50,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":112.0,"stock_meters":20000,"lead_time_days":12},
    {"sku_code":"SKU-1004","product_name":"1.1kV Copper PVC 10mm2 2-Core Unarmoured","voltage_kv":1.1,"conductor_material":"copper","cross_section_mm2":10,"insulation_type":"PVC","armoured":False,"number_of_cores":2,"price_per_meter":82.0,"stock_meters":30000,"lead_time_days":7},
    {"sku_code":"SKU-1005","product_name":"1.1kV Aluminium PVC 70mm2 4-Core Armoured","voltage_kv":1.1,"conductor_material":"aluminium","cross_section_mm2":70,"insulation_type":"PVC","armoured":True,"number_of_cores":4,"price_per_meter":155.0,"stock_meters":12000,"lead_time_days":14},
    {"sku_code":"SKU-1006","product_name":"1.1kV Copper XLPE 95mm2 1-Core Unarmoured","voltage_kv":1.1,"conductor_material":"copper","cross_section_mm2":95,"insulation_type":"XLPE","armoured":False,"number_of_cores":1,"price_per_meter":175.0,"stock_meters":10000,"lead_time_days":10},
    {"sku_code":"SKU-1007","product_name":"1.1kV Aluminium XLPE 120mm2 3-Core Armoured","voltage_kv":1.1,"conductor_material":"aluminium","cross_section_mm2":120,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":192.0,"stock_meters":8000,"lead_time_days":18},
    {"sku_code":"SKU-1008","product_name":"1.1kV Copper PVC 25mm2 4-Core Armoured","voltage_kv":1.1,"conductor_material":"copper","cross_section_mm2":25,"insulation_type":"PVC","armoured":True,"number_of_cores":4,"price_per_meter":118.0,"stock_meters":16000,"lead_time_days":12},
    {"sku_code":"SKU-1009","product_name":"3.3kV Copper XLPE 50mm2 3-Core Armoured","voltage_kv":3.3,"conductor_material":"copper","cross_section_mm2":50,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":385.0,"stock_meters":9000,"lead_time_days":21},
    {"sku_code":"SKU-1010","product_name":"3.3kV Copper XLPE 95mm2 1-Core Unarmoured","voltage_kv":3.3,"conductor_material":"copper","cross_section_mm2":95,"insulation_type":"XLPE","armoured":False,"number_of_cores":1,"price_per_meter":450.0,"stock_meters":7000,"lead_time_days":18},
    {"sku_code":"SKU-1011","product_name":"3.3kV Aluminium XLPE 70mm2 3-Core Armoured","voltage_kv":3.3,"conductor_material":"aluminium","cross_section_mm2":70,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":295.0,"stock_meters":11000,"lead_time_days":21},
    {"sku_code":"SKU-1012","product_name":"3.3kV Copper EPR 35mm2 3-Core Armoured","voltage_kv":3.3,"conductor_material":"copper","cross_section_mm2":35,"insulation_type":"EPR","armoured":True,"number_of_cores":3,"price_per_meter":420.0,"stock_meters":6000,"lead_time_days":25},
    {"sku_code":"SKU-1013","product_name":"3.3kV Aluminium PVC 120mm2 3-Core Armoured","voltage_kv":3.3,"conductor_material":"aluminium","cross_section_mm2":120,"insulation_type":"PVC","armoured":True,"number_of_cores":3,"price_per_meter":355.0,"stock_meters":8500,"lead_time_days":21},
    {"sku_code":"SKU-1014","product_name":"3.3kV Copper XLPE 185mm2 1-Core Unarmoured","voltage_kv":3.3,"conductor_material":"copper","cross_section_mm2":185,"insulation_type":"XLPE","armoured":False,"number_of_cores":1,"price_per_meter":575.0,"stock_meters":5000,"lead_time_days":28},
    {"sku_code":"SKU-1015","product_name":"6.6kV Copper XLPE 70mm2 3-Core Armoured","voltage_kv":6.6,"conductor_material":"copper","cross_section_mm2":70,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":680.0,"stock_meters":7000,"lead_time_days":28},
    {"sku_code":"SKU-1016","product_name":"6.6kV Aluminium XLPE 95mm2 3-Core Armoured","voltage_kv":6.6,"conductor_material":"aluminium","cross_section_mm2":95,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":520.0,"stock_meters":9000,"lead_time_days":28},
    {"sku_code":"SKU-1017","product_name":"6.6kV Copper EPR 50mm2 3-Core Armoured","voltage_kv":6.6,"conductor_material":"copper","cross_section_mm2":50,"insulation_type":"EPR","armoured":True,"number_of_cores":3,"price_per_meter":720.0,"stock_meters":5500,"lead_time_days":30},
    {"sku_code":"SKU-1018","product_name":"6.6kV Copper XLPE 150mm2 1-Core Unarmoured","voltage_kv":6.6,"conductor_material":"copper","cross_section_mm2":150,"insulation_type":"XLPE","armoured":False,"number_of_cores":1,"price_per_meter":850.0,"stock_meters":4000,"lead_time_days":25},
    {"sku_code":"SKU-1019","product_name":"6.6kV Aluminium PVC 120mm2 3-Core Armoured","voltage_kv":6.6,"conductor_material":"aluminium","cross_section_mm2":120,"insulation_type":"PVC","armoured":True,"number_of_cores":3,"price_per_meter":610.0,"stock_meters":6500,"lead_time_days":28},
    {"sku_code":"SKU-1020","product_name":"6.6kV Copper XLPE 240mm2 3-Core Armoured","voltage_kv":6.6,"conductor_material":"copper","cross_section_mm2":240,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":880.0,"stock_meters":3500,"lead_time_days":35},
    {"sku_code":"SKU-1021","product_name":"11kV Copper XLPE 95mm2 3-Core Armoured","voltage_kv":11.0,"conductor_material":"copper","cross_section_mm2":95,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":1100.0,"stock_meters":5000,"lead_time_days":35},
    {"sku_code":"SKU-1022","product_name":"11kV Copper XLPE 150mm2 1-Core Unarmoured","voltage_kv":11.0,"conductor_material":"copper","cross_section_mm2":150,"insulation_type":"XLPE","armoured":False,"number_of_cores":1,"price_per_meter":1250.0,"stock_meters":4000,"lead_time_days":30},
    {"sku_code":"SKU-1023","product_name":"11kV Aluminium XLPE 120mm2 3-Core Armoured","voltage_kv":11.0,"conductor_material":"aluminium","cross_section_mm2":120,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":890.0,"stock_meters":6000,"lead_time_days":35},
    {"sku_code":"SKU-1024","product_name":"11kV Copper EPR 70mm2 3-Core Armoured","voltage_kv":11.0,"conductor_material":"copper","cross_section_mm2":70,"insulation_type":"EPR","armoured":True,"number_of_cores":3,"price_per_meter":1180.0,"stock_meters":3500,"lead_time_days":40},
    {"sku_code":"SKU-1025","product_name":"11kV Copper XLPE 240mm2 3-Core Armoured","voltage_kv":11.0,"conductor_material":"copper","cross_section_mm2":240,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":1450.0,"stock_meters":2500,"lead_time_days":42},
    {"sku_code":"SKU-1026","product_name":"11kV Aluminium XLPE 185mm2 3-Core Armoured","voltage_kv":11.0,"conductor_material":"aluminium","cross_section_mm2":185,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":1050.0,"stock_meters":3000,"lead_time_days":40},
    {"sku_code":"SKU-1027","product_name":"11kV Copper HEPR 50mm2 3-Core Armoured","voltage_kv":11.0,"conductor_material":"copper","cross_section_mm2":50,"insulation_type":"HEPR","armoured":True,"number_of_cores":3,"price_per_meter":1320.0,"stock_meters":2000,"lead_time_days":45},
    {"sku_code":"SKU-1028","product_name":"11kV Aluminium EPR 95mm2 3-Core Armoured","voltage_kv":11.0,"conductor_material":"aluminium","cross_section_mm2":95,"insulation_type":"EPR","armoured":True,"number_of_cores":3,"price_per_meter":980.0,"stock_meters":4000,"lead_time_days":38},
    {"sku_code":"SKU-1029","product_name":"22kV Copper XLPE 150mm2 3-Core Armoured","voltage_kv":22.0,"conductor_material":"copper","cross_section_mm2":150,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":2200.0,"stock_meters":2000,"lead_time_days":45},
    {"sku_code":"SKU-1030","product_name":"22kV Copper XLPE 95mm2 1-Core Unarmoured","voltage_kv":22.0,"conductor_material":"copper","cross_section_mm2":95,"insulation_type":"XLPE","armoured":False,"number_of_cores":1,"price_per_meter":1750.0,"stock_meters":3000,"lead_time_days":40},
    {"sku_code":"SKU-1031","product_name":"22kV Aluminium XLPE 185mm2 3-Core Armoured","voltage_kv":22.0,"conductor_material":"aluminium","cross_section_mm2":185,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":1850.0,"stock_meters":2500,"lead_time_days":45},
    {"sku_code":"SKU-1032","product_name":"22kV Copper EPR 120mm2 3-Core Armoured","voltage_kv":22.0,"conductor_material":"copper","cross_section_mm2":120,"insulation_type":"EPR","armoured":True,"number_of_cores":3,"price_per_meter":2450.0,"stock_meters":1500,"lead_time_days":45},
    {"sku_code":"SKU-1033","product_name":"22kV Copper XLPE 240mm2 3-Core Armoured","voltage_kv":22.0,"conductor_material":"copper","cross_section_mm2":240,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":2950.0,"stock_meters":1200,"lead_time_days":45},
    {"sku_code":"SKU-1034","product_name":"22kV Aluminium XLPE 120mm2 3-Core Armoured","voltage_kv":22.0,"conductor_material":"aluminium","cross_section_mm2":120,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":1620.0,"stock_meters":2200,"lead_time_days":45},
    {"sku_code":"SKU-1035","product_name":"22kV Copper HEPR 95mm2 3-Core Armoured","voltage_kv":22.0,"conductor_material":"copper","cross_section_mm2":95,"insulation_type":"HEPR","armoured":True,"number_of_cores":3,"price_per_meter":2600.0,"stock_meters":1000,"lead_time_days":45},
    {"sku_code":"SKU-1036","product_name":"22kV Aluminium EPR 150mm2 3-Core Armoured","voltage_kv":22.0,"conductor_material":"aluminium","cross_section_mm2":150,"insulation_type":"EPR","armoured":True,"number_of_cores":3,"price_per_meter":1980.0,"stock_meters":1800,"lead_time_days":45},
    {"sku_code":"SKU-1037","product_name":"33kV Copper XLPE 185mm2 3-Core Armoured","voltage_kv":33.0,"conductor_material":"copper","cross_section_mm2":185,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":3800.0,"stock_meters":1000,"lead_time_days":45},
    {"sku_code":"SKU-1038","product_name":"33kV Copper XLPE 120mm2 1-Core Unarmoured","voltage_kv":33.0,"conductor_material":"copper","cross_section_mm2":120,"insulation_type":"XLPE","armoured":False,"number_of_cores":1,"price_per_meter":2800.0,"stock_meters":1500,"lead_time_days":45},
    {"sku_code":"SKU-1039","product_name":"33kV Aluminium XLPE 240mm2 3-Core Armoured","voltage_kv":33.0,"conductor_material":"aluminium","cross_section_mm2":240,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":3200.0,"stock_meters":800,"lead_time_days":45},
    {"sku_code":"SKU-1040","product_name":"33kV Copper EPR 150mm2 3-Core Armoured","voltage_kv":33.0,"conductor_material":"copper","cross_section_mm2":150,"insulation_type":"EPR","armoured":True,"number_of_cores":3,"price_per_meter":4200.0,"stock_meters":700,"lead_time_days":45},
    {"sku_code":"SKU-1041","product_name":"33kV Copper XLPE 300mm2 3-Core Armoured","voltage_kv":33.0,"conductor_material":"copper","cross_section_mm2":300,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":4800.0,"stock_meters":600,"lead_time_days":45},
    {"sku_code":"SKU-1042","product_name":"1.1kV Copper XLPE 185mm2 3-Core Armoured","voltage_kv":1.1,"conductor_material":"copper","cross_section_mm2":185,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":198.0,"stock_meters":7000,"lead_time_days":18},
    {"sku_code":"SKU-1043","product_name":"3.3kV Copper XLPE 240mm2 3-Core Armoured","voltage_kv":3.3,"conductor_material":"copper","cross_section_mm2":240,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":595.0,"stock_meters":4000,"lead_time_days":30},
    {"sku_code":"SKU-1044","product_name":"6.6kV Aluminium XLPE 185mm2 3-Core Armoured","voltage_kv":6.6,"conductor_material":"aluminium","cross_section_mm2":185,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":760.0,"stock_meters":4500,"lead_time_days":32},
    {"sku_code":"SKU-1045","product_name":"11kV Copper XLPE 300mm2 3-Core Armoured","voltage_kv":11.0,"conductor_material":"copper","cross_section_mm2":300,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":1500.0,"stock_meters":2000,"lead_time_days":45},
    {"sku_code":"SKU-1046","product_name":"1.1kV Aluminium XLPE 240mm2 3-Core Armoured","voltage_kv":1.1,"conductor_material":"aluminium","cross_section_mm2":240,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":195.0,"stock_meters":6000,"lead_time_days":21},
    {"sku_code":"SKU-1047","product_name":"3.3kV Aluminium XLPE 185mm2 3-Core Armoured","voltage_kv":3.3,"conductor_material":"aluminium","cross_section_mm2":185,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":460.0,"stock_meters":5000,"lead_time_days":28},
    {"sku_code":"SKU-1048","product_name":"22kV Copper XLPE 70mm2 3-Core Armoured","voltage_kv":22.0,"conductor_material":"copper","cross_section_mm2":70,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":1580.0,"stock_meters":2800,"lead_time_days":45},
    {"sku_code":"SKU-1049","product_name":"33kV Aluminium XLPE 185mm2 3-Core Armoured","voltage_kv":33.0,"conductor_material":"aluminium","cross_section_mm2":185,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":2900.0,"stock_meters":900,"lead_time_days":45},
    {"sku_code":"SKU-1050","product_name":"11kV Aluminium XLPE 300mm2 3-Core Armoured","voltage_kv":11.0,"conductor_material":"aluminium","cross_section_mm2":300,"insulation_type":"XLPE","armoured":True,"number_of_cores":3,"price_per_meter":1200.0,"stock_meters":1800,"lead_time_days":45},
]

def validate_fix_sku(sku: dict) -> dict | None:
    try:
        def validate_fix_sku(sku: dict) -> dict | None:
            if not isinstance(sku, dict):
                print(f"  SKU dropped — expected dict, got {type(sku).__name__}: {str(sku)[:60]}")
                return None
        sku["conductor_material"] = str(sku.get("conductor_material", "copper")).lower().strip()
        sku["insulation_type"]    = str(sku.get("insulation_type",    "XLPE")).upper().strip()
        sku["voltage_kv"]         = float(sku.get("voltage_kv",        1.1))
        sku["cross_section_mm2"]  = float(sku.get("cross_section_mm2", 35))
        sku["number_of_cores"]    = int(sku.get("number_of_cores",     1))
        sku["price_per_meter"]    = float(sku.get("price_per_meter",   200))
        sku["stock_meters"]       = int(sku.get("stock_meters",        5000))
        sku["lead_time_days"]     = int(sku.get("lead_time_days",      14))

        if sku["conductor_material"] not in VALID_CONDUCTORS:
            sku["conductor_material"] = "copper"
        if sku["insulation_type"] not in VALID_INSULATIONS:
            sku["insulation_type"] = "XLPE"
        if sku["voltage_kv"] not in VALID_VOLTAGES:
            sku["voltage_kv"] = min(VALID_VOLTAGES, key=lambda v: abs(v - sku["voltage_kv"]))
        if sku["cross_section_mm2"] not in VALID_SECTIONS:
            sku["cross_section_mm2"] = min(VALID_SECTIONS, key=lambda s: abs(s - sku["cross_section_mm2"]))
        if sku["number_of_cores"] not in VALID_CORES:
            sku["number_of_cores"] = 1
        # Physical reality: PVC not used for HV
        if sku["voltage_kv"] >= 11.0 and sku["insulation_type"] == "PVC":
            sku["insulation_type"] = "XLPE"
        # Price sanity (₹)
        if sku["price_per_meter"] < 50:
            sku["price_per_meter"] *= 100   # model gave dollar price, scale to INR
        if sku["price_per_meter"] <= 0:
            sku["price_per_meter"] = 200.0
        return sku
    except Exception as e:
        print(f"  SKU dropped — validation error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# PHASE 1 — HARDCODED REFERENCE DATA
# standards + tests (real IS/IEC data, no Ollama)
# ═══════════════════════════════════════════════════════════════

STANDARDS_DATA = [
    {
        "standard_code": "IS 7098-1",
        "standard_name": "XLPE Insulated Cables — Up to 1100V",
        "issuing_body":  "IS",
        "version":       "2011",
        "description":   "Specifies requirements for XLPE insulated PVC sheathed cables with aluminium or copper conductors for working voltages up to and including 1100V. Covers conductor resistance, insulation resistance, voltage test and physical properties of insulation and sheathing.",
    },
    {
        "standard_code": "IS 7098-2",
        "standard_name": "XLPE Insulated Cables — 3.3kV to 33kV",
        "issuing_body":  "IS",
        "version":       "2011",
        "description":   "Specifies requirements for XLPE insulated cables for working voltages from 3.3kV up to and including 33kV. Covers conductor construction, insulation thickness, metallic screen, armour and outer sheath requirements along with type, routine and sample tests.",
    },
    {
        "standard_code": "IS 1554-1",
        "standard_name": "PVC Insulated Cables — Up to 1100V",
        "issuing_body":  "IS",
        "version":       "1988",
        "description":   "Specifies requirements for PVC insulated and sheathed cables with stranded aluminium or copper conductors for working voltages up to and including 1100V. Specifies conductor resistance, insulation resistance, voltage test and heat resistance.",
    },
    {
        "standard_code": "IS 1554-2",
        "standard_name": "PVC Insulated Cables — 3.3kV to 11kV",
        "issuing_body":  "IS",
        "version":       "1988",
        "description":   "Specifies requirements for PVC insulated cables for voltages 3.3kV to 11kV. Covers insulation thickness, conductor screening, metallic sheath or armour, and routine plus type test requirements for medium voltage distribution cables.",
    },
    {
        "standard_code": "IEC 60502-1",
        "standard_name": "Power Cables — 1kV to 30kV, Part 1",
        "issuing_body":  "IEC",
        "version":       "2004",
        "description":   "International standard for extruded solid dielectric insulated power cables and accessories for rated voltages from 1kV up to 30kV. Defines conductor, insulation, screen, armour and sheath construction requirements with complete test schedules.",
    },
    {
        "standard_code": "IEC 60502-2",
        "standard_name": "Power Cables — 6kV to 30kV, Part 2",
        "issuing_body":  "IEC",
        "version":       "2005",
        "description":   "Specifies cables with extruded solid dielectric insulation for rated voltages from 6kV to 30kV. Includes requirements for conductor screening, insulation screening, metallic layer and outer sheath. Defines all type and routine test methods.",
    },
    {
        "standard_code": "BS 6622",
        "standard_name": "Cables with XLPE Insulation — 3.8kV to 19kV",
        "issuing_body":  "BS",
        "version":       "1999",
        "description":   "British standard for cables with extruded cross-linked polyethylene insulation for rated voltages from 3.8kV to 19kV. Covers armoured and unarmoured designs for copper and aluminium conductors with complete dimensional and electrical requirements.",
    },
    {
        "standard_code": "BS 7835",
        "standard_name": "Cables with XLPE Insulation — 22kV to 66kV",
        "issuing_body":  "BS",
        "version":       "1996",
        "description":   "British standard for high voltage cables with XLPE insulation rated 22kV to 66kV. Specifies conductor cross-sections, insulation thickness, metallic screen cross-section and outer sheath for both directly buried and duct-installed applications.",
    },
    {
        "standard_code": "IEC 60228",
        "standard_name": "Conductors of Insulated Cables",
        "issuing_body":  "IEC",
        "version":       "2004",
        "description":   "Defines classes of conductors for insulated cables. Covers solid and stranded circular conductors, shaped stranded conductors and flexible conductors in copper and aluminium. Specifies maximum resistance values at 20°C for each class and cross-section.",
    },
    {
        "standard_code": "IS 8130",
        "standard_name": "Conductors for Insulated Electric Cables",
        "issuing_body":  "IS",
        "version":       "2013",
        "description":   "Indian standard for conductors used in insulated electric cables and flexible cords. Specifies conductor classes, nominal cross-sectional areas and maximum DC resistance values at 20°C for aluminium and copper conductors used in power cables.",
    },
]

TESTS_DATA = [
    # test_name, category, test_type, description, standard_code
    ("High Voltage Test",              "electrical", "routine", "Applies AC or DC voltage significantly higher than working voltage for a specified duration to verify insulation integrity and detect manufacturing defects.", "IEC 60502-1"),
    ("Insulation Resistance Test",     "electrical", "routine", "Measures DC resistance of insulation between conductor and screen/sheath using a megohmmeter. Result must exceed minimum specified value at working temperature.", "IS 7098-1"),
    ("Conductor Resistance Test",      "electrical", "routine", "Measures DC resistance per unit length of conductor at 20°C and compares against maximum values specified in IEC 60228 or IS 8130 for the conductor class.", "IEC 60228"),
    ("Partial Discharge Test",         "electrical", "routine", "Detects localised electrical discharges within insulation that do not bridge conductors. Measured in picocoulombs at 1.73 times rated voltage. Critical for XLPE HV cables.", "IEC 60502-2"),
    ("Dielectric Loss Angle Test",     "electrical", "type",    "Measures tangent delta (tan δ) of insulation to assess dielectric quality. High values indicate insulation degradation or manufacturing defects in extruded insulation.", "IEC 60502-2"),
    ("Tensile Strength Test",          "mechanical", "type",    "Measures force required to break insulation or sheath material per unit area. Verifies polymer compound quality meets minimum elongation at break requirements.", "IS 7098-1"),
    ("Elongation at Break Test",       "mechanical", "type",    "Measures percentage stretch of insulation or sheath sample at point of fracture. Minimum elongation must be maintained before and after ageing.", "IS 1554-1"),
    ("Bend Test",                      "mechanical", "type",    "Bends the complete cable assembly around a mandrel of specified diameter at ambient temperature. Checks for cracking of insulation, sheath or armour after bending.", "IS 7098-2"),
    ("Armour Resistance Test",         "electrical", "routine", "Measures DC resistance per unit length of steel wire or tape armour. Verifies armour provides adequate earth fault return path per specified maximum resistance.", "IS 7098-2"),
    ("Hot Set Test",                   "thermal",    "type",    "Applies mechanical load to XLPE insulation sample at 200°C for 15 minutes and measures elongation under load and permanent set after cooling. Verifies degree of crosslinking.", "IEC 60502-1"),
    ("Thermal Stability Test",         "thermal",    "type",    "Heats PVC compound samples to assess resistance to thermal degradation. Checks colour change and mechanical property retention after oven ageing at specified temperature.", "IS 1554-1"),
    ("Smoke Density Test",             "thermal",    "sample",  "Measures optical density of smoke produced when cable burns. Relevant for cables in enclosed spaces, tunnels and buildings requiring low-smoke-emission properties.", "IEC 60502-1"),
    ("Fire Propagation Test",          "thermal",    "type",    "Assesses tendency of cable to spread fire when installed in bunches on cable trays. Cable must self-extinguish within specified time after ignition source removed.", "BS 6622"),
    ("Stripping Force Test",           "mechanical", "sample",  "Measures force required to strip insulation from conductor. Verifies adhesion is within specified range — too high makes jointing difficult, too low causes field failures.", "IS 8130"),
    ("Water Absorption Test",          "mechanical", "type",    "Immerses insulation samples in water at 70°C for specified days and measures change in electrical properties. Verifies insulation compound moisture resistance.", "IEC 60502-2"),
]


def phase1_reference_data(cur):
    print("\\n[Phase 1] Inserting standards and tests...")
    std_map = {}   # standard_code → standard_id (int)
    for s in STANDARDS_DATA:
        cur.execute("""
            INSERT INTO standards (standard_code, standard_name, issuing_body, description, version)
            VALUES (%(standard_code)s, %(standard_name)s, %(issuing_body)s, %(description)s, %(version)s)
            ON CONFLICT (standard_code) DO UPDATE
                SET standard_name = EXCLUDED.standard_name,
                    description   = EXCLUDED.description
            RETURNING standard_id
        """, s)
        sid = cur.fetchone()[0]
        std_map[s["standard_code"]] = sid
        print(f"  standard: {s['standard_code']} → id={sid}")

    test_map = {}  # test_name → test_id (int)
    for t in TESTS_DATA:
        name, category, ttype, desc, std_code = t
        cur.execute("""
            INSERT INTO tests (test_name, test_category, test_type, description, standard_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (test_name) DO UPDATE
                SET description = EXCLUDED.description
            RETURNING test_id
        """, (name, category, ttype, desc, std_map.get(std_code)))
        tid = cur.fetchone()[0]
        test_map[name] = tid

    print(f"  {len(std_map)} standards, {len(test_map)} tests inserted.")
    return std_map, test_map


# ═══════════════════════════════════════════════════════════════
# PHASE 2 — TENANT SETUP
# companies → company_config → users → client_profile
# ═══════════════════════════════════════════════════════════════

COMPANIES_DATA = [
    {
        "company_name":      "Polycab Wires Ltd",
        "subscription_tier": "pro",
        "config": {
            "auto_accept_threshold":  0.87,
            "hitl_trigger_threshold": 0.52,
            "default_margin_pct":     13.5,
            "max_active_rfps":        20,
            "tender_portal_urls":     json.dumps(["https://eprocure.gov.in", "https://gem.gov.in"]),
        },
    },
    {
        "company_name":      "Havells India Ltd",
        "subscription_tier": "enterprise",
        "config": {
            "auto_accept_threshold":  0.90,
            "hitl_trigger_threshold": 0.55,
            "default_margin_pct":     15.0,
            "max_active_rfps":        50,
            "tender_portal_urls":     json.dumps(["https://eprocure.gov.in", "https://gem.gov.in", "https://mahatenders.gov.in"]),
        },
    },
]

ROLES = [
    "admin", "sales_manager", "technical_manager",
    "pricing_manager", "sales_executive", "viewer", "agent",
]

CLIENT_NAMES = [
    ("L&T Construction",       "LSTK_contractor", "low",    "active"),
    ("Tata Projects",          "LSTK_contractor", "medium", "active"),
    ("KEC International",      "LSTK_contractor", "medium", "active"),
    ("Sterlite Power",         "private",         "low",    "active"),
    ("Kalpataru Power",        "private",         "high",   "active"),
    ("Power Grid Corporation", "PSU",             "low",    "active"),
    ("GMR Infrastructure",     "private",         "medium", "active"),
    ("Adani Electricity",      "private",         "low",    "active"),
    ("ONGC Ltd",               "PSU",             "medium", "prospect"),
    ("BHEL",                   "government",      "high",   "prospect"),
]


def phase2_tenant_setup(cur):
    print("\\n[Phase 2] Setting up tenants, users, clients...")
    companies = []

    for cd in COMPANIES_DATA:
        cur.execute("""
            INSERT INTO companies (company_name, industry_vertical, subscription_tier)
            VALUES (%s, 'cables', %s)
            RETURNING company_id
        """, (cd["company_name"], cd["subscription_tier"]))
        cid = cur.fetchone()[0]

        cfg = cd["config"]
        cur.execute("""
            INSERT INTO company_config
              (company_id, auto_accept_threshold, hitl_trigger_threshold,
               default_margin_pct, max_active_rfps, tender_portal_urls)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (cid, cfg["auto_accept_threshold"], cfg["hitl_trigger_threshold"],
              cfg["default_margin_pct"], cfg["max_active_rfps"],
              cfg["tender_portal_urls"]))

        company = {"company_id": cid, "company_name": cd["company_name"],
                   "users": {}, "clients": []}
        domain  = cd["company_name"].lower().replace(" ", "").replace(".", "")

        for role in ROLES:
            cur.execute("""
                INSERT INTO users (company_id, email, full_name, role)
                VALUES (%s, %s, %s, %s)
                RETURNING user_id
            """, (cid, f"{role}@{domain}.com", fake.name(), role))
            company["users"][role] = cur.fetchone()[0]

        for cname, ctype, price_sens, rel_status in CLIENT_NAMES:
            cur.execute("""
                INSERT INTO client_profile
                  (company_id, client_name, client_type, sector,
                   price_sensitivity, relationship_status)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING client_id
            """, (cid, cname, ctype,
                  random.choice(["Power", "Infrastructure", "Oil & Gas", "Railways"]),
                  price_sens, rel_status))
            company["clients"].append({
                "client_id":   cur.fetchone()[0],
                "client_name": cname,
                "price_sensitivity": price_sens,
            })

        companies.append(company)
        print(f"  Company: {cd['company_name']} → {cid}")

    return companies


# ═══════════════════════════════════════════════════════════════
# PHASE 3 — PRODUCT CATALOG (Ollama)
# product_catalog → product_standards → client_preferred_standards
# → market_pricing (derived from catalog prices)
# ═══════════════════════════════════════════════════════════════

SKU_PROMPT = """Generate exactly 10 realistic cable product SKUs for an Indian cable manufacturer.
SKU codes must be exactly SKU-{start:04d} through SKU-{end:04d}.
Return ONLY a JSON array of exactly 10 objects. No explanation. No markdown. Just the raw array.

Each object must have exactly these fields:
- "sku_code": string, one of SKU-{start:04d} to SKU-{end:04d}
- "product_name": string describing the cable
- "voltage_kv": number, MUST be exactly one of: 1.1, 3.3, 6.6, 11.0, 22.0, 33.0
- "conductor_material": string, MUST be exactly "copper" or "aluminium"
- "cross_section_mm2": number, MUST be exactly one of: 10,16,25,35,50,70,95,120,150,185,240,300
- "insulation_type": string, MUST be exactly "XLPE", "PVC", "EPR", or "HEPR"
- "armoured": boolean
- "number_of_cores": integer, MUST be exactly 1, 2, 3, or 4
- "price_per_meter": number in Indian Rupees
- "stock_meters": integer between 1000 and 50000
- "lead_time_days": integer between 7 and 45

Rules:
- voltage >= 11.0 MUST use XLPE, EPR, or HEPR (NOT PVC)
- Aluminium is 25% cheaper than copper at same spec
- Mix voltages and conductors across the 10 items"""


def phase3_catalog(cur, companies, std_map):
    print("\\n[Phase 3] Generating product catalog via Ollama...")

    # Standard code → standard_id mapping for bridge table
    # Map the common standard codes in our system to their IDs
    catalog_std_codes = {
        "IS 7098-1": std_map.get("IS 7098-1"),
        "IS 7098-2": std_map.get("IS 7098-2"),
        "IS 1554-1": std_map.get("IS 1554-1"),
        "IS 1554-2": std_map.get("IS 1554-2"),
        "IEC 60502-1": std_map.get("IEC 60502-1"),
        "IEC 60502-2": std_map.get("IEC 60502-2"),
        "BS 6622":    std_map.get("BS 6622"),
    }
    # Remove any None values (standards not in our seed)
    catalog_std_codes = {k: v for k, v in catalog_std_codes.items() if v}

    def assign_standards_to_sku(sku):
        """Pick realistic standards for a SKU based on its voltage."""
        stds = []
        v = sku["voltage_kv"]
        ins = sku["insulation_type"]
        if v <= 1.1:
            stds = ["IS 7098-1", "IS 1554-1", "IEC 60502-1"] if ins == "XLPE" else ["IS 1554-1", "IEC 60502-1"]
        elif v <= 6.6:
            stds = ["IS 7098-2", "IS 1554-2", "IEC 60502-1"]
        elif v <= 11.0:
            stds = ["IS 7098-2", "IEC 60502-1", "IEC 60502-2"]
        else:
            stds = ["IS 7098-2", "IEC 60502-2", "BS 6622"]
        # Pick 1-3 of the applicable standards that exist in our DB
        applicable = [s for s in stds if s in catalog_std_codes]
        return random.sample(applicable, k=min(random.randint(1, 3), len(applicable)))

    print("  Calling Ollama in 5 batches of 10 (this may take 2-4 minutes)...")
    all_raw_skus = []
    for batch_num in range(5):
        start_code = 1001 + batch_num * 10
        end_code   = start_code + 9
        prompt = SKU_PROMPT.format(start=start_code, end=end_code)
        try:
            batch = call_ollama(prompt)
            if isinstance(batch, list):
                all_raw_skus.extend(batch)
                print(f"  Batch {batch_num+1}/5 (SKU-{start_code:04d}–SKU-{end_code:04d}): {len(batch)} items")
            else:
                print(f"  Batch {batch_num+1}/5: unexpected response type {type(batch)}, skipping")
        except Exception as e:
            print(f"  Batch {batch_num+1}/5 failed: {e}, skipping")

    raw_skus = all_raw_skus
    if not isinstance(raw_skus, list):
        raw_skus = list(raw_skus.values())[0] if isinstance(raw_skus, dict) else []

    print(f"  Ollama returned {len(raw_skus)} SKUs, validating...")
    validated = [validate_fix_sku(sku) for sku in raw_skus]
    validated = [s for s in validated if s is not None]
    if len(validated) < 5:
        print(f"  ⚠ Only {len(validated)} SKUs from Ollama — using hardcoded fallback SKUs")
    validated = FALLBACK_SKUS

    print(f"  {len(validated)} SKUs passed validation")

    all_std_ids = list(catalog_std_codes.values())

    for company in companies:
        company["skus"] = []
        inserted_codes = set()

        for i, sku in enumerate(validated):
            sku_code = f"{sku.get('sku_code', f'SKU-{1001+i}')}-{company['company_name'][:3].upper()}"
            if sku_code in inserted_codes:
                sku_code = f"{sku_code}-{i}"
            inserted_codes.add(sku_code)

            product_name = sku.get("product_name",
                f"{sku['voltage_kv']}kV {sku['conductor_material'].title()} "
                f"{sku['cross_section_mm2']}mm² {sku['insulation_type']} Cable")

            cur.execute("""
                INSERT INTO product_catalog
                  (company_id, sku_code, product_name, voltage_kv,
                   conductor_material, cross_section_mm2, insulation_type,
                   armoured, number_of_cores, price_per_meter,
                   stock_meters, lead_time_days)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING sku_id
            """, (
                company["company_id"], sku_code, product_name,
                sku["voltage_kv"], sku["conductor_material"],
                sku["cross_section_mm2"], sku["insulation_type"],
                sku.get("armoured", False), sku["number_of_cores"],
                sku["price_per_meter"],
                sku.get("stock_meters", 5000), sku.get("lead_time_days", 14),
            ))
            sku_id = cur.fetchone()[0]

            sku_record = {**sku, "sku_id": sku_id, "sku_code": sku_code}
            company["skus"].append(sku_record)

            # product_standards bridge
            assigned_std_codes = assign_standards_to_sku(sku)
            for std_code in assigned_std_codes:
                cur.execute("""
                    INSERT INTO product_standards (sku_id, standard_id)
                    VALUES (%s, %s) ON CONFLICT DO NOTHING
                """, (sku_id, catalog_std_codes[std_code]))

        # client_preferred_standards: each client prefers 1-3 standards
        for client in company["clients"]:
            preferred_std_ids = random.sample(all_std_ids, k=random.randint(1, 3))
            for sid in preferred_std_ids:
                cur.execute("""
                    INSERT INTO client_preferred_standards (client_id, standard_id)
                    VALUES (%s, %s) ON CONFLICT DO NOTHING
                """, (client["client_id"], sid))

        # market_pricing: derived from catalog prices per dimension combo
        seen_dims = set()
        for sku in company["skus"]:
            dim_key = (sku["voltage_kv"], sku["conductor_material"], sku["cross_section_mm2"])
            if dim_key in seen_dims:
                continue
            seen_dims.add(dim_key)

            base = sku["price_per_meter"]
            assigned_std_codes = assign_standards_to_sku(sku)
            std_id = catalog_std_codes.get(assigned_std_codes[0]) if assigned_std_codes else None

            # cur.execute("""
            #     INSERT INTO market_pricing
            #       (product_category, voltage_kv, conductor_material,
            #        cross_section_mm2, standard_id,
            #        min_market_price, max_market_price, avg_market_price, sample_size)
            #     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            #     ON CONFLICT (product_category, voltage_kv, conductor_material,
            #                  cross_section_mm2, standard_id)
            #     DO UPDATE SET
            #         avg_market_price = (market_pricing.avg_market_price + EXCLUDED.avg_market_price) / 2,
            #         last_updated     = NOW()
            # """, (
            #     f"{sku['voltage_kv']}kV_CABLE",
            #     sku["voltage_kv"], sku["conductor_material"], sku["cross_section_mm2"],
            #     std_id,
            #     round(base * 0.85, 2),
            #     round(base * 1.15, 2),
            #     round(base, 2),
            #     random.randint(8, 25),
            # ))
            
            pricing_args = (
                f"{sku['voltage_kv']}kV CABLE", sku["voltage_kv"], sku["conductor_material"],
                sku["cross_section_mm2"], std_id,
                round(base * 0.85, 2), round(base * 1.15, 2), round(base, 2),
                random.randint(8, 25),
            )
            if std_id is not None:
                cur.execute("""
                    INSERT INTO market_pricing
                        (product_category, voltage_kv, conductor_material, cross_section_mm2, standard_id,
                        min_market_price, max_market_price, avg_market_price, sample_size)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (product_category, voltage_kv, conductor_material, cross_section_mm2, standard_id)
                    DO UPDATE SET
                        avg_market_price = (market_pricing.avg_market_price + EXCLUDED.avg_market_price) / 2,
                        last_updated = NOW()
                """, pricing_args)
            else:
                cur.execute("""
                    INSERT INTO market_pricing
                        (product_category, voltage_kv, conductor_material, cross_section_mm2, standard_id,
                        min_market_price, max_market_price, avg_market_price, sample_size)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, pricing_args)

        print(f"  {company['company_name']}: {len(company['skus'])} SKUs, market_pricing derived")


# ═══════════════════════════════════════════════════════════════
# PHASE 4 — HISTORICAL PIPELINE DATA
# 30 completed (submitted) RFPs per company — backbone for
# product_performance, bid_history and pricing intelligence
# ═══════════════════════════════════════════════════════════════

def insert_rfp_status_history(cur, rfp_id, changed_by_id=None, is_agent=True, max_status="submitted"):
    all_transitions = [("new","processing"), ("processing","matched"), ("matched","submitted")]
    stop_at = {"processing": 1, "matched": 2, "submitted": 3}
    transitions = all_transitions[:stop_at.get(max_status, 3)]
    
    by_type = "agent" if is_agent else "user"
    for i, (old, new) in enumerate(transitions):
        cur.execute("""
            INSERT INTO rfp_status_history
              (rfp_id, old_status, new_status, changed_by_type, changed_by_id,
               changed_at)
            VALUES (%s,%s,%s,%s,%s, NOW() - INTERVAL %s)
        """, (rfp_id, old, new, by_type,
              None if is_agent else changed_by_id,
              f"{(3 - i) * 2} hours"))


def phase4_historical(cur, companies, std_map, test_map):
    print("\n[Phase 4] Inserting 30 historical (submitted) RFPs per company...")
    all_std_ids  = list(std_map.values())
    all_test_ids = list(test_map.values())

    for company in companies:
        skus    = company["skus"]
        clients = company["clients"]
        users   = company["users"]
        cid     = company["company_id"]
        company["historical_rfps"] = []

        for i in range(30):
            client = random.choice(clients)
            days_ago = random.randint(30, 365)
            result   = random.choices(["won", "lost"], weights=[0.45, 0.55])[0]

            # rfp_document
            cur.execute("""
                INSERT INTO rfp_documents
                  (company_id, issuer_client_id, tender_ref, project_name,
                   deadline, status, relevance_label, document_path, created_at)
                VALUES (%s,%s,%s,%s,%s,'submitted','high',%s,
                        NOW() - INTERVAL %s)
                RETURNING rfp_id
            """, (
                cid, client["client_id"],
                f"HIST/{random.choice(IN_STATES)}/ELEC/2024/{str(i+1).zfill(3)}",
                f"{fake.city()} Grid Cabling Project",
                now_minus(days=days_ago - 5),
                f"s3://rfpilot/rfps/hist_{i+1}_{cid}.pdf",
                f"{days_ago} days",
            ))
            rfp_id = cur.fetchone()[0]

            # MULTI-ITEM LOGIC STARTS HERE
            num_items = random.randint(1, 4)
            item_records = []

            for item_no in range(1, num_items + 1):
                item_sku = random.choice(skus)
                quantity = random.randint(500, 5000)

                cur.execute("""
                    INSERT INTO rfp_items
                      (rfp_id, item_no, voltage_kv, conductor_material,
                       cross_section_mm2, insulation_type, armoured,
                       number_of_cores, quantity, unit)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'meters')
                    RETURNING item_id
                """, (
                    rfp_id, item_no,
                    item_sku["voltage_kv"], item_sku["conductor_material"],
                    item_sku["cross_section_mm2"], item_sku["insulation_type"],
                    item_sku.get("armoured", False),
                    item_sku["number_of_cores"],
                    quantity,
                ))

                item_id = cur.fetchone()[0]
                item_records.append((item_id, item_sku, quantity))

                # standards (1–2)
                for sid in random.sample(all_std_ids, k=random.randint(1, 2)):
                    cur.execute(
                        "INSERT INTO rfp_item_standards VALUES (%s,%s) ON CONFLICT DO NOTHING",
                        (item_id, sid)
                    )

                # tests (1–3)
                for tid in random.sample(all_test_ids, k=random.randint(1, 3)):
                    cur.execute(
                        "INSERT INTO rfp_item_tests VALUES (%s,%s) ON CONFLICT DO NOTHING",
                        (item_id, tid)
                    )

            # LOOP OVER ITEMS FOR MATCH + PRICING
            for item_id, item_sku, quantity in item_records:
                rrf = round(random.uniform(0.87, 0.96), 4)

                # product_match
                cur.execute("""
                    INSERT INTO product_matches
                      (rfp_item_id, sku_id, company_id, bm25_score,
                       semantic_score, rrf_score, llm_rerank_score,
                       match_rank, is_selected, match_status,
                       reviewed_by, reviewed_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,1,TRUE,'auto_accepted',NULL,NULL)
                    RETURNING match_id
                """, (
                    item_id, item_sku["sku_id"], cid,
                    round(random.uniform(0.80, 0.95), 4),
                    round(random.uniform(0.82, 0.96), 4),
                    rrf,
                    round(rrf + random.uniform(0.01, 0.03), 4),
                ))

                # pricing_calculation
                base   = float(item_sku["price_per_meter"])
                margin = round(random.uniform(10.0, 18.0), 2)
                total  = round(base * quantity * (1 + margin / 100), 2)

                cur.execute("""
                    INSERT INTO pricing_calculations
                      (rfp_item_id, sku_id, company_id, quantity,
                       base_price, material_cost, margin,
                       total_bid_price, pricing_status, human_edited,
                       approved_by, approved_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'approved',FALSE,%s,
                            NOW() - INTERVAL %s)
                """, (
                    item_id, item_sku["sku_id"], cid, quantity,
                    base, round(base * quantity, 2), margin,
                    total, users["pricing_manager"],
                    f"{days_ago - 1} days",
                ))
                
            # response_document (submitted)
            cur.execute("""
                INSERT INTO response_documents
                  (rfp_id, company_id, version, generated_document_path,
                   status, approved_by, approved_at, submitted_by)
                VALUES (%s,%s,1,%s,'submitted',%s,
                        NOW() - INTERVAL %s, %s)
            """, (
                rfp_id, cid,
                f"s3://rfpilot/responses/resp_{rfp_id}.docx",
                users["sales_manager"],
                f"{days_ago - 1} days",
                users["sales_executive"],
            ))

            # bid_history — record actual outcome
            # cur.execute("""
            #     INSERT INTO bid_history
            #       (rfp_id, company_id, client_id, sku_id,
            #        quantity, bid_price, result,
            #        win_reason, loss_reason, contract_value,
            #        submitted_at)
            #     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            #             NOW() - INTERVAL %s)
            # """, (
            #     rfp_id, cid, client["client_id"], sku["sku_id"],
            #     quantity, total, result,
            #     "competitive_price" if result == "won" else None,
            #     random.choice(["PRICE_HIGH", "COMPETITOR_PREFERRED"]) if result == "lost" else None,
            #     total if result == "won" else None,
            #     f"{days_ago} days",
            # ))
            
            cur.execute("""INSERT INTO bid_history
    (rfp_id, company_id, client_id, result,
     loss_reason, match_score_at_submission,
     response_time_days, contract_value, submitted_at)
    VALUES (%s,%s,%s,%s, %s,%s, %s,%s, NOW() - INTERVAL %s)""",
    (
        rfp_id, cid, client["client_id"], result,
        random.choice(["price_too_high", "competitor_preferred"]) if result == "lost" else None,
        rrf,                           # match_score_at_submission (already computed above)
        random.randint(1, 5),          # response_time_days — NOT NULL, was completely missing
        total if result == "won" else None,
        f"{days_ago} days",
    ))

            # rfp_status_history (audit trail)
            insert_rfp_status_history(cur, rfp_id, is_agent=True)
            # agent_execution_logs for sales + technical agent
            for agent_name, task_type in [
                ("SalesAgent",     "rfp_classification"),
                ("TechnicalAgent", "matching"),
                ("PricingAgent",   "pricing"),
            ]:
                cur.execute("""
                    INSERT INTO agent_execution_logs
                      (rfp_id, company_id, agent_name, task_type,
                       execution_time_ms, status, created_at)
                    VALUES (%s,%s,%s,%s,%s,'success', NOW() - INTERVAL %s)
                """, (
                    rfp_id, cid, agent_name, task_type,
                    random.randint(800, 8000),
                    f"{days_ago} days",
                ))

            company["historical_rfps"].append(rfp_id)

        print(f"  {company['company_name']}: 30 historical RFPs inserted")

    # product_performance: aggregate from bid_history using SQL
    print("  Aggregating product_performance from bid_history...")
    for company in companies:
        cur.execute("""
    INSERT INTO product_performance
        (sku_id, client_id, company_id, total_orders, complaint_count, avg_delivery_delay_days, last_updated)
    SELECT
        pm.sku_id,
        bh.client_id,
        bh.company_id,
        COUNT(*)                                                          AS total_orders,
        SUM(CASE WHEN bh.loss_reason = 'wrong_spec' THEN 1 ELSE 0 END)  AS complaint_count,
        ROUND(AVG(RANDOM() * 5)::NUMERIC, 2)                             AS avg_delivery_delay,
        NOW()
    FROM bid_history bh
    JOIN rfp_items ri        ON ri.rfp_id       = bh.rfp_id
    JOIN product_matches pm  ON pm.rfp_item_id  = ri.item_id
                             AND pm.is_selected  = TRUE
    WHERE bh.company_id = %s
    GROUP BY pm.sku_id, bh.client_id, bh.company_id
    ON CONFLICT (sku_id, client_id) DO UPDATE SET
        total_orders    = EXCLUDED.total_orders,
        complaint_count = EXCLUDED.complaint_count,
        last_updated    = NOW()
""", (company["company_id"],))
    print("  product_performance populated.")

        

# ═══════════════════════════════════════════════════════════════
# PHASE 5 — TEST PIPELINE DATA (3 Scenarios)
# Deliberately designed RFPs to test every agent decision path
# ═══════════════════════════════════════════════════════════════

def phase5_test_pipeline(cur, companies, std_map, test_map):
    print("\\n[Phase 5] Inserting test pipeline RFPs (3 scenarios)...")
    all_std_ids  = list(std_map.values())
    all_test_ids = list(test_map.values())

    for company in companies:
        skus    = company["skus"]
        clients = company["clients"]
        users   = company["users"]
        cid     = company["company_id"]

        # ── SCENARIO A: HIGH MATCH (8 RFPs) ────────────────────
        # rfp_item specs = exactly a catalog SKU → auto_accepted
        print(f"  {company['company_name']} — Scenario A: 8 high-match RFPs")
        for i in range(8):
            sku    = random.choice([s for s in skus if s.get("stock_meters", 0) > 1000])
            client = random.choice(clients)

            cur.execute("""
                INSERT INTO rfp_documents
                  (company_id, issuer_client_id, tender_ref, project_name,
                   deadline, status, relevance_label, document_path)
                VALUES (%s,%s,%s,%s,%s,'processing','high',%s)
                RETURNING rfp_id
            """, (
                cid, client["client_id"],
                f"TEST/HM/{2026}/{str(i+1).zfill(3)}",
                f"{fake.city()} Substation Cabling — High Match Test {i+1}",
                now_minus(days=-(random.randint(15, 30))),  # future deadline
                f"s3://rfpilot/rfps/test_hm_{i+1}.pdf",
            ))
            rfp_id = cur.fetchone()[0]

            quantity = random.randint(1000, 4000)
            cur.execute("""
                INSERT INTO rfp_items
                  (rfp_id, item_no, voltage_kv, conductor_material,
                   cross_section_mm2, insulation_type, armoured,
                   number_of_cores, quantity, unit)
                VALUES (%s,1,%s,%s,%s,%s,%s,%s,%s,'meters')
                RETURNING item_id
            """, (
                rfp_id,
                # EXACT match with catalog SKU
                sku["voltage_kv"], sku["conductor_material"],
                sku["cross_section_mm2"], sku["insulation_type"],
                sku.get("armoured", False), sku["number_of_cores"],
                quantity,
            ))
            item_id = cur.fetchone()[0]

            for sid in random.sample(all_std_ids, 2):
                cur.execute("INSERT INTO rfp_item_standards VALUES (%s,%s) ON CONFLICT DO NOTHING", (item_id, sid))
            for tid in random.sample(all_test_ids, 2):
                cur.execute("INSERT INTO rfp_item_tests    VALUES (%s,%s) ON CONFLICT DO NOTHING", (item_id, tid))

            # product_match: auto_accepted (agent already ran)
            rrf = round(random.uniform(0.88, 0.97), 4)
            cur.execute("""
                INSERT INTO product_matches
                  (rfp_item_id, sku_id, company_id, bm25_score,
                   semantic_score, rrf_score, llm_rerank_score,
                   match_rank, is_selected, match_status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,1,TRUE,'auto_accepted')
            """, (
                item_id, sku["sku_id"], cid,
                round(random.uniform(0.85, 0.95), 4),
                round(random.uniform(0.87, 0.96), 4),
                rrf, round(rrf + 0.02, 4),
            ))

            # pricing_calculation: pending_approval (awaiting Pricing Manager)
            base  = float(sku["price_per_meter"])
            total = round(base * quantity * 1.13, 2)
            cur.execute("""
                INSERT INTO pricing_calculations
                  (rfp_item_id, sku_id, company_id, quantity, base_price,
                   material_cost, margin, total_bid_price,
                   pricing_status, human_edited)
                VALUES (%s,%s,%s,%s,%s,%s,13.0,%s,'pending_approval',FALSE)
            """, (item_id, sku["sku_id"], cid, quantity,
                  base, round(base * quantity, 2), total))

            insert_rfp_status_history(cur, rfp_id, is_agent=True, max_status="processing")
            # notification for pricing_manager
            cur.execute("""
                INSERT INTO notifications
                  (company_id, user_id, rfp_id, type, message, metadata)
                VALUES (%s,%s,%s,'pricing_approval',
                        'Pricing ready for approval — auto-matched RFP',
                        %s)
            """, (cid, users["pricing_manager"], rfp_id,
                  json.dumps({"rfp_id": str(rfp_id), "total_bid_price": total})))

        # ── SCENARIO B: PARTIAL MATCH (5 RFPs) ────────────────
        # ONE field differs from catalog → pending_review for Technical Manager
        print(f"  {company['company_name']} — Scenario B: 5 partial-match RFPs")
        for i in range(5):
            sku    = random.choice(skus)
            client = random.choice(clients)

            # Flip conductor material — one mismatch
            mismatched_conductor = "aluminium" if sku["conductor_material"] == "copper" else "copper"

            cur.execute("""
                INSERT INTO rfp_documents
                  (company_id, issuer_client_id, tender_ref, project_name,
                   deadline, status, relevance_label, document_path)
                VALUES (%s,%s,%s,%s,%s,'processing','medium',%s)
                RETURNING rfp_id
            """, (
                cid, client["client_id"],
                f"TEST/PM/{2026}/{str(i+1).zfill(3)}",
                f"{fake.city()} Industrial Cabling — Partial Match Test {i+1}",
                now_minus(days=-(random.randint(10, 25))),
                f"s3://rfpilot/rfps/test_pm_{i+1}.pdf",
            ))
            rfp_id = cur.fetchone()[0]

            quantity = random.randint(500, 3000)
            cur.execute("""
                INSERT INTO rfp_items
                  (rfp_id, item_no, voltage_kv, conductor_material,
                   cross_section_mm2, insulation_type, armoured,
                   number_of_cores, quantity, unit)
                VALUES (%s,1,%s,%s,%s,%s,%s,%s,%s,'meters')
                RETURNING item_id
            """, (
                rfp_id,
                sku["voltage_kv"], mismatched_conductor,  # ← mismatch
                sku["cross_section_mm2"], sku["insulation_type"],
                sku.get("armoured", False), sku["number_of_cores"],
                quantity,
            ))
            item_id = cur.fetchone()[0]

            for sid in random.sample(all_std_ids, 2):
                cur.execute("INSERT INTO rfp_item_standards VALUES (%s,%s) ON CONFLICT DO NOTHING", (item_id, sid))

            # product_match: pending_review (score 0.60–0.84)
            rrf = round(random.uniform(0.60, 0.84), 4)
            cur.execute("""
                INSERT INTO product_matches
                  (rfp_item_id, sku_id, company_id, bm25_score,
                   semantic_score, rrf_score, match_rank,
                   is_selected, match_status)
                VALUES (%s,%s,%s,%s,%s,%s,1,FALSE,'pending_review')
            """, (
                item_id, sku["sku_id"], cid,
                round(random.uniform(0.55, 0.80), 4),
                round(random.uniform(0.58, 0.82), 4),
                rrf,
            ))

            insert_rfp_status_history(cur, rfp_id, is_agent=True, max_status="processing")
            # notification for technical_manager
            cur.execute("""
                INSERT INTO notifications
                  (company_id, user_id, rfp_id, type, message, metadata)
                VALUES (%s,%s,%s,'match_review',
                        'Partial match requires your review before pricing',
                        %s)
            """, (cid, users["technical_manager"], rfp_id,
                  json.dumps({"rfp_id": str(rfp_id), "rrf_score": rrf,
                              "mismatch_field": "conductor_material"})))

        # ── SCENARIO C: HITL — NO MATCH (2 RFPs) ─────────────
        # Spec that no SKU in catalog covers → HITL triggered
        print(f"  {company['company_name']} — Scenario C: 2 HITL RFPs")
        hitl_specs = [
            # 33kV 4-core HEPR 300mm² — unlikely to be in catalog
            {"voltage_kv": 33.0, "conductor_material": "copper",
             "cross_section_mm2": 300.0, "insulation_type": "HEPR",
             "armoured": True, "number_of_cores": 4},
            # 22kV 4-core EPR 240mm² aluminium — rare combination
            {"voltage_kv": 22.0, "conductor_material": "aluminium",
             "cross_section_mm2": 240.0, "insulation_type": "EPR",
             "armoured": True, "number_of_cores": 4},
        ]

        for i, spec in enumerate(hitl_specs):
            client = random.choice(clients)

            cur.execute("""
                INSERT INTO rfp_documents
                  (company_id, issuer_client_id, tender_ref, project_name,
                   deadline, status, relevance_label, document_path)
                VALUES (%s,%s,%s,%s,%s,'processing','high',%s)
                RETURNING rfp_id
            """, (
                cid, client["client_id"],
                f"TEST/HITL/{2026}/{str(i+1).zfill(3)}",
                f"{fake.city()} EHV Transmission — HITL Test {i+1}",
                now_minus(days=-(random.randint(20, 40))),
                f"s3://rfpilot/rfps/test_hitl_{i+1}.pdf",
            ))
            rfp_id = cur.fetchone()[0]

            quantity = random.randint(200, 1000)
            cur.execute("""
                INSERT INTO rfp_items
                  (rfp_id, item_no, voltage_kv, conductor_material,
                   cross_section_mm2, insulation_type, armoured,
                   number_of_cores, quantity, unit)
                VALUES (%s,1,%s,%s,%s,%s,%s,%s,%s,'meters')
                RETURNING item_id
            """, (
                rfp_id,
                spec["voltage_kv"], spec["conductor_material"],
                spec["cross_section_mm2"], spec["insulation_type"],
                spec["armoured"], spec["number_of_cores"],
                quantity,
            ))
            item_id = cur.fetchone()[0]

            for sid in random.sample(all_std_ids, 2):
                cur.execute("INSERT INTO rfp_item_standards VALUES (%s,%s) ON CONFLICT DO NOTHING", (item_id, sid))

            # Closest SKU — find the nearest by voltage
            closest = min(skus, key=lambda s: abs(s["voltage_kv"] - spec["voltage_kv"]))

            # product_match: hitl_triggered (score < 0.50)
            rrf = round(random.uniform(0.20, 0.49), 4)
            cur.execute("""
                INSERT INTO product_matches
                  (rfp_item_id, sku_id, company_id, bm25_score,
                   semantic_score, rrf_score, match_rank,
                   is_selected, match_status)
                VALUES (%s,%s,%s,%s,%s,%s,1,FALSE,'hitl_triggered')
            """, (
                item_id, closest["sku_id"], cid,
                round(random.uniform(0.15, 0.45), 4),
                round(random.uniform(0.18, 0.47), 4),
                rrf,
            ))

            # custom_product_request (the HITL record)
            gap = (f"RFP requires {spec['voltage_kv']}kV {spec['conductor_material']} "
                   f"{spec['cross_section_mm2']}mm² {spec['insulation_type']} "
                   f"{spec['number_of_cores']}-core cable. "
                   f"Closest catalog SKU is {closest['sku_code']} "
                   f"({closest['voltage_kv']}kV {closest['conductor_material']} "
                   f"{closest['insulation_type']}). "
                   f"Primary gap: insulation type mismatch and cross-section unavailable.")

            cur.execute("""
                INSERT INTO custom_product_requests
                  (rfp_id, rfp_item_id, company_id, closest_sku_id,
                   gap_analysis, requested_by, assigned_to, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'pending')
            """, (
                rfp_id, item_id, cid, closest["sku_id"],
                gap, users["agent"], users["technical_manager"],
            ))

            insert_rfp_status_history(cur, rfp_id, is_agent=True, max_status="processing")
            # notification for technical_manager (HITL)
            cur.execute("""
                INSERT INTO notifications
                  (company_id, user_id, rfp_id, type, message, metadata)
                VALUES (%s,%s,%s,'hitl_required',
                        'No catalog match found — manual product review required',
                        %s)
            """, (cid, users["technical_manager"], rfp_id,
                  json.dumps({"rfp_id": str(rfp_id), "rrf_score": rrf,
                              "spec": spec})))

    print("  Test pipeline complete.")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    print("=== RFPilot Seed Data Script ===")
    conn = get_conn()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            print("\n[Pre-flight] Wiping existing seed data...")
            cur.execute("""
                TRUNCATE TABLE
                    standards,
                    companies
                RESTART IDENTITY CASCADE
            """)
            conn.commit()
            print("  All tables cleared.\n")
            
            std_map, test_map = phase1_reference_data(cur)
            conn.commit()

            companies = phase2_tenant_setup(cur)
            conn.commit()

            phase3_catalog(cur, companies, std_map)
            conn.commit()

            phase4_historical(cur, companies, std_map, test_map)
            conn.commit()

            phase5_test_pipeline(cur, companies, std_map, test_map)
            conn.commit()

        print("\\n=== Seed complete ===")
        for c in companies:
            print(f"  {c['company_name']}: {len(c['skus'])} SKUs, "
                  f"{len(c['historical_rfps'])} historical RFPs")

    except Exception as e:
        conn.rollback()
        print(f"\\n[ERROR] Rolling back. {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

