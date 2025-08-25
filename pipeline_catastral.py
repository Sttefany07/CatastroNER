# -*- coding: utf-8 -*-
"""
Pipeline NER -> Limpieza -> Normalización/Validación -> JSON (+ FastAPI)
Autor: STTEFANY

Requisitos (instalar):
  pip install spacy fastapi uvicorn pydantic[dotenv] openpyxl assemblyai

Ejecución de API:
  uvicorn pipeline_catastral:app --reload --host 0.0.0.0 --port 8000

Notas:
- Coloca tu modelo spaCy en MODEL_PATH (directorio con meta.json).
- Coloca 'ubigeo.xlsx' o 'ubigeo.csv' junto a este archivo (cabeceras detectables).
"""

import re
import os
import json
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
import csv
import spacy
import unicodedata
from pathlib import Path
from pydantic import BaseModel
from fastapi import FastAPI, UploadFile, File, HTTPException
from transcriber import transcribe_audio

# =========================
# 0) CONFIGURACIÓN / MAPEOS
# =========================

# Cambia a la ruta de tu modelo entrenado
MODEL_PATH = "model-last-tuned"

# Mapeos estándar de etiquetas
# Izquierda: label de tu modelo | Derecha: clave estándar del JSON final
LABEL_MAP = {
    # Bloque 0 - Identificación
    "NUMERO_FICHA": "NUMERO_FICHA",
    "CODIGO_CATASTRAL": "CODIGO_UNICO_CATASTRAL",
    "CODIGO_REFERENCIA_CATASTRAL": "CODIGO_REFERENCIA_CATASTRAL",
    "UBIGEO_DEPARTAMENTO": "DEPARTAMENTO",
    "UBIGEO_PROVINCIA": "PROVINCIA",
    "UBIGEO_DISTRITO": "DISTRITO",
    "SECTOR": "SECTOR",
    "MANZANA": "MANZANA",
    "LOTE": "LOTE",
    "EDIFICA": "EDIFICA",
    "ENTRADA": "ENTRADA",
    "PISO": "PISO",
    "UNIDAD": "UNIDAD",
    "DC": "DC",
    "CODIGO_CONTRIBUYENTE": "CODIGO_CONTRIBUYENTE",
    "CODIGO_PREDIAL": "CODIGO_PREDIAL",

    # I. Ubicación del predio
    "CODIGO_VIA": "CODIGO_VIA",
    "TIPO_VIA": "TIPO_VIA",
    "NOMBRE_VIA": "NOMBRE_VIA",
    "TIPO_PUERTA": "TIPO_PUERTA",
    "NUMERO_MUNICIPAL": "NUMERO_MUNICIPAL",
    "CONDICION_NUMERACION": "CONDICION_NUMERACION",
    "TIPO_EDIFICACION": "TIPO_EDIFICACION",
    "TIPO_INTERIOR": "TIPO_INTERIOR",
    "NUMERO_INTERIOR": "NUMERO_INTERIOR",
    "CODIGO_HU": "CODIGO_HU",
    "NOMBRE_HABILITACION": "NOMBRE_HABILITACION",
    "ZONA": "ZONA_SECTOR_ETAPA",
    "SUBLOTE": "SUBLOTE",

    # II. Titular catastral
    "TIPO_TITULAR": "TIPO_TITULAR",
    "ESTADO_CIVIL": "ESTADO_CIVIL",
    "TIPO_DOC": "TIPO_DOC_IDENTIDAD",
    "DNI": "NUMERO_DOCUMENTO",
    "NOMBRES": "NOMBRES",
    "APELLIDO_PATERNO": "APELLIDO_PATERNO",
    "APELLIDO_MATERNO": "APELLIDO_MATERNO",
    "RUC": "NUMERO_RUC",
    "RAZON_SOCIAL": "RAZON_SOCIAL",
    "PERSONA_JURIDICA": "PERSONA_JURIDICA",

    # III. Domicilio fiscal
    "DOMICILIO_FISCAL": "DOMICILIO_FISCAL",
    "DOMICILIO_DEPARTAMENTO": "DOMICILIO_DEPARTAMENTO",
    "DOMICILIO_PROVINCIA": "DOMICILIO_PROVINCIA",
    "DOMICILIO_DISTRITO": "DOMICILIO_DISTRITO",
    "DOMICILIO_VIA": "DOMICILIO_VIA",
    "DOMICILIO_NUMERO": "DOMICILIO_NUMERO",
    "DOMICILIO_INTERIOR": "DOMICILIO_INTERIOR",
    "DOMICILIO_HU": "DOMICILIO_HU",
    "DOMICILIO_HABILITACION": "DOMICILIO_HABILITACION",
    "DOMICILIO_ZONA": "DOMICILIO_ZONA",
    "DOMICILIO_MANZANA": "DOMICILIO_MANZANA",
    "DOMICILIO_LOTE": "DOMICILIO_LOTE",
    "DOMICILIO_SUBLOTE": "DOMICILIO_SUBLOTE",
    "TELEFONO": "TELEFONO",
    "ANEXO": "ANEXO",
    "CORREO": "CORREO_ELECTRONICO",

    # IV. Titularidad
    "CONDICION_TITULAR": "CONDICION_TITULAR",
    "FORMA_ADQUISICION": "FORMA_ADQUISICION",
    "FECHA_ADQUISICION": "FECHA_ADQUISICION",

    # V. Predio
    "CLASIFICACION_PREDIO": "CLASIFICACION_PREDIO",
    "CODIGO_USO": "CODIGO_USO",
    "USO_PREDIO": "USO_PREDIO",
    "ZONIFICACION": "ZONIFICACION",
    "AREA_TERRENO_ADQUIRIDA": "AREA_TERRENO_ADQUIRIDA",
    "AREA_TERRENO_VERIFICADA": "AREA_TERRENO_VERIFICADA",
    "MEDIDA_FRENTE": "MEDIDA_FRENTE",
    "MEDIDA_DERECHA": "MEDIDA_DERECHA",
    "MEDIDA_IZQUIERDA": "MEDIDA_IZQUIERDA",
    "MEDIDA_FONDO": "MEDIDA_FONDO",
    "COLINDANCIA_FRENTE": "COLINDANCIA_FRENTE",
    "COLINDANCIA_DERECHA": "COLINDANCIA_DERECHA",
    "COLINDANCIA_IZQUIERDA": "COLINDANCIA_IZQUIERDA",
    "COLINDANCIA_FONDO": "COLINDANCIA_FONDO",
    "SERVICIO_LUZ": "SERVICIO_LUZ",
    "SERVICIO_AGUA": "SERVICIO_AGUA",
    "SERVICIO_TELEFONO": "SERVICIO_TELEFONO",
    "SERVICIO_DESAGUE": "SERVICIO_DESAGUE",
    "SERVICIO_GAS": "SERVICIO_GAS",
    "SERVICIO_INTERNET": "SERVICIO_INTERNET",
    "SERVICIO_TV": "SERVICIO_TV",

    # VI. Construcciones
    "NUMERO_PISO": "NUMERO_PISO",
    "FECHA_CONSTRUCCION": "FECHA_CONSTRUCCION",
    "MEP": "MEP",
    "ECS": "ECS",
    "ECC": "ECC",
    "MUROS_COLUMNAS": "MUROS_COLUMNAS",
    "TECHOS": "TECHOS",
    "PISOS": "PISOS",
    "PUERTAS_VENTANAS": "PUERTAS_VENTANAS",
    "REVEST": "REVEST",
    "BANOS": "BANOS",
    "INSTALACIONES": "INSTALACIONES",
    "AREA_CONSTRUIDA": "AREA_VERIFICADA",
    "UCA": "UCA",
    "PORCENTAJE_BIEN_COMUN": "PORCENTAJE_BIEN_COMUN",

    # VII. Obras complementarias
    "OBRA_CODIGO": "OBRA_CODIGO",
    "OBRA_DESCRIPCION": "OBRA_DESCRIPCION",
    "OBRA_FECHA_CONSTRUCCION": "OBRA_FECHA_CONSTRUCCION",
    "OBRA_MEP": "OBRA_MEP",
    "OBRA_ECS": "OBRA_ECS",
    "OBRA_ECC": "OBRA_ECC",
    "OBRA_PRODUCTO_TOTAL": "OBRA_PRODUCTO_TOTAL",
    "OBRA_UNIDAD": "OBRA_UNIDAD"
}

# Extensiones según spans reales del ASR/NER
LABEL_MAP.update({
    "DEPARTAMENTO_NOMBRE": "DEPARTAMENTO",
    "PROVINCIA_NOMBRE": "PROVINCIA",
    "DISTRITO_NOMBRE": "DISTRITO",
    "HABILITACION_URBANA": "NOMBRE_HABILITACION",
    "COLINDANTE_FRENTE": "COLINDANCIA_FRENTE",
    "COLINDANTE_DERECHA": "COLINDANCIA_DERECHA",
    "COLINDANTE_IZQUIERDA": "COLINDANCIA_IZQUIERDA",
    "COLINDANTE_FONDO": "COLINDANCIA_FONDO",
    "MES": "MES",
    "ANIO": "ANIO",
})

# -------------------------
# 1) Normalizadores/Helpers
# -------------------------
def _normalize_upper(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip().upper()

def _digits_only(s: str) -> str:
    return re.sub(r"\D+", "", s)

def _keep_alnum_basic(s: str) -> str:
    return re.sub(r"[^0-9A-Za-zÁÉÍÓÚÑáéíóúñ\-\./\s]", "", s).strip()

def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

def _normalize_place(s: str) -> str:
    s = _normalize_upper(s)
    s = _strip_accents(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# === Helpers específicos para fechas/números/CUC ===
MONTHS_ES = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04", "mayo": "05", "junio": "06",
    "julio": "07", "agosto": "08", "septiembre": "09", "setiembre": "09", "octubre": "10",
    "noviembre": "11", "diciembre": "12"
}

def parse_fecha_es(s: str) -> Optional[str]:
    """
    '15 de junio del 2015' -> '2015-06-15'
    Acepta: '15 junio 2015', 'junio 2015', 'junio del 2015', '2015'
    """
    if not s: return None
    t = s.strip().lower()
    t = re.sub(r"[,.]", " ", t)
    t = re.sub(r"\s+", " ", t)

    # dd de mes de(l) yyyy
    m = re.search(r"(\d{1,2})\s+de\s+([a-záéíóú]+)\s+(?:de\s+|del\s+)?(\d{4})", t)
    if m:
        dd, mes, yyyy = m.groups()
        mes = _strip_accents(mes).lower()
        mm = MONTHS_ES.get(mes)
        if mm: return f"{yyyy}-{mm}-{int(dd):02d}"

    # dd mes yyyy
    m = re.search(r"(\d{1,2})\s+([a-záéíóú]+)\s+(\d{4})", t)
    if m:
        dd, mes, yyyy = m.groups()
        mes = _strip_accents(mes).lower()
        mm = MONTHS_ES.get(mes)
        if mm: return f"{yyyy}-{mm}-{int(dd):02d}"

    # mes de(l) yyyy  (acepta 'del')
    m = re.search(r"([a-záéíóú]+)\s+(?:de\s+|del\s+)?(\d{4})", t)
    if m:
        mes, yyyy = m.groups()
        mes = _strip_accents(mes).lower()
        mm = MONTHS_ES.get(mes)
        if mm: return f"{yyyy}-{mm}-01"

    # yyyy
    m = re.search(r"\b(19|20)\d{2}\b", t)
    if m:
        yyyy = m.group(0)
        return f"{yyyy}-01-01"

    raw_digits = re.sub(r"[^\d]", "", s)
    return raw_digits or None

def extract_cuc(text: str) -> Optional[str]:
    """ Extrae un CUC de 12 dígitos (contiguos o 6 pares cerca de 'código único catastral'). """
    if not text: return None
    m = re.search(r"\b(\d{12})\b", text)
    if m: return m.group(1)

    pat_pairs = r"(?:\d{2})"
    m2 = re.search(
        rf"(?:c[oó]digo\s+u[nm]ico\s+catastral|cuc).*?(({pat_pairs}[\s,]+){{5}}{pat_pairs})",
        text, flags=re.IGNORECASE
    )
    if m2:
        seq = re.sub(r"[^\d]", "", m2.group(1))
        if len(seq) == 12:
            return seq

    all_digits = re.findall(r"\d", text)
    if len(all_digits) >= 12:
        return "".join(all_digits[:12])
    return None

def normalize_to_digits_first(s: str) -> Optional[str]:
    """ 'sector 35' -> '35' (si hay dígitos). """
    if not s: return None
    m = re.search(r"\d+", s)
    return m.group(0) if m else s.strip()

# =========================
# NORMALIZERS
# =========================
NORMALIZERS = {
    # Documentos
    "DNI": lambda x: re.sub(r"\D", "", x) if x else None,  # solo 8 dígitos
    "NUMERO_DOCUMENTO": lambda x: re.sub(r"\D", "", x) if x else None,
    "RUC": lambda x: re.sub(r"\D", "", x)[:11] if x else None,  # 11 dígitos

    # Teléfono
    "TELEFONO": lambda x: re.sub(r"\D", "", x) if x else None,  # limpia guiones/espacios

    # Correos
    "CORREO_ELECTRONICO": lambda x: x.strip().lower() if x else None,

    # Códigos numéricos (Catastrales, contribuyente, predial)
    "CODIGO_UNICO_CATASTRAL": lambda x: re.sub(r"\D", "", x)[:12] if x else None,
    "CODIGO_CONTRIBUYENTE": lambda x: re.sub(r"\D", "", x) if x else None,
    "CODIGO_PREDIAL": lambda x: re.sub(r"\D", "", x) if x else None,

    # Ubicación
    "DEPARTAMENTO": lambda x: x.strip().upper() if x else None,
    "PROVINCIA": lambda x: x.strip().upper() if x else None,
    "DISTRITO": lambda x: x.strip().upper() if x else None,
    "NOMBRE_VIA": lambda x: x.strip().title() if x else None,
    "NUMERO_MUNICIPAL": lambda x: re.sub(r"\D", "", x) if x else None,
    "NUMERO_INTERIOR": lambda x: x.strip().upper() if x else None,
    "NOMBRE_HABILITACION": lambda x: x.strip().title() if x else None,
    "ZONA_SECTOR_ETAPA": lambda x: x.strip().upper() if x else None,
    "MANZANA": lambda x: re.sub(r"\s+", "", x.upper()) if x else None,
    "LOTE": lambda x: x.strip().upper() if x else None,
    "SUBLOTE": lambda x: x.strip().upper() if x else None,

    # Titular
    "NOMBRES": lambda x: x.strip().title() if x else None,
    "APELLIDO_PATERNO": lambda x: x.strip().title() if x else None,
    "APELLIDO_MATERNO": lambda x: x.strip().title() if x else None,
    "RAZON_SOCIAL": lambda x: x.strip().upper() if x else None,

    # Fechas
    "FECHA_ADQUISICION": lambda x: re.sub(r"[^0-9/.-]", "", x) if x else None,
    "FECHA_CONSTRUCCION": lambda x: re.sub(r"[^0-9/.-]", "", x) if x else None,
    "OBRA_FECHA_CONSTRUCCION": lambda x: re.sub(r"[^0-9/.-]", "", x) if x else None,

    # Áreas y medidas (convertir a float si posible)
    "AREA_TERRENO_ADQUIRIDA": lambda x: float(re.sub(r"[^\d.]", "", x)) if re.sub(r"[^\d.]", "", x) else None,
    "AREA_TERRENO_VERIFICADA": lambda x: float(re.sub(r"[^\d.]", "", x)) if re.sub(r"[^\d.]", "", x) else None,
    "AREA_VERIFICADA": lambda x: float(re.sub(r"[^\d.]", "", x)) if re.sub(r"[^\d.]", "", x) else None,
    "MEDIDA_FRENTE": lambda x: float(re.sub(r"[^\d.]", "", x)) if re.sub(r"[^\d.]", "", x) else None,
    "MEDIDA_DERECHA": lambda x: float(re.sub(r"[^\d.]", "", x)) if re.sub(r"[^\d.]", "", x) else None,
    "MEDIDA_IZQUIERDA": lambda x: float(re.sub(r"[^\d.]", "", x)) if re.sub(r"[^\d.]", "", x) else None,
    "MEDIDA_FONDO": lambda x: float(re.sub(r"[^\d.]", "", x)) if re.sub(r"[^\d.]", "", x) else None,

    # Servicios (1=Sí, 2=No → normalizar a bool)
    "SERVICIO_LUZ": lambda x: True if str(x).strip().upper() in ["1", "SI", "SÍ"] else False,
    "SERVICIO_AGUA": lambda x: True if str(x).strip().upper() in ["1", "SI", "SÍ"] else False,
    "SERVICIO_TELEFONO": lambda x: True if str(x).strip().upper() in ["1", "SI", "SÍ"] else False,
    "SERVICIO_DESAGUE": lambda x: True if str(x).strip().upper() in ["1", "SI", "SÍ"] else False,
    "SERVICIO_GAS": lambda x: True if str(x).strip().upper() in ["1", "SI", "SÍ"] else False,
    "SERVICIO_INTERNET": lambda x: True if str(x).strip().upper() in ["1", "SI", "SÍ"] else False,
    "SERVICIO_TV": lambda x: True if str(x).strip().upper() in ["1", "SI", "SÍ"] else False,
}

# Ajustes mejorados
NORMALIZERS.update({
    # CUC: usa extractor robusto
    "CODIGO_UNICO_CATASTRAL": lambda x: extract_cuc(x) or (re.sub(r"\D", "", x)[:12] if x else None),

    # Números de ubicación: tomar dígitos si existen
    "SECTOR": lambda x: normalize_to_digits_first(x),
    "MANZANA": lambda x: normalize_to_digits_first(x),
    "LOTE": lambda x: normalize_to_digits_first(x),
    "NUMERO_INTERIOR": lambda x: normalize_to_digits_first(x),

    # Fecha adquisición a ISO
    "FECHA_ADQUISICION": lambda x: parse_fecha_es(x),

    # MEP/ECS/ECC: normaliza a catálogo (y resuelve conflictos simples)
    "MEP": lambda x: (
        "CONCRETO" if re.search(r"\bconcreto\b", x, flags=re.I) else
        "LADRILLO" if re.search(r"\bladrillo(s)?\b", x, flags=re.I) else
        "MADERA"   if re.search(r"\bmadera\b", x, flags=re.I) else
        "ADOBE"    if re.search(r"\badobe\b", x, flags=re.I) else
        "QUINCHA"  if re.search(r"\bquincha\b", x, flags=re.I) else
        _normalize_upper(x)
    ),

    # Dirección/HU en bonito
    "NOMBRE_HABILITACION": lambda x: x.strip().title() if x else None,
})

# --------------------
# 2) Validadores/RegEx
# --------------------
VALIDATORS = {
    # Documentos (ya mapeados por LABEL_MAP)
    "NUMERO_DOCUMENTO": re.compile(r"^\d{8}$"),        # DNI: 8 dígitos
    "NUMERO_RUC": re.compile(r"^\d{11}$"),             # RUC: 11 dígitos

    # Teléfono / correo
    "TELEFONO": re.compile(r"^9\d{8}$"),               # Perú: 9 + 8 dígitos
    "CORREO_ELECTRONICO": re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"),

    # Códigos catastrales
    "CODIGO_UNICO_CATASTRAL": re.compile(r"^\d{12}$"), # 12 exactos
    "CODIGO_CONTRIBUYENTE": re.compile(r"^\d{6,10}$"),
    "CODIGO_PREDIAL": re.compile(r"^\d{6,10}$"),

    # Numeraciones
    "NUMERO_MUNICIPAL": re.compile(r"^[0-9A-Za-z\-\/\. ]{1,15}$"),
    "NUMERO_INTERIOR": re.compile(r"^[0-9A-Za-z\-\/\. ]{1,10}$"),

    # Ubicación simple
    "SECTOR": re.compile(r"^[0-9A-Za-z\- ]{1,10}$"),
    "MANZANA": re.compile(r"^[0-9A-Za-z\- ]{1,10}$"),
    "LOTE": re.compile(r"^[0-9A-Za-z\- ]{1,10}$"),

    # Zonificación (evita ‘DNI’)
    "ZONIFICACION": re.compile(r"^(?!DNI$)[A-Z0-9\-\/]{2,10}$"),

    # Áreas / medidas (decimales opcionales)
    "AREA_TERRENO_ADQUIRIDA": re.compile(r"^\d+(\.\d{1,2})?$"),
    "AREA_TERRENO_VERIFICADA": re.compile(r"^\d+(\.\d{1,2})?$"),
    "AREA_VERIFICADA": re.compile(r"^\d+(\.\d{1,2})?$"),
    "MEDIDA_FRENTE": re.compile(r"^\d+(\.\d{1,2})?$"),
    "MEDIDA_DERECHA": re.compile(r"^\d+(\.\d{1,2})?$"),
    "MEDIDA_IZQUIERDA": re.compile(r"^\d+(\.\d{1,2})?$"),
    "MEDIDA_FONDO": re.compile(r"^\d+(\.\d{1,2})?$"),

    # Fecha ISO
    "FECHA_ADQUISICION": re.compile(r"^\d{4}-\d{2}-\d{2}$"),
}

# -----------------------------------
# 3) Catálogo UBIGEO
# -----------------------------------
UBIGEO_CACHE: Dict[str, str] = {}
UBIGEO_PATHS = [
    Path(__file__).parent / "ubigeo.xlsx",  # producción
    Path(__file__).parent / "ubigeo.csv",   # fallback CSV
]

def _detect_headers(headers: List[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    # Devuelve nombres canónicos de columnas
    H = { (h or "").upper().strip(): (h or "") for h in headers }
    def pick(*cands):
        for c in cands:
            if c in H: return H[c]
        return None
    col_dep  = pick("DEPARTAMENTO", "DPTO", "DEPA", "DEPART")
    col_prov = pick("PROVINCIA", "PROV")
    col_dist = pick("DISTRITO", "DIST")
    col_ubi  = pick("UBIGEO", "COD_UBIGEO", "UBI")
    return col_dep, col_prov, col_dist, col_ubi

def _load_ubigeo_from_excel(path: Path) -> Dict[str, str]:
    try:
        import openpyxl  # pip install openpyxl
    except ImportError:
        return {}
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return {}
    headers = [str(h or "").strip() for h in rows[0]]
    col_dep, col_prov, col_dist, col_ubi = _detect_headers(headers)
    if not all([col_dep, col_prov, col_dist, col_ubi]):
        return {}
    idx = {h: i for i, h in enumerate(headers)}
    cat: Dict[str, str] = {}
    for r in rows[1:]:
        dep  = _normalize_place(str(r[idx[col_dep]])  if r[idx[col_dep]]  is not None else "")
        prov = _normalize_place(str(r[idx[col_prov]]) if r[idx[col_prov]] is not None else "")
        dist = _normalize_place(str(r[idx[col_dist]]) if r[idx[col_dist]] is not None else "")
        ubi  = re.sub(r"\D", "", str(r[idx[col_ubi]]) if r[idx[col_ubi]] is not None else "")
        if dep and prov and dist and len(ubi) == 6:
            key = f"{dep}|{prov}|{dist}"
            cat[key] = ubi
    return cat

def _load_ubigeo_from_csv(path: Path) -> Dict[str, str]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        col_dep, col_prov, col_dist, col_ubi = _detect_headers(reader.fieldnames or [])
        if not all([col_dep, col_prov, col_dist, col_ubi]):
            return {}
        cat: Dict[str, str] = {}
        for row in reader:
            dep  = _normalize_place(row.get(col_dep, ""))
            prov = _normalize_place(row.get(col_prov, ""))
            dist = _normalize_place(row.get(col_dist, ""))
            ubi  = re.sub(r"\D", "", row.get(col_ubi, ""))
            if dep and prov and dist and len(ubi) == 6:
                key = f"{dep}|{prov}|{dist}"
                cat[key] = ubi
        return cat

def load_ubigeo_catalog() -> Dict[str, str]:
    global UBIGEO_CACHE
    if UBIGEO_CACHE:
        return UBIGEO_CACHE
    for p in UBIGEO_PATHS:
        if p.suffix.lower() in {".xlsx", ".xls"} and p.exists():
            cat = _load_ubigeo_from_excel(p)
            if cat:
                UBIGEO_CACHE = cat
                return UBIGEO_CACHE
        if p.suffix.lower() == ".csv" and p.exists():
            cat = _load_ubigeo_from_csv(p)
            if cat:
                UBIGEO_CACHE = cat
                return UBIGEO_CACHE
    UBIGEO_CACHE = {}
    return UBIGEO_CACHE

# -----------------------------------------
# 4) Limpieza previa (antes de pasar a NER)
# -----------------------------------------
def clean_text(raw: str) -> str:
    """
    Limpieza ligera pensada para texto con ruido ASR/OCR:
    - Colapsa números tipo "15, 000, 23" -> "1500023"
    - Normaliza comas repetidas y espacios
    - Arregla 'soviquado'/'sovicado' -> 'ubicado' (ejemplo típico)
    - Mantiene tildes; el modelo suele beneficiarse del texto original
    """
    s = raw
    s = re.sub(r"(?:(?<=\d)[\s,]+(?=\d))+", "", s)  # unir números partidos
    s = re.sub(r"\bso[vb]i[cq]uado\b", "ubicado", s, flags=re.IGNORECASE)
    s = re.sub(r"\bhablicaci[oó]n\b", "habilitacion", s, flags=re.IGNORECASE)
    s = re.sub(r"\bproe?dial\b", "predial", s, flags=re.IGNORECASE)
    s = re.sub(r"\brantas\b", "rentas", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# ---------------------------------------
# 5) Inferencia NER y fusión de entidades
# ---------------------------------------
@dataclass
class SpanInfo:
    label: str
    text: str
    start: int
    end: int
    score: Optional[float] = None

def load_model(model_path: str = MODEL_PATH):
    try:
        nlp = spacy.load(model_path)
    except Exception as e:
        raise RuntimeError(f"No se pudo cargar el modelo spaCy en '{model_path}': {e}")
    return nlp

_NLP: Optional[spacy.Language] = None
def get_nlp():
    global _NLP
    if _NLP is None:
        _NLP = load_model(MODEL_PATH)
    return _NLP

def run_ner(text: str) -> List[SpanInfo]:
    nlp = get_nlp()
    doc = nlp(text)
    spans: List[SpanInfo] = []
    for ent in doc.ents:
        spans.append(SpanInfo(ent.label_, ent.text, ent.start_char, ent.end_char, getattr(ent, "kb_id_", None)))
    return spans

def map_and_merge(spans: List[SpanInfo]) -> Tuple[Dict[str, List[SpanInfo]], Dict[str, List[SpanInfo]]]:
    """
    - Mapea labels a claves estándar
    - Combina por clave (guardando todos los candidatos para auditoría)
    - Devuelve:
      * por_campo: {campo_std: [SpanInfo,...]} (ordenado por preferencia simple)
      * por_label: {label_original: [SpanInfo,...]}
    """
    por_campo: Dict[str, List[SpanInfo]] = {}
    por_label: Dict[str, List[SpanInfo]] = {}
    for sp in spans:
        por_label.setdefault(sp.label, []).append(sp)
        if sp.label in LABEL_MAP:
            std = LABEL_MAP[sp.label]
            por_campo.setdefault(std, []).append(sp)
    # Heurística simple: ordenar por (longitud desc, inicio asc)
    for k in por_campo:
        por_campo[k].sort(key=lambda s: (-len(s.text.strip()), s.start))
    return por_campo, por_label

# -------------------------------------------------
# 6) Normalización + Validación + UBIGEO inferido
# -------------------------------------------------
@dataclass
class FieldResult:
    raw: Optional[str] = None
    normalized: Optional[str] = None
    valid: Optional[bool] = None
    errors: List[str] = field(default_factory=list)
    sources: List[Dict[str, Any]] = field(default_factory=list)  # spans

def normalize_field(key: str, value: str) -> str:
    fn = NORMALIZERS.get(key, lambda x: x.strip() if isinstance(x, str) else x)
    return fn(value or "")

def validate_field(key: str, normalized: str) -> Tuple[bool, Optional[str]]:
    if not normalized:
        return False, "VACIO"
    rx = VALIDATORS.get(key)
    if rx is None:
        return True, None  # sin regex dura -> lo damos por válido si no está vacío
    ok = bool(rx.match(normalized))
    return ok, (None if ok else "FORMATO_INVALIDO")

def pick_best_text(candidates: List[SpanInfo]) -> Optional[str]:
    if not candidates:
        return None
    # ya vienen ordenados por map_and_merge => tomamos el primero
    return candidates[0].text.strip()

def infer_ubigeo(fields: Dict[str, FieldResult]) -> Optional[str]:
    """
    Si tenemos DEPARTAMENTO / PROVINCIA / DISTRITO, intenta construir UBIGEO (6 dígitos)
    desde el catálogo cargado.
    """
    dep = (fields.get("DEPARTAMENTO") or FieldResult()).normalized or ""
    prov = (fields.get("PROVINCIA") or FieldResult()).normalized or ""
    dist = (fields.get("DISTRITO") or FieldResult()).normalized or ""
    if not (dep and prov and dist):
        return None
    cat = load_ubigeo_catalog()
    if not cat:
        return None
    key = f"{_normalize_place(dep)}|{_normalize_place(prov)}|{_normalize_place(dist)}"
    return cat.get(key)

def build_fields(por_campo: Dict[str, List[SpanInfo]], full_text: Optional[str] = None) -> Dict[str, FieldResult]:
    result: Dict[str, FieldResult] = {}
    for key, candidates in por_campo.items():
        fr = FieldResult()
        fr.raw = pick_best_text(candidates)
        fr.normalized = normalize_field(key, fr.raw or "")
        ok, err = validate_field(key, fr.normalized or "")
        fr.valid = ok
        if err:
            fr.errors.append(err)
        # auditar spans
        fr.sources = [
            {"label": c.label, "text": c.text, "start": c.start, "end": c.end}
            for c in candidates
        ]
        result[key] = fr

    # Protección específica
    if "ZONIFICACION" in result and (result["ZONIFICACION"].normalized or "").upper() == "DNI":
        result["ZONIFICACION"].valid = False
        result["ZONIFICACION"].errors.append("COINCIDE_LITERAL_DNI")

    # UBIGEO (si no vino del modelo, inferir con catálogo)
    if "UBIGEO" not in result:
        ub = infer_ubigeo(result)
        if ub:
            result["UBIGEO"] = FieldResult(raw=ub, normalized=ub, valid=True, errors=[],
                                           sources=[{"label": "INFERIDO", "text": ub}])

    # Combinar MES + ANIO en FECHA_CONSTRUCCION si aplica
    if "FECHA_CONSTRUCCION" not in result:
        mes = (result.get("MES") or FieldResult()).raw or ""
        anio = (result.get("ANIO") or FieldResult()).raw or ""
        anio_clean = re.sub(r"\D+", " ", anio).strip()  # 'del 2015' -> '2015'
        candidate = f"{mes} {anio_clean}".strip()
        if mes or anio:
            iso = parse_fecha_es(candidate)
            if iso:
                result["FECHA_CONSTRUCCION"] = FieldResult(
                    raw=candidate,
                    normalized=iso, valid=True, errors=[],
                    sources=[{"label": "MES/ANIO_FUSION", "text": candidate}]
                )

    # --- REPARACIÓN CUC usando el texto completo ---
    cuc_key = "CODIGO_UNICO_CATASTRAL"
    if full_text:
        if cuc_key in result:
            curr = result[cuc_key].normalized or ""
            if len(str(curr)) != 12:
                found = extract_cuc(full_text)
                if found and len(found) == 12:
                    result[cuc_key].normalized = found
                    ok, err = validate_field(cuc_key, found)
                    result[cuc_key].valid = ok
                    result[cuc_key].errors = [] if ok else ["FORMATO_INVALIDO"]
                    result[cuc_key].sources.append({"label": "REPARADO_FULLTEXT", "text": found})
        else:
            found = extract_cuc(full_text)
            if found and len(found) == 12:
                result[cuc_key] = FieldResult(
                    raw=found, normalized=found, valid=True, errors=[],
                    sources=[{"label": "INFERIDO_FULLTEXT", "text": found}]
                )

    return result

# --------------------------------------------
# 7) Salida JSON canónica + campos complementos
# --------------------------------------------
def assemble_output(text: str, spans: List[SpanInfo], fields: Dict[str, FieldResult]) -> Dict[str, Any]:
    out = {
        "input_length": len(text),
        "fields": {},
        "summary": {
            "valid_count": 0,
            "invalid_count": 0,
            "empty_count": 0
        },
        "spans": [
            {"label": s.label, "text": s.text, "start": s.start, "end": s.end}
            for s in spans
        ],
    }
    valid_cnt = invalid_cnt = empty_cnt = 0
    for k, fr in fields.items():
        out["fields"][k] = {
            "raw": fr.raw,
            "normalized": fr.normalized,
            "valid": fr.valid,
            "errors": fr.errors,
            "sources": fr.sources
        }
        if (fr.normalized or "") == "":
            empty_cnt += 1
        elif fr.valid:
            valid_cnt += 1
        else:
            invalid_cnt += 1
    out["summary"].update({
        "valid_count": valid_cnt,
        "invalid_count": invalid_cnt,
        "empty_count": empty_cnt
    })
    return out

# ---------------------------------------
# 8) Función principal de procesamiento
# ---------------------------------------
def process_text(raw_text: str) -> Dict[str, Any]:
    cleaned = clean_text(raw_text)
    spans = run_ner(cleaned)
    por_campo, _ = map_and_merge(spans)
    fields = build_fields(por_campo, full_text=cleaned)  # <-- pasa el texto completo
    return assemble_output(cleaned, spans, fields)

# ---------------
# 9) FastAPI App
# ---------------
class ExtractRequest(BaseModel):
    text: str

app = FastAPI(title="Pipeline Catastral NER -> JSON", version="1.0")

@app.get("/health")
def health():
    # Fuerza carga de modelo para detectar problemas temprano
    _ = get_nlp()
    _ = load_ubigeo_catalog()
    has_ubigeo = bool(UBIGEO_CACHE)
    has_aai = bool(os.getenv("ASSEMBLYAI_API_KEY"))
    return {"status": "ok", "model_loaded": True, "ubigeo_loaded": has_ubigeo, "assemblyai_key": has_aai}

@app.post("/extract")
def extract(req: ExtractRequest):
    return process_text(req.text)

@app.post("/transcribir")
async def transcribir(file: UploadFile = File(...)):
    try:
        audio_bytes = await file.read()
        return transcribe_audio(audio_bytes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/transcribir_extract")
async def transcribir_y_extraer(file: UploadFile = File(...)):
    """
    1) Transcribe audio -> texto
    2) Pasa por clean_text + NER + normalización + validación
    3) Retorna JSON canónico con summary
    """
    try:
        audio_bytes = await file.read()
        t = transcribe_audio(audio_bytes)
        result = process_text(t.get("text") or "")
        # opcional: incluir metadatos de ASR
        result["asr"] = {
            "confidence": t.get("confidence"),
            "num_words": len(t.get("words") or []),
        }
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------
# 10) CLI para uso directo
# ------------------------
if __name__ == "__main__":
    import argparse, sys
    p = argparse.ArgumentParser(description="Pipeline Catastral (NER->JSON)")
    p.add_argument("--file", "-f", help="Ruta de archivo de texto a procesar")
    p.add_argument("--text", "-t", help="Texto directo a procesar")
    args = p.parse_args()

    if not args.text and not args.file:
        print("Usa --text '...' o --file ruta.txt", file=sys.stderr)
        sys.exit(1)

    if args.file:
        content = Path(args.file).read_text(encoding="utf-8", errors="ignore")
    else:
        content = args.text

    result = process_text(content)
    print(json.dumps(result, ensure_ascii=False, indent=2))
