#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module de traitement pour la génération automatisée de samplesheets.
Contient deux fonctions principales : preparator et converter.
"""
import os
import time
import re
import csv
import json
from collections import defaultdict

import pandas as pd

# =========================
# Configuration par défaut
# =========================
# Chemin temporaire pour le fichier nettoyé
CLEANED_TEMP = os.path.join(os.getcwd(), "cleaned_file.csv")
# Répertoire pour les logs d'exécution
LOG_AUTO_SS_PATH = os.path.join(os.getcwd(), "logs")
# Fichier de log des erreurs
ERROR_SS_LOG_PATH = os.path.join(os.getcwd(), "error_ss_autoV3.err")

# Projets valides et combinaisons autorisées
VALID_PROJECTS = [
    'ncov','hsv12','fluabv','vzv','20236','16206','22188','21098','21710',
    '23067','23128','10042','15228','23127','23161'
]
VALID_COMBINATIONS = {
    'VIRO-NCOV': ['articV41','articV532','articV542'],
    'VIRO-GRIPPE': ['simplex','multiplex'],
    'VIRO-HSV': ['multiplex'],
    'VIRO-VZV': ['multiplex'],
    'VIRO-EV': ['multiplex','meta'],
    'VIRO-META-RD': ['routine-LCR','meta','Non-meta','WTA','revelo','multiplex','urgent'],
    'VIRO-VRS': ['multiplex','meta'],
    'VIRO-HEPATITE': ['Non-meta','multiplex','simplex'],
    'VIRO-VIH': ['Non-meta','multiplex'],
    'MYCOBACTERIUM': ['multiplex'],
    'VIRO-CMV': ['multiplex','simplex'],
    'NGS-BK': ['multiplex'],
    'VIRO-META-DIAG': ['routine-LCR','meta','WTA','revelo','multiplex','urgent']
}

# =========================
# Helpers de logging
# =========================
def log_to_file(message: str, log_dir: str, log_filename: str = "log.log"):
    os.makedirs(log_dir, exist_ok=True)
    with open(os.path.join(log_dir, log_filename), 'a', encoding='utf-8') as lf:
        lf.write(f"{time.ctime()}: {message}\n")


def log_error(message: str):
    with open(ERROR_SS_LOG_PATH, 'a', encoding='utf-8') as ef:
        ef.write(f"{time.ctime()}: {message}\n")
    log_to_file(f"ERROR: {message}", LOG_AUTO_SS_PATH)

# =========================
# Validation de run_id
# =========================
def validate_run_id(run_id: str) -> str | None:
    if not isinstance(run_id, str):
        return None
    parts = run_id.split('_')
    if len(parts) != 4 or not (parts[0].isdigit() and len(parts[0]) == 6) \
       or len(parts[1]) != 6 or len(parts[3]) != 10:
        return None
    # Correction d'éventuel padding
    if len(parts[2]) == 3 and parts[2].isdigit() and not parts[2].startswith('0'):
        parts[2] = '0' + parts[2]
    elif not (parts[2].isdigit() and len(parts[2]) == 4):
        return None
    return '_'.join(parts)

# =========================
# Fonction principale : preparator
# =========================
def preparator(input_path: str, output_dir: str) -> str | None:
    """
    Lecture, nettoyage et validation de la samplesheet brute.
    Génère un fichier CSV traité dans output_dir et retourne son chemin.
    """
    print(f"[Preparator] Processing raw file: {input_path}")
    try:
        with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read().replace('é', '')
        with open(CLEANED_TEMP, 'w', encoding='utf-8') as tmp:
            tmp.write(content)
        df = pd.read_csv(CLEANED_TEMP, delimiter=';', dtype={'ID_GLIMS': str})
    except Exception as e:
        log_error(f"Preparator read error: {e}")
        return None

    correct_headers = ['Lane','Sample_ID','ID_GLIMS','index','index2',
                       'Sample_Project','Set_index','protocol','primers',
                       'sequencer','run_id','bioinfo_project']
    df.columns = correct_headers

    # Vérifications basiques
    for col in correct_headers:
        if col not in ['Sample_Project','run_id','ID_GLIMS'] and df[col].isnull().any():
            log_error(f"Column {col} contains nulls")

    # Nettoyage de Sample_ID
    for sid in df['Sample_ID']:
        if re.search(r'[+* ]', sid):
            clean = re.sub(r'[+* ]', '', sid)
            df['Sample_ID'].replace(sid, clean, inplace=True)
            log_to_file(f"Cleaned Sample_ID {sid} -> {clean}", LOG_AUTO_SS_PATH)

    # Validation et suffixe sur index/index2
    index_lengths = set()
    for col_idx in ['index','index2']:
        for i, seq in enumerate(df[col_idx]):
            filt = ''.join(re.findall(r'[ACGT]', seq))
            if filt != seq:
                print(f"The {col_idx} at row {i+1} contained invalid characters. They have been removed.")
                log_to_file(f"Filtered {col_idx} row {i+1}: {seq} -> {filt}", LOG_AUTO_SS_PATH)
            df.at[i, col_idx] = filt
        for seq in df[col_idx]:
            index_lengths.add(len(seq))
    if {8,10}.issubset(index_lengths):
        for col_idx in ['index','index2']:
            suf = 'AT' if col_idx=='index' else 'GT'
            mask = df[col_idx].str.len() == 8
            df.loc[mask, col_idx] = df.loc[mask, col_idx] + suf
        print("Suffix 'AT' added to 'index' and 'GT' to 'index2' for sequences of length 8.")
        log_to_file("Appended suffixes to length-8 indexes", LOG_AUTO_SS_PATH)

    # Vérification projets & primers
    if df['Sample_Project'].isnull().any():
        log_error("Sample_Project column incomplete")
    if df['primers'].isnull().any():
        log_error("primers column incomplete")

    # Validation run_id
    first_id = df['run_id'].iloc[0]
    vr = validate_run_id(first_id)
    if vr is None:
        log_error(f"Invalid run_id format: {first_id}")
    else:
        df['run_id'] = vr

    # Validation bioinfo_project
    bad_proj = df.loc[~df['bioinfo_project'].isin(VALID_PROJECTS), 'bioinfo_project']
    if not bad_proj.empty:
        log_error(f"Unknown bioinfo_project values: {bad_proj.values}")

    for idx, row in df.iterrows():
        sp, pr, sid = row['Sample_Project'], row['primers'], row['Sample_ID']
        if sp in VALID_COMBINATIONS:
            if pr not in VALID_COMBINATIONS[sp]:
                log_error(f"Invalid primers '{pr}' for Sample_ID {sid} in project {sp}")
        else:
            log_error(f"Invalid Sample_Project '{sp}' for Sample_ID {sid}")

    # Vérification ID_GLIMS pour MYCOBACTERIUM
    for idx, row in df.iterrows():
        if row['Sample_Project']=='MYCOBACTERIUM':
            if not re.match(r'^\d{12}$', str(row['ID_GLIMS']).strip()):
                log_error(f"Invalid ID_GLIMS for Sample_ID {row['Sample_ID']}")
        else:
            df.at[idx, 'ID_GLIMS'] = ''

    # Suppression de la colonne Lane si redondante
    if df['Lane'].nunique() == 1 and df['Lane'].iloc[0] == 1:
        df.drop('Lane', axis=1, inplace=True)

    # Écriture atomique du CSV traité
    run_id = df['run_id'].iloc[0] or 'unknown'
    out_file = os.path.join(output_dir, f"{run_id}.csv")
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    tmp_out = out_file + ".tmp"
    try:
        with open(tmp_out, 'w', newline='') as fout:
            fout.write("[Header]\n[Reads]\n100\n100\n[Settings]\n[Data]\n")
            df.to_csv(fout, index=False, sep=',')
        os.replace(tmp_out, out_file)
        log_to_file(f"Preparator output: {out_file}", LOG_AUTO_SS_PATH)
        return out_file
    except Exception as e:
        log_error(f"Preparator write error: {e}")
        try:
            os.remove(tmp_out)
        except:
            pass
        return None

# =========================
# Fonction auxiliaire : reverse_complement
# =========================
def reverse_complement(seq: str) -> str:
    comp = {'A':'T', 'C':'G', 'G':'C', 'T':'A', 'N':'N'}
    return ''.join(comp.get(b, 'N') for b in reversed(seq))

# =========================
# Fonction secondaire : converter
# =========================
def converter(input_file: str, output_dir: str,
              delimiter: str = ';', no_sp: bool = False,
              rc_i5: bool = False, no_header: bool = False) -> list[str]:
    """
    Lit un fichier traité et génère plusieurs fichiers CSV par combinaison projet/primer.
    Retourne la liste des chemins générés.
    """
    print(f"[Converter] Processing treated file: {input_file}")
    data = defaultdict(list)
    try:
        with open(input_file, 'r') as f:
            if no_header:
                for _ in range(6): next(f)
            header = next(f).strip().split(delimiter)
            lane_present = 'Lane' in header
            reader = csv.reader(f, delimiter=delimiter)
            for row in reader:
                d = dict(zip(header, row))
                key = (d['Sample_Project'], d['primers'], d['Lane'] if lane_present else None)
                data[key].append(d)
    except Exception as e:
        log_error(f"Converter read error: {e}")
        return []

    os.makedirs(output_dir, exist_ok=True)
    out_files = []
    for key, rows in data.items():
        sp, pr, lane = key
        fname = f"Lane{lane}_{sp}_{pr}.csv" if lane else f"{sp}_{pr}.csv"
        path = os.path.join(output_dir, fname)
        with open(path, 'w', newline='') as outf:
            w = csv.writer(outf, delimiter=';')
            w.writerow(["Identifiant","Identifiant_GLIMS","Index_1",
                        "Sequence_index_1","Index_2","Sequence_index_2"])
            cnt = defaultdict(int)
            for r in rows:
                si = r['Set_index']; cnt[si] += 1
                prefix = f"{si}_{cnt[si]}"
                seq2 = reverse_complement(r['index2']) if rc_i5 else r['index2']
                w.writerow([r['Sample_ID'], r.get('ID_GLIMS',''), prefix,
                            r['index'], prefix, seq2])
        log_to_file(f"Converter output: {path}", LOG_AUTO_SS_PATH)
        out_files.append(path)
    return out_files
