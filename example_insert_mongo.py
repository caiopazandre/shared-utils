#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import csv
from datetime import datetime
from typing import Any, List
import pandas as pd
from pymongo import MongoClient, errors
from bson import ObjectId

# ---------- Configurações ----------
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "mingest"
COL_MAIN = "cIgtaoArq"
COL_LAYOUT = "cLyoutTbela"
CSV_FILE = "dados.csv"   # ajuste para o seu arquivo
OUTPUT_CSV = "processed.csv"  # arquivo de saída com _id e cIdtfdIgtao

# ---------- Logging ----------
# ---------- Logging ----------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[logging.StreamHandler(), 
                              logging.FileHandler("ingest.log", encoding="utf-8")
                            ])

# ---------- Helpers ----------
def parse_cdLayout_cell(cell: Any) -> List[Any]:
    """Interpreta o conteúdo da célula cdLayout: tenta JSON, senão separa por ';' ou ','."""
    if cell is None:
        return []
    if isinstance(cell, list):
        return cell
    if isinstance(cell, str):
        s = cell.strip()
        if s == "":
            return []
        try:
            parsed = json.loads(s)
            return parsed if isinstance(parsed, list) else [parsed]
        except Exception:
            sep = ';' if ';' in s else ','
            return [p.strip() for p in s.split(sep) if p.strip()]
    return [cell]

# ---------- Conexão Mongo ----------
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col_main = db[COL_MAIN]
col_layout = db[COL_LAYOUT]

# ---------- Leitura CSV ----------
df = pd.read_csv(CSV_FILE, dtype=str).fillna("")  # ler tudo como string e normalizar

# ---------- Abrir arquivo de saída uma vez e manter aberto até o final ----------
fout = open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8")
writer = csv.writer(fout)
# cabeçalho: duas colunas novas no início seguidas das colunas originais
writer.writerow(["_id", "cIdtfdIgtao"] + list(df.columns))

# ---------- Loop principal conforme sua referência ----------
for idx, row in df.iterrows():
    try:
        # montar documento principal sem cdLayout
        doc = {}
        for k, v in row.items():
            if k == "cdLayout":
                continue
            val = v.strip() if isinstance(v, str) else v
            doc[k] = val if val != "" else None

        # opcional: adicionar timestamp
        doc["ingestedAt"] = datetime.utcnow()

        # inserir documento principal (insert_one retorna inserted_id)
        res = col_main.insert_one(doc)
        inserted_id = res.inserted_id
        logging.info(f"linha {idx}: inserido _id={inserted_id}")

        # buscar cIdtfdIgtao no documento inserido
        # projetar apenas o campo necessário para economizar banda
        inserted_doc = col_main.find_one({"_id": inserted_id}, {"cIdtfdIgtao": 1})
        cIdtfdIgtao = inserted_doc.get("cIdtfdIgtao") if inserted_doc else None

        # se por algum motivo o servidor não gerou cIdtfdIgtao, aplicar fallback opcional
        if not cIdtfdIgtao:
            logging.error(f'Falha na inserção de registro, inclusão de layout ignorado.')
            continue

        # preparar e inserir layouts se existir cdLayout
        raw_cd = row.get("cdLayout", "")
        cd_list = parse_cdLayout_cell(raw_cd)
        if cd_list:
            # dentro do loop, onde você já tem cIdtfdIgtao e layout_val
            layout_docs = []
            for i, layout_val in enumerate(cd_list):
                # se layout_val já for dict, mesclar; se for primitivo, armazenar em "cdLayout"
                if isinstance(layout_val, dict):
                    doc_layout = {**layout_val}               # campos do objeto de layout
                else:
                    logging.error(f'O layout informado na planilha é invalido, o valor {type(layout_val)} não é um objeto')
                    continue

                # adicionar o identificador e metadados
                doc_layout["cIdtfdIgtao"] = cIdtfdIgtao

                layout_docs.append(doc_layout)
            # inserir todos os layouts deste ingest em lote
            res_layout = col_layout.insert_many(layout_docs)
            logging.info(f"linha {idx}: inseridos {len(res_layout.inserted_ids)} layouts para cIdtfdIgtao={cIdtfdIgtao}")

        # ---------- escrever linha no CSV de saída ----------
        id_str = str(inserted_id)
        cid_str = str(cIdtfdIgtao)
        original_values = [row[col] for col in df.columns]
        writer.writerow([id_str, cid_str] + original_values)

    except errors.PyMongoError as e:
        logging.error(f"linha {idx}: erro ao processar registro: {e}")
        # aqui você pode decidir: continuar, retry, ou abortar. Exemplo: continuar
        continue

# fechar arquivo de saída
try:
    fout.flush()
except Exception:
    logging.error(f'Falha na gravação do registro de saida contendo _id e ')
fout.close()

logging.info("Processamento finalizado.")
