#XML
# │
# ├── Spark XML reader
# │      └── lê o rowTag diretamente
# │
# └── parser streaming
#        └── mantém somente os ancestrais
#                    │
#                    ▼
#             chave de correlação
#                    │
#          ┌─────────┴─────────┐
#          ▼                   ▼
#      rowTag do Spark     ancestrais
#          │                   │
#          └─────────┬─────────┘
#                    ▼
#             objeto VARIANT
# ============================================================
# XML -> Spark rowTag + contexto ancestral streaming
#      -> VARIANT -> display()
#
# NÃO grava tabela Delta.
#
# Estratégia:
#
#   1. Spark lê diretamente o ROW_TAG.
#   2. Um parser streaming lê somente a árvore estrutural
#      necessária para descobrir os ancestrais.
#   3. O parser NÃO constrói o objeto inteiro do rowTag.
#   4. Para cada ocorrência do rowTag ele gera:
#
#          arquivo + ordinal + ancestrais
#
#   5. Spark gera o mesmo ordinal para as linhas do rowTag.
#   6. Fazemos JOIN:
#
#          arquivo + ordinal
#
#   7. Montamos:
#
#       {
#         "empresa": {...},
#         "departamento": {...},
#         "funcionario": {...}
#       }
#
#   8. Convertemos para VARIANT.
#
# ============================================================


from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)

import xml.etree.ElementTree as ET
from io import BytesIO
import json


# ============================================================
# 1. CONFIGURAÇÃO
# ============================================================

ROW_TAG = "funcionario"

INPUT_PATH = "dbfs:/tmp/xml/input/*.xml"


# ============================================================
# 2. NORMALIZAÇÃO DE TAG
# ============================================================

def normalize_tag(tag):
    """
    Remove namespace XML.

    Exemplo:

        {http://empresa.com}empresa

    vira:

        empresa
    """

    if "}" in tag:
        return tag.split("}", 1)[1]

    return tag


# ============================================================
# 3. CRIA SOMENTE O OBJETO DO ANCESTRAL
# ============================================================
#
# IMPORTANTE:
#
# Esta função NÃO processa os filhos.
#
# Isso é proposital.
#
# Queremos manter em memória somente:
#
#     empresa -> seus atributos
#     departamento -> seus atributos
#
# e NÃO:
#
#     funcionario -> todos os seus campos
#
# ============================================================

def ancestor_to_value(element):

    return {
        f"_{normalize_tag(key)}": value
        for key, value in element.attrib.items()
    }


# ============================================================
# 4. PARSER STREAMING DOS ANCESTRAIS
# ============================================================
#
# O parser percorre o XML usando iterparse().
#
# Ele mantém somente o caminho atual:
#
#     empresa
#     empresa -> departamento
#     empresa -> departamento -> funcionario
#
# Quando encontra o rowTag:
#
#     NÃO lê o conteúdo do funcionario.
#
# Ele somente captura os ancestrais.
#
# ============================================================

def extract_ancestor_context(
    xml_bytes,
    row_tag
):

    stream = BytesIO(xml_bytes)

    stack = []

    ordinal = 0

    for event, element in ET.iterparse(
        stream,
        events=("start", "end")
    ):

        tag = normalize_tag(element.tag)

        # ====================================================
        # START
        # ====================================================

        if event == "start":

            stack.append(element)

            continue

        # ====================================================
        # END
        # ====================================================

        if tag == row_tag:

            ordinal += 1

            ancestors = {}

            # ------------------------------------------------
            # stack:
            #
            # [
            #     empresa,
            #     departamento,
            #     funcionario
            # ]
            #
            # stack[:-1]:
            #
            # [
            #     empresa,
            #     departamento
            # ]
            # ------------------------------------------------

            for ancestor in stack[:-1]:

                ancestor_tag = normalize_tag(
                    ancestor.tag
                )

                ancestor_value = ancestor_to_value(
                    ancestor
                )

                # ------------------------------------------------
                # Primeiro ancestral deste nome
                # ------------------------------------------------

                if ancestor_tag not in ancestors:

                    ancestors[ancestor_tag] = (
                        ancestor_value
                    )

                # ------------------------------------------------
                # Caso existam dois ancestrais com o mesmo
                # nome no caminho
                #
                # Ex:
                #
                # empresa
                #   filial
                #       filial
                #           funcionario
                # ------------------------------------------------

                else:

                    previous = ancestors[ancestor_tag]

                    if isinstance(previous, list):

                        previous.append(
                            ancestor_value
                        )

                    else:

                        ancestors[ancestor_tag] = [
                            previous,
                            ancestor_value
                        ]

            # ------------------------------------------------
            # NÃO tocamos nos filhos do rowTag.
            #
            # Apenas emitimos:
            #
            # ordinal + ancestrais
            # ------------------------------------------------

            yield (
                ordinal,
                json.dumps(
                    ancestors,
                    ensure_ascii=False,
                    separators=(",", ":")
                )
            )

            # ------------------------------------------------
            # Libera memória.
            # ------------------------------------------------

            element.clear()

        # ====================================================
        # REMOVE DA STACK
        # ====================================================

        stack.pop()


# ============================================================
# 5. LER ROWTAG DIRETAMENTE COM SPARK
# ============================================================
#
# O Spark XML fica responsável por interpretar:
#
# <funcionario>
#     <nome>...</nome>
# </funcionario>
#
# O conteúdo completo do funcionario permanece no Spark,
# e não em uma lista Python.
#
# ============================================================

rowtag_df = (
    spark.read
        .format("xml")
        .option("rowTag", ROW_TAG)
        .option("attributePrefix", "_")
        .load(INPUT_PATH)
)


# ============================================================
# 6. ADICIONAR CAMINHO DO ARQUIVO
# ============================================================

rowtag_df = (
    rowtag_df
        .withColumn(
            "_source_file",
            F.input_file_name()
        )
)


# ============================================================
# 7. OBSERVAÇÃO SOBRE ORDINAL
# ============================================================
#
# Precisamos saber qual foi:
#
#     funcionario #1
#     funcionario #2
#     funcionario #3
#
# dentro de cada arquivo.
#
# Não existe uma coluna ordinal fornecida pelo XML reader.
#
# Para esse protótipo, adicionamos o ordinal em RDD,
# preservando a ordem dos registros entregues pelo reader.
#
# IMPORTANTE:
#
# Em produção, recomendo trocar isso por uma chave explícita
# do XML, quando existir.
#
# ============================================================


original_columns = rowtag_df.columns


# ------------------------------------------------------------
# Cria índice dentro de cada partição
#
# zipWithIndex() gera um índice global da sequência.
# ------------------------------------------------------------

indexed_rdd = (
    rowtag_df
        .rdd
        .zipWithIndex()
        .map(
            lambda x: (
                x[0],
                x[1]
            )
        )
)


# ------------------------------------------------------------
# Volta para DataFrame.
# ------------------------------------------------------------

indexed_df = spark.createDataFrame(
    indexed_rdd,
    schema=StructType(
        [
            *rowtag_df.schema.fields,
            StructField(
                "_global_index",
                LongType(),
                False
            )
        ]
    )
)


# ============================================================
# 8. CRIAR ORDINAL POR ARQUIVO
# ============================================================
#
# Como os arquivos são processados independentemente,
# usamos row_number() dentro do arquivo.
#
# ============================================================

from pyspark.sql.window import Window


window_spec = (
    Window
        .partitionBy("_source_file")
        .orderBy("_global_index")
)


rowtag_indexed_df = (
    indexed_df
        .withColumn(
            "_ordinal",
            F.row_number().over(
                window_spec
            )
        )
)


# ============================================================
# 9. LER SOMENTE OS ARQUIVOS NECESSÁRIOS PARA CONTEXTO
# ============================================================
#
# Neste ponto usamos binaryFile APENAS para executar
# o parser estrutural.
#
# O objetivo NÃO é interpretar o rowTag.
#
# O parser irá parar conceitualmente no rowTag e guardar
# somente:
#
#     ancestrais
#
# e não o conteúdo do rowTag.
#
# ============================================================

files_df = (
    spark.read
        .format("binaryFile")
        .load(INPUT_PATH)
        .select(
            F.col("path"),
            F.col("content")
        )
)


# ============================================================
# 10. CRIAR SCHEMA DE CONTEXTO
# ============================================================

ancestor_schema = StructType(
    [
        StructField(
            "source_file",
            StringType(),
            False
        ),

        StructField(
            "ordinal",
            LongType(),
            False
        ),

        StructField(
            "ancestors_json",
            StringType(),
            False
        )
    ]
)


# ============================================================
# 11. PROCESSAR ARQUIVOS PARA CONTEXTO
# ============================================================

def process_context(iterator):

    for row in iterator:

        source_file = row["path"]

        xml_content = row["content"]

        try:

            # ------------------------------------------------
            # Parser streaming.
            #
            # O generator devolve um registro de cada vez.
            #
            # Não existe:
            #
            # results = []
            #
            # ------------------------------------------------

            for ordinal, ancestors_json in (
                extract_ancestor_context(
                    xml_content,
                    ROW_TAG
                )
            ):

                yield (
                    source_file,
                    ordinal,
                    ancestors_json
                )

        except Exception as exc:

            raise RuntimeError(
                f"Erro processando contexto: "
                f"{source_file}"
            ) from exc


# ============================================================
# 12. EXECUTAR PARSER DISTRIBUÍDO
# ============================================================

ancestor_df = (
    files_df
        .rdd
        .mapPartitions(
            process_context
        )
)


ancestor_df = spark.createDataFrame(
    ancestor_df,
    schema=ancestor_schema
)


# ============================================================
# 13. CONVERTER JSON DOS ANCESTRAIS
# ============================================================

ancestor_df = (
    ancestor_df
        .withColumn(
            "_ancestors",
            F.parse_json(
                F.col("ancestors_json")
            )
        )
        .drop("ancestors_json")
)


# ============================================================
# 14. JOIN ROWTAG + CONTEXTO
# ============================================================
#
# Chave:
#
#     source_file
#     ordinal
#
# ============================================================

joined_df = (
    rowtag_indexed_df.alias("row")
        .join(
            ancestor_df.alias("anc"),
            (
                F.col("row._source_file")
                ==
                F.col("anc.source_file")
            )
            &
            (
                F.col("row._ordinal")
                ==
                F.col("anc.ordinal")
            ),
            "left"
        )
)


# ============================================================
# 15. TRANSFORMAR O ROWTAG EM OBJETO
# ============================================================
#
# Como o rowTag foi lido diretamente pelo Spark,
# agora transformamos somente a Row Spark em JSON.
#
# NÃO fizemos isso durante o parsing do XML.
#
# ============================================================

rowtag_columns = [
    column
    for column in original_columns
]


rowtag_struct = F.struct(
    *[
        F.col(f"row.{column}").alias(column)
        for column in rowtag_columns
    ]
)


# ------------------------------------------------------------
# Caso o Spark tenha criado um campo "_VALUE", ele permanece.
# ------------------------------------------------------------

rowtag_json = F.to_json(
    rowtag_struct
)


# ============================================================
# 16. MONTAR OBJETO FINAL
# ============================================================
#
# Começamos com os ancestrais.
#
# Depois adicionamos:
#
#     funcionario
#
# ============================================================

final_df = (
    joined_df
        .withColumn(
            "_rowtag_object",
            F.parse_json(
                rowtag_json
            )
        )
)


# ============================================================
# 17. CONSTRUIR JSON FINAL
# ============================================================
#
# Aqui usamos to_json + from_json/VARIANT para combinar
# os objetos.
#
# Uma forma simples e robusta no Databricks é utilizar
# parse_json com concat de objetos JSON.
#
# ============================================================

final_df = (
    final_df
        .select(
            F.col("row._source_file")
                .alias("source_file"),

            F.lit(ROW_TAG)
                .alias("rowTag"),

            F.to_json(
                F.struct(
                    F.col("anc._ancestors")
                        .alias("ancestors"),

                    F.col("_rowtag_object")
                        .alias("rowtag")
                )
            ).alias("_temporary")
        )
)


# ============================================================
# 18. NOTA
# ============================================================
#
# O objeto acima ainda possui:
#
# {
#   "ancestors": {...},
#   "rowtag": {...}
# }
#
# Queremos:
#
# {
#   "empresa": {...},
#   "departamento": {...},
#   "funcionario": {...}
# }
#
# Para isso usamos uma expressão SQL que combina os objetos.
#
# ============================================================


final_df.createOrReplaceTempView(
    "_xml_intermediate"
)


# ============================================================
# 19. CONSTRUIR VARIANT FINAL
# ============================================================

result_df = spark.sql(f"""
    SELECT
        source_file,
        rowTag,

        parse_json(
            concat(
                '{{',

                CASE
                    WHEN get_json_object(
                        _temporary,
                        '$.ancestors'
                    ) IS NOT NULL
                    AND get_json_object(
                        _temporary,
                        '$.ancestors'
                    ) <> '{{}}'
                    THEN
                        regexp_replace(
                            regexp_replace(
                                get_json_object(
                                    _temporary,
                                    '$.ancestors'
                                ),
                                '^\\\\{{',
                                ''
                            ),
                            '\\\\}}$',
                            ''
                        )
                    ELSE ''
                END,

                CASE
                    WHEN get_json_object(
                        _temporary,
                        '$.ancestors'
                    ) IS NOT NULL
                    AND get_json_object(
                        _temporary,
                        '$.ancestors'
                    ) <> '{{}}'
                    THEN ','
                    ELSE ''
                END,

                '"{ROW_TAG}":',

                get_json_object(
                    _temporary,
                    '$.rowtag'
                ),

                '}}'
            )
        ) AS data

    FROM _xml_intermediate
""")


# ============================================================
# 20. SCHEMA
# ============================================================

result_df.printSchema()


# ============================================================
# 21. DISPLAY FINAL
# ============================================================

display(
    result_df
)


# ============================================================
# 22. DISPLAY LEGÍVEL
# ============================================================

display(
    result_df.select(
        "rowTag",
        F.to_json(
            F.col("data")
        ).alias("data")
    )
)