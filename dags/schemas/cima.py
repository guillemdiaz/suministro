ID_NOMBRE = [{"name": "id", "type": "INTEGER"}, {"name": "nombre", "type": "STRING"}]

ESTADO = {
    "name": "estado",
    "type": "RECORD",
    "fields": [
        {"name": "aut", "type": "INTEGER"},
        {"name": "susp", "type": "INTEGER"},
        {"name": "rev", "type": "INTEGER"},
    ],
}

DOCS = {
    "name": "docs",
    "type": "RECORD",
    "mode": "REPEATED",
    "fields": [
        {"name": "tipo", "type": "INTEGER"},
        {"name": "url", "type": "STRING"},
        {"name": "urlHtml", "type": "STRING"},
        {"name": "secc", "type": "BOOLEAN"},
        {"name": "fecha", "type": "INTEGER"},
    ],
}

FOTOS = {
    "name": "fotos",
    "type": "RECORD",
    "mode": "REPEATED",
    "fields": [
        {"name": "tipo", "type": "STRING"},
        {"name": "url", "type": "STRING"},
        {"name": "fecha", "type": "INTEGER"},
    ],
}

ATCS = {
    "name": "atcs",
    "type": "RECORD",
    "mode": "REPEATED",
    "fields": [
        {"name": "codigo", "type": "STRING"},
        {"name": "nombre", "type": "STRING"},
        {"name": "nivel", "type": "INTEGER"},
    ],
}

PRINCIPIOS_ACTIVOS = {
    "name": "principiosActivos",
    "type": "RECORD",
    "mode": "REPEATED",
    "fields": [
        {"name": "id", "type": "INTEGER"},
        {"name": "codigo", "type": "STRING"},
        {"name": "nombre", "type": "STRING"},
        {"name": "cantidad", "type": "STRING"},
        {"name": "unidad", "type": "STRING"},
        {"name": "orden", "type": "INTEGER"},
    ],
}

EXCIPIENTES = {
    "name": "excipientes",
    "type": "RECORD",
    "mode": "REPEATED",
    "fields": [
        {"name": "id", "type": "INTEGER"},
        {"name": "nombre", "type": "STRING"},
        {"name": "cantidad", "type": "STRING"},
        {"name": "unidad", "type": "STRING"},
        {"name": "orden", "type": "INTEGER"},
    ],
}

VIAS_ADMINISTRACION = {
    "name": "viasAdministracion",
    "type": "RECORD",
    "mode": "REPEATED",
    "fields": ID_NOMBRE,
}

DETALLE_PROBLEMA_SUMINISTRO = {
    "name": "detalleProblemaSuministro",
    "type": "RECORD",
    "fields": [
        {"name": "cn", "type": "STRING"},
        {"name": "fini", "type": "INTEGER"},
        {"name": "ffin", "type": "INTEGER"},
        {"name": "activo", "type": "BOOLEAN"},
        {"name": "observ", "type": "STRING"},
    ],
}

PRESENTACIONES_NESTED = {
    "name": "presentaciones",
    "type": "RECORD",
    "mode": "REPEATED",
    "fields": [
        {"name": "cn", "type": "STRING"},
        {"name": "nombre", "type": "STRING"},
        ESTADO,
        {"name": "comerc", "type": "BOOLEAN"},
        {"name": "psum", "type": "BOOLEAN"},
        DETALLE_PROBLEMA_SUMINISTRO,
    ],
}

# ---------------------------------- SCHEMAS ----------------------------------

PSUMINISTRO_SCHEMA = [
    {"name": "cn", "type": "STRING"},
    {"name": "nombre", "type": "STRING"},
    {"name": "tipoProblemaSuministro", "type": "INTEGER"},
    {"name": "fini", "type": "INTEGER"},
    {"name": "ffin", "type": "INTEGER"},
    {"name": "activo", "type": "BOOLEAN"},
    {"name": "observ", "type": "STRING"},
]

PRESENTACIONES_SCHEMA = [
    {"name": "nregistro", "type": "STRING"},
    {"name": "cn", "type": "STRING"},
    {"name": "nombre", "type": "STRING"},
    {"name": "pactivos", "type": "STRING"},
    {"name": "labtitular", "type": "STRING"},
    {"name": "labcomercializador", "type": "STRING"},
    {"name": "cpresc", "type": "STRING"},
    ESTADO,
    {"name": "comerc", "type": "BOOLEAN"},
    {"name": "conduc", "type": "BOOLEAN"},
    {"name": "receta", "type": "BOOLEAN"},
    {"name": "generico", "type": "BOOLEAN"},
    {"name": "triangulo", "type": "BOOLEAN"},
    {"name": "huerfano", "type": "BOOLEAN"},
    {"name": "biosimilar", "type": "BOOLEAN"},
    {"name": "nosustituible", "type": "RECORD", "fields": ID_NOMBRE},
    {"name": "psum", "type": "BOOLEAN"},
    {"name": "notas", "type": "BOOLEAN"},
    {"name": "materialesInf", "type": "BOOLEAN"},
    {"name": "ema", "type": "BOOLEAN"},
    DOCS,
    FOTOS,
    PRINCIPIOS_ACTIVOS,
    EXCIPIENTES,
    ATCS,
    {"name": "vtm", "type": "RECORD", "fields": ID_NOMBRE},
    {"name": "dcp", "type": "RECORD", "fields": ID_NOMBRE},
    {"name": "dcpf", "type": "RECORD", "fields": ID_NOMBRE},
    DETALLE_PROBLEMA_SUMINISTRO,
]

MEDICAMENTOS_SCHEMA = [
    {"name": "nregistro", "type": "STRING"},
    {"name": "nombre", "type": "STRING"},
    {"name": "pactivos", "type": "STRING"},
    {"name": "labtitular", "type": "STRING"},
    {"name": "labcomercializador", "type": "STRING"},
    {"name": "cpresc", "type": "STRING"},
    ESTADO,
    {"name": "comerc", "type": "BOOLEAN"},
    {"name": "receta", "type": "BOOLEAN"},
    {"name": "generico", "type": "BOOLEAN"},
    {"name": "conduc", "type": "BOOLEAN"},
    {"name": "triangulo", "type": "BOOLEAN"},
    {"name": "huerfano", "type": "BOOLEAN"},
    {"name": "biosimilar", "type": "BOOLEAN"},
    {"name": "nosustituible", "type": "RECORD", "fields": ID_NOMBRE},
    {"name": "psum", "type": "BOOLEAN"},
    {"name": "notas", "type": "BOOLEAN"},
    {"name": "materialesInf", "type": "BOOLEAN"},
    {"name": "ema", "type": "BOOLEAN"},
    DOCS,
    FOTOS,
    ATCS,
    PRINCIPIOS_ACTIVOS,
    EXCIPIENTES,
    VIAS_ADMINISTRACION,
    PRESENTACIONES_NESTED,
    {"name": "formaFarmaceutica", "type": "RECORD", "fields": ID_NOMBRE},
    {"name": "formaFarmaceuticaSimplificada", "type": "RECORD", "fields": ID_NOMBRE},
    {"name": "vtm", "type": "RECORD", "fields": ID_NOMBRE},
    {"name": "dosis", "type": "STRING"},
]

ATC_SCHEMA = [{"name": "codigo", "type": "STRING"}, {"name": "nombre", "type": "STRING"}]

LAB_SCHEMA = [{"name": "nombre", "type": "STRING"}]
