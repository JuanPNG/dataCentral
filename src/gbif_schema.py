gbif_schema = {
    "fields": [
        {
            "name": "accession",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "gbif_usageKey",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "species",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "decimalLatitude",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },
        {
            "name": "decimalLongitude",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },
        {
            "name": "geodeticDatum",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "coordinateUncertaintyInMeters",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },
        {
            "name": "eventDate",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "continent",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "gadm",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "level0",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {
                            "name": "name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "gid",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        }
                    ]
                },
                {
                    "name": "level1",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {
                            "name": "name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "gid",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        }
                    ]
                },
                {
                    "name": "level2",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {
                            "name": "name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "gid",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        }
                    ]
                },
                {
                    "name": "level3",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {
                            "name": "name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "gid",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        }
                    ]
                }
            ]
        },
        {
            "name": "countryCode",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "basisOfRecord",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "occurrenceStatus",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "occurrenceID",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "gbifID",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "issues",
            "type": "STRING",
            "mode": "REPEATED"
        },
        {
            "name": "kingdom",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "phylum",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "order",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "family",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "genus",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "scientificName",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "acceptedScientificName",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "taxonomicStatus",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "isSequenced",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "iucnRedListCategory",
            "type": "STRING",
            "mode": "NULLABLE"
        }
    ]
}