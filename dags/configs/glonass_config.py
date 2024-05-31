DEFAULT_TASK_CONFIG = {
    'conn_id': 'glonass-ask-api',
    'export_to': {
        'format': 'postgres',
        'prefix': 'glonass',
        'src_schema': 'src',
        'stg_schema': 'stg',
        'ods_schema': 'ods',
        'conn_id': 'postgres-prod'
    }
}

GLONASS_CONFIG = [
    # Справочник видов груза
    {
        "task_kwargs": {
            "method": "sync",
            "object_type": "dict",
            "data_path": "cargos"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "readdicts", 
                    "dicts": "cargos"
                }
        },
        "extract_kwargs": {
            "table_name": "readdicts_cargos",
            "primary_keys": [
                "id"
            ]
        }
    },
    # Справочник техники
    {
        "task_kwargs": {
            "method": "sync",
            "object_type": "dict",
            "data_path": "objects"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "readdicts", 
                    "dicts": "objects"
                }
        },
        "extract_kwargs": {
            "table_name": "readdicts_objects",
            "primary_keys": [
                "id"
            ]
        }
    },
    # Типы пользовательских событий
    {
        "task_kwargs": {
            "method": "sync",
            "object_type": "dict",
            "data_path": "manualeventtypes"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "readdicts", 
                    "dicts": "manualeventtypes"
                }
        },
        "extract_kwargs": {
            "table_name": "readdicts_manualeventtypes",
            "primary_keys": [
                "id"
            ]
        }
    },
    # Причины пользовательских событий
    {
        "task_kwargs": {
            "method": "sync",
            "object_type": "dict",
            "data_path": "manualeventcauses"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "readdicts", 
                    "dicts": "manualeventcauses"
                }
        },
        "extract_kwargs": {
            "table_name": "readdicts_manualeventcauses",
            "primary_keys": [
                "id"
            ]
        }
    },
    # Модели
    {
        "task_kwargs": {
            "method": "sync",
            "object_type": "dict",
            "data_path": "models"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "readdicts", 
                    "dicts": "models"
                }
        },
        "extract_kwargs": {
            "table_name": "readdicts_models",
            "primary_keys": [
                "id"
            ]
        }
    },
    # Типы моделей
    {
        "task_kwargs": {
            "method": "sync",
            "object_type": "dict",
            "data_path": "modeltypes"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "readdicts", 
                    "dicts": "modeltypes"
                }
        },
        "extract_kwargs": {
            "table_name": "readdicts_modeltypes",
            "primary_keys": [
                "id"
            ]
        }
    },
    # Зоны
    {
        "task_kwargs": {
            "method": "sync",
            "object_type": "dict",
            "data_path": "zones"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "readdicts", 
                    "dicts": "zones"
                }
        },
        "extract_kwargs": {
            "table_name": "readdicts_zones",
            "primary_keys": [
                "id"
            ]
        }
    },
    # Группы зон
    {
        "task_kwargs": {
            "method": "sync",
            "object_type": "dict",
            "data_path": "zonegroups"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "readdicts", 
                    "dicts": "zonegroups"
                }
        },
        "extract_kwargs": {
            "table_name": "readdicts_zonegroups",
            "primary_keys": [
                "id"
            ]
        }
    },
    # Список горных участков сервера
    {
        "task_kwargs": {
            "method": "sync",
            "object_type": "dict",
            "data_path": "areas"
        },
        "request_kwargs": {
            "module": "mining",
            "args": {
                    "func": "GetMiningAreas"
                }
        },
        "extract_kwargs": {
            "table_name": "mining_areas",
            "primary_keys": [
                "id"
            ]
        }
    },
    # Получение ремонтов
    {
        "task_kwargs": {
            "method": "async",
            "object_type": "report",
            "data_path": "repairs"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "getrepairs",
                    "datetype": 0,
                    "beg": "{{ start_date }}",
                    "end": "{{ end_date }}"
                }
        },
        "extract_kwargs": {
            "table_name": "repairs",
            "primary_keys": [
                "id"
            ]
        }
    },
    # Детализированная таблица с простоями
    {
        "task_kwargs": {
            "method": "async",
            "object_type": "report",
            "data_path": "krv_manual_events"
        },
        "request_kwargs": {
            "module": "mining",
            "args": {
                    "func": "GetKRVManualEvents",
                    "begin": "{{ start_date }}",
                    "end": "{{ end_date }}",
                    # "mobj": "{{ objects }}"
                }
        },
        "extract_kwargs": {
            "table_name": "mining_krv_manual_events",
            "primary_keys": [
                "id"
            ]
        }
    },
    # Детализированная таблица с рейсами с учетом корректировок диспетчера
    {
        "task_kwargs": {
            "method": "async",
            "object_type": "report",
            "data_path": "krvsms"
        },
        "request_kwargs": {
            "module": "mining",
            "args": {
                    "func": "getKRVSM",
                    "begin": "{{ start_date }}",
                    "end": "{{ end_date }}",
                    "maid": "{{ areas }}"
                }
        },
        "extract_kwargs": {
            "table_name": "mining_krvsms",
            "primary_keys": [
                "id"
            ]
        }
    },
    # 101. Отчет по рейсам
    {
        "task_kwargs": {
            "method": "async",
            "object_type": "report"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "report",
                    "id": "9f5ebbd2-27e0-4549-9faa-eeeadf295141",
                    "sid": "3b955c7c-03de-4298-ae2f-2a55d2915cf2",
                    # "objects": "{{ objects }}",
                    "begin": "{{ start_date }}",
                    "end": "{{ end_date }}",
                    "spantype": 2,
                    "offset": 420,
                    "attr": "stage_meters=100"
                }
        },
        "extract_kwargs": {
            "table_name": "report_routes",
            "primary_keys": [
                "_date",
                "_mobileObjectId",
                "_info",
                "_sm"
            ]
        }
    },
    # 105a. Отчет по простоям (тревоги и пользовательские события)
    {
        "task_kwargs": {
            "method": "async",
            "object_type": "report"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "report",
                    "id": "090992f0-9084-46ea-9096-28cba53ad24e",
                    "sid": "0b24dc9f-dde3-458f-befe-a0a9766a8754",
                    # "objects": "{{ objects }}",
                    "begin": "{{ start_date }}",
                    "end": "{{ end_date }}",	
                    "spantype": 2,
                    "offset": 420,
                    "attr": "minIdleTime=00:05:00"
                }
        },
        "extract_kwargs": {
            "table_name": "report_idle_alerts",
            "primary_keys": [
                "_date",
                "_mobileObjectId",
                "_info",
                "_sm"
            ]
        }
    },
    # 200. Отчет по топливу
    {
        "task_kwargs": {
            "method": "async",
            "object_type": "report"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "report",
                    "id": "8a14f4f4-cf2e-4da3-ba4b-8ef47df85d88",
                    "sid": "f58be4cc-2c8f-4db6-a8ab-a83daf4ff610",
                    # "objects": "{{ objects }}",
                    "begin": "{{ start_date }}",
                    "end": "{{ end_date }}",	
                    "spantype": 2,
                    "offset": 420,
                    "attr": "fuel_required=Да"
                }
        },
        "extract_kwargs": {
            "table_name": "report_fuel_usage",
            "primary_keys": [
                "_date",
                "_mobileObjectId",
                "_info",
                "_sm"
            ]
        }
    },
    # 100. Отчет о работе техники
    {
        "task_kwargs": {
            "method": "async",
            "object_type": "report"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "report",
                    "id": "d37f098e-f3a0-4bfc-b117-d34a78064986",
                    "sid": "1ecc2385-47ed-4596-adc3-f3e69b0d262e",
                    # "objects": "{{ objects }}",
                    "begin": "{{ start_date }}",
                    "end": "{{ end_date }}",
                    "spantype": 2,
                    "offset": 420,
                    "attr": "with_engSens=True"
                }
        },
        "extract_kwargs": {
            "table_name": "report_equipment_activity",
            "primary_keys": [
                "_date",
                "_mobileObjectId",
                "_info",
                "_sm"
            ]
        }
    },
    # 201. Отчет по сливам/заправкам
    {
        "task_kwargs": {
            "method": "async",
            "object_type": "report"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "report",
                    "id": "7a25722c-873c-4f94-a575-c7058321a17b",
                    "sid": "f86b06fa-6615-456d-a3de-a6b7d204c592",
                    # "objects": "{{ objects }}",
                    "begin": "{{ start_date }}",
                    "end": "{{ end_date }}",
                    "spantype": 2,
                    "offset": 420,
                    "attr": "min_value=10"
                }
        },
        "extract_kwargs": {
            "table_name": "report_refuels",
            "primary_keys": [
                "_date",
                "_mobileObjectId",
                "_info",
                "_sm"
            ]
        }
    },
    # 302. Отчет по вхождению в зоны
    {
        "task_kwargs": {
            "method": "async",
            "object_type": "report"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "report",
                    "id": "e48ad6e9-1fe0-4677-8bff-9d8c74c8c32a",
                    "sid": "895ea0d6-ad08-43cd-afdd-8fc412b034cc",
                    # "objects": "{{ objects }}",
                    "begin": "{{ start_date }}",
                    "end": "{{ end_date }}",
                    "spantype": 2,
                    "offset": 420,
                    "attr": "min_entry_time=00:00:30",
                    "additional": "{{ additional }}"
                }
        },
        "extract_kwargs": {
            "table_name": "report_zone_entries",
            "primary_keys": [
                "_date",
                "_mobileObjectId",
                "_info",
                "_sm"
            ]
        }
    },
    # 113. Отчет о работе ДВС
    {
        "task_kwargs": {
            "method": "async",
            "object_type": "report"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "report",
                    "id": "570169c3-80e6-4a2b-bf47-2615cdc99728",
                    "sid": "9682a1ff-0e2c-4969-b1cc-fa08eb4313fd",
                    # "objects": "{{ objects }}",
                    "begin": "{{ start_date }}",
                    "end": "{{ end_date }}",
                    "spantype": 2,
                    "offset": 420
                }
        },
        "extract_kwargs": {
            "table_name": "report_engine_activity",
            "primary_keys": [
                "_date",
                "_mobileObjectId",
                "_info",
                "_sm"
            ]
        }
    },
    # 301. Отчет по превышениям скорости (45 км/ч)
    {
        "task_kwargs": {
            "method": "async",
            "object_type": "report"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "report",
                    "id": "524194e1-1e7d-4107-b7b8-e1f7306d7657",
                    "sid": "791d1761-6e66-45eb-99d5-f68f76273af8",
                    # "objects": "{{ objects }}",
                    "begin": "{{ start_date }}",
                    "end": "{{ end_date }}",	
                    "spantype": 2,
                    "offset": 420,
                    "attr": "max_norm_speed=45,max_speed=70"
                }
        }, 
        "extract_kwargs": {
            "table_name": "report_speeding_45kmh",
            "primary_keys": [
                "_date",
                "_mobileObjectId",
                "_info",
                "_sm"
            ]
        }
    },
    # 301. Отчет по превышениям скорости (50 км/ч)
    {
        "task_kwargs": {
            "method": "async",
            "object_type": "report"
        },
        "request_kwargs": {
            "module": "main",
            "args": {
                    "func": "report",
                    "id": "524194e1-1e7d-4107-b7b8-e1f7306d7657",
                    "sid": "791d1761-6e66-45eb-99d5-f68f76273af8",
                    # "objects": "{{ objects }}",
                    "begin": "{{ start_date }}",
                    "end": "{{ end_date }}",	
                    "spantype": 2,
                    "offset": 420,
                    "attr": "max_norm_speed=50,max_speed=70"
                }
        },
        "extract_kwargs": {
            "table_name": "report_speeding_50kmh",
            "primary_keys": [
                "_date",
                "_mobileObjectId",
                "_info",
                "_sm"
            ]
        }
    }
]