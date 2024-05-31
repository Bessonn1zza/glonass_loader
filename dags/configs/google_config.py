DEFAULT_TASK_CONFIG = {
    'conn_id': 'google-service-account',
    'export_to': {
        'format': 'postgres',
        'prefix': 'googlesheets',
        'src_schema': 'src',
        'stg_schema': 'stg',
        'ods_schema': 'ods',
        'conn_id': 'postgres-dev'
    }
}

GOOGLE_SHEET_CONFIG = [
    # Производительность / Экскаваторы
    {
        "request_kwargs": {
            "sheet_id": "18gIEGHDY0Hf4KDu7Oj1oSPeU8LWLqrYpyoK4pjvd1Ac",
            "sheet_gid": 1966891340,
            "field_mapping": {
                "calendar_dt": 0,
                "shift_id": 1,
                "object_nm": 2,
                "cargo_volume": 3,
                "working_hours": 4,
                "fuel_start_litres": 5,
                "fuel_end_litres": 6,
                "fueling_litres": 7,
                "notes": 8
            }
        },
        "extract_kwargs": {
            "table_name": "performance_excavator_log",
            "primary_keys": [
                "calendar_dt",
                "shift_id",
                "object_nm"
            ]
        }
    },
    # Производительность / Самосвалы
    {
        "request_kwargs": {
            "sheet_id": "18gIEGHDY0Hf4KDu7Oj1oSPeU8LWLqrYpyoK4pjvd1Ac",
            "sheet_gid": 381068630,
            "field_mapping": {
                "calendar_dt": 0,
                "shift_id": 1,
                "object_nm": 2,
                "mileage_km": 3,
                "cargo_volume": 4,
                "working_hours": 5,
                "fuel_start_litres": 6,
                "fuel_end_litres": 7,
                "fueling_litres": 8,
                "notes": 9
            }
        },
        "extract_kwargs": {
            "table_name": "performance_truck_log",
            "primary_keys": [
                "calendar_dt",
                "shift_id",
                "object_nm"
            ]
        }
    },
    # Производительность / Стоимость топлива
    {
        "request_kwargs": {
            "sheet_id": "18gIEGHDY0Hf4KDu7Oj1oSPeU8LWLqrYpyoK4pjvd1Ac",
            "sheet_gid": 1137840738,
            "field_mapping": {
                "calendar_dt": 0,
                "price_rub": 1,
                "is_price_per_ton_flg": 2
            }
        },
        "extract_kwargs": {
            "table_name": "fuel_cost_log",
            "primary_keys": [
                "calendar_dt"
            ]
        }
    },
    # Плотность топлива
    {
        "request_kwargs": {
            "sheet_id": "1SVm4J8uMSEBncwHJg6Vzjf5JxZfKpynwpHXfHs0pshk",
            "sheet_gid": 1984146964,
            "field_mapping": {
                "calendar_dt": 0,
                "fuel_density": 1
            }
        },
        "extract_kwargs": {
            "table_name": "fuel_density_log",
            "primary_keys": [
                "calendar_dt"
            ]
        }
    },
    # График работы
    {
        "request_kwargs": {
            "sheet_id": "19JbmHmSXDtnuhSrlPLLCT5BngRGa6ED4s1yXWzdlVF4",
            "field_mapping": {
                "calendar_dt": 0,
                "shift_id": 1,
                "object_nm": 2,
                "employee_nm": 3
            }
        },
        "extract_kwargs": {
            "table_name": "work_schedule_log",
            "primary_keys": [
                "calendar_dt",
                "shift_id",
                "object_nm"
            ]
        }
    }
]