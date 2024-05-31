import json
import hashlib
from typing import Dict, Any, List, Optional
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class DataItem:
    row_pk: Dict[str, Any]
    row_data: Dict[str, Any]
    row_hash_key: str

class DataProcessor:
    def __init__(self, metadata: Dict[str, Any], content: List[Dict[str, Any]], primary_keys: List[str], path_to_data: Optional[str] = None, remove_duplicates: bool = True):
        self.metadata = metadata
        self.content = content
        self.primary_keys = primary_keys
        self.path_to_data = path_to_data
        self.remove_duplicates = remove_duplicates

    def extract_data(self, item):
        return item.get(self.path_to_data, {}) if self.path_to_data else item

    def process_items(self):
        merged_data_dict = defaultdict(list)
        data = self.extract_data(self.content)
        for item in data:
            hash_key = hashlib.sha512(json.dumps(item, sort_keys=True).encode()).hexdigest()
            pk_fields = {key: item[key] for key in self.primary_keys}
            data_fields = {key: item[key] for key in item if key not in self.primary_keys}
            data_item = DataItem(row_pk=pk_fields, row_data=data_fields, row_hash_key=hash_key)
            key_fields = tuple(item[key] for key in self.primary_keys)
            merged_data_dict[key_fields].append(data_item)

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        if self.remove_duplicates:
            items_with_dag_context = [{
                'message': json.dumps({
                    'ts': ts,
                    'hash': data_items[0].row_hash_key,
                    'pk': dict(zip(self.primary_keys, key_fields)),
                    'data': data_items[0].row_data
                })
            } for key_fields, data_items in merged_data_dict.items()]
        else:
            items_with_dag_context = [{
                'message': json.dumps({
                    'ts': ts,
                    'hash': data_item.row_hash_key,
                    'pk': dict(zip(self.primary_keys, key_fields)),
                    'data': data_item.row_data
                })
            } for key_fields, data_items in merged_data_dict.items() for data_item in data_items]

        if not items_with_dag_context:
            return [{**self.metadata, 'message': json.dumps({})}]

        return [dict(**self.metadata, **item) for item in items_with_dag_context]
    
metadata = {'dag_id': 'glonass_loader', 'dag_run_id': 'scheduled__2024-04-01T03:00:00+00:00', 'dag_start_dttm': datetime(2024, 4, 2, 20, 8, 35, 971216, tzinfo=timezone.utc)}
content = {'result': 'OK', 'cargos': [{'id': 'e11ffe3a-1022-42ec-aae0-eafb7d7e93c1', 'name': 'Глина', 'ent': '4756530d-c05a-47fd-8d29-ae8d3516b226', 'code': 'Глина', 'deleted': False, 'units': '', 'units_name': '', 'density': 0, 'attributes': {}}, {'id': '7d5874b0-8405-4408-bda0-ebc28968363a', 'name': 'Порода', 'ent': '4756530d-c05a-47fd-8d29-ae8d3516b226', 'code': 'ROCK', 'deleted': False, 'units': '', 'units_name': '', 'density': 0, 'attributes': {}}, {'id': '360e776d-f8a8-4160-bcbd-e9eee8514557', 'name': 'Уголь', 'ent': '4756530d-c05a-47fd-8d29-ae8d3516b226', 'code': 'Уголь', 'deleted': False, 'units': '', 'units_name': '', 'density': 0, 'attributes': {}}]}
processor = DataProcessor(metadata, content, ['id'], 'cargos')
result = processor.process_items()
print(result)