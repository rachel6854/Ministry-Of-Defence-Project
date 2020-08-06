from bplustree import create_bplustree, insert, delete, update
import db_api
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type
import os.path

from dataclasses_json import dataclass_json
import operator

DB_ROOT = Path('db_files')


@dataclass_json
@dataclass
class DBField:
    name: str
    type: Type


@dataclass_json
@dataclass
class SelectionCriteria:
    field_name: str
    operator: str
    value: Any


@dataclass_json
@dataclass
class DBTable:
    name: str
    fields: List[DBField]
    key_field_name: str

    def __init__(self, table_name, fields=None, key_field_name=None):
        self.__table_file__, self.__matadata_file__ = self.create_table_file(table_name, fields, key_field_name)
        self.metadata = self.read_metadata()
        self.indexes = self.read_indexes()

    def read_table(self):
        with open(f"{self.__table_file__}", "r") as table_file:
            return json.load(table_file)

    def read_index(self, name_of_index_file):
        with open(name_of_index_file, "r") as index_file:
            return json.load(index_file)

    def read_indexes(self):
        result = []
        for index in self.metadata.get("indexes"):
            index_file = f"{DB_ROOT}/{index}_index.json"
            result.append(self.read_index(index_file))
        return result

    def read_metadata(self):
        with open(f"{self.__matadata_file__}", "r") as metadata_file:
            return json.load(metadata_file)

    def is_file_exist(self, fname):
        return os.path.isfile(fname)

    def create_table_file(self, table_name, fields, key_field_name):
        if not self.is_file_exist(DB_ROOT / f"{table_name}.json"):
            with (DB_ROOT / f"{table_name}.json").open("w") as table_file:
                json.dump([], table_file)

            fields_lst = {}
            for x in fields:
                fields_lst.update({x.name: x.type.__name__})
            with (DB_ROOT / f"{table_name}_metadata.json").open("w") as metadata_file:
                json.dump({"fields": fields_lst, "key": key_field_name, "indexes": []}, metadata_file)

        return f"{DB_ROOT}/{table_name}.json", f"{DB_ROOT}/{table_name}_metadata.json"

    def count(self) -> int:
        return len(self.read_table())

    def validate_values(self, values):
        fields = self.metadata.get("fields")

        for val in values:
            if fields.get(val) is None:
                raise Exception("ERROR! Unknown column name")
            if not isinstance(values.get(val), eval(fields.get(val))):
                raise Exception("ERROR! No match type to type", eval(fields.get(val)))

    def insert_to_index_file(self, values, primary_key, offset):
        for i, index in enumerate(self.indexes):
            key_of_index = self.metadata.get("indexes")[i]
            if values.get(key_of_index) is not None:
                insert(index, (values.get(key_of_index), values.get(primary_key)), offset)

            with (DB_ROOT / f"{key_of_index}_index.json").open("w") as index_file:
                json.dump(index, index_file)

    def insert_record(self, values: Dict[str, Any]) -> None:
        self.validate_values(values)

        data = self.read_table()

        primary_key = self.metadata.get("key")
        for i in data:
            if i.get(primary_key) == values.get(primary_key):
                raise ValueError

        data.append(values)

        with open(f"{self.__table_file__}", "w") as table_file:
            json.dump(data, table_file)

        self.insert_to_index_file(values, primary_key, len(data) - 1)

    def delete_from_index_file(self, key, name_of_index):
        index = self.indexes[self.metadata.get("indexes").index(name_of_index)]
        delete(index, key)

        with (DB_ROOT / f"{name_of_index}_index.json").open("w") as index_file:
            json.dump(index, index_file)

    def delete_record(self, key: Any) -> None:
        primary_key = self.metadata.get("key")

        data = self.read_table()

        data_after_remove = [x for x in data if x.get(primary_key) != key]
        if len(data_after_remove) == len(data):
            raise ValueError

        with open(f"{self.__table_file__}", "w") as table_file:
            json.dump(data_after_remove, table_file)

        self.delete_from_index_file((key, key), primary_key)

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        data = self.read_table()
        primary_key = self.metadata.get("key")
        data_after_remove = []

        for x in data:
            if not self.should_be_selected(criteria, x):
                data_after_remove.append(x)
            else:
                for key in x:
                    if key in self.metadata.get("indexes"):  # if key has an index
                        self.delete_from_index_file((x.get(key), x.get(primary_key)), key)

        with open(f"{self.__table_file__}", "w") as table_file:
            json.dump(data_after_remove, table_file)

    def get_record(self, key: Any) -> Dict[str, Any]:
        primary_key = self.metadata.get("key")

        data = self.read_table()

        for x in data:
            if x.get(primary_key) == key:
                return x

    def update_index_file(self, key, new_value, key_of_index):
        index = self.indexes[self.metadata.get("indexes").index(key_of_index)]
        update(index, key, new_value)

        with (DB_ROOT / f"{key_of_index}_index.json").open("w") as index_file:
            json.dump(index, index_file)

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        self.validate_values(values)
        primary_key = self.metadata.get("key")

        data = self.read_table()

        data_after_update = []
        for x in data:
            if x.get(primary_key) == key:
                for val in values:
                    x[val] = values[val]

                    if values[val] in self.metadata.get("indexes"):  # if key has an index
                        self.update_index_file((x[val], x[primary_key]), values[val], val)

            data_after_update.append(x)

        with open(f"{self.__table_file__}", "w") as table_file:
            json.dump(data_after_update, table_file)

    def cmp(self, arg1, op: str, arg2):
        ops = {
            '<': operator.lt,
            '<=': operator.le,
            '==': operator.eq,
            '!=': operator.ne,
            '>=': operator.ge,
            '>': operator.gt
        }
        operation = ops.get(op)
        return operation(arg1, arg2)

    def should_be_selected(self, criteria, x):
        for c in criteria:
            if not self.cmp(x.get(c.field_name), c.operator if c.operator != '=' else '==', c.value):
                return False
        return True

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        data = self.read_table()

        return [x for x in data if self.should_be_selected(criteria, x)]

    def create_index_metadata_file(self, index, field_to_index):
        # insert to metadata
        self.metadata["indexes"].append(field_to_index)
        with open(f"{self.__matadata_file__}", "w") as metadata_file:
            json.dump(self.metadata, metadata_file)

        # create index file
        with (DB_ROOT / f"{field_to_index}_index.json").open("w") as index_file:
            json.dump(index, index_file)

        self.indexes = self.read_indexes()

    def create_index(self, field_to_index: str) -> None:
        if field_to_index not in self.metadata.get("indexes"):
            data = self.read_table()
            index = create_bplustree()
            for offset, item in enumerate(data):
                insert(index, item.get(field_to_index), offset)

            self.create_index_metadata_file(index, field_to_index)


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    def __init__(self):
        self.__metadata_file__ = self.create_metadata()
        self.metadata = self.read_metadata()

    def read_metadata(self):
        with (DB_ROOT / "database_metadata.json").open() as metadata_file:
            data = json.load(metadata_file)
        return data

    def is_file_exist(self, fname):
        return os.path.isfile(fname)

    def create_metadata(self):
        if not self.is_file_exist(f"{DB_ROOT}/database_metadata.json"):
            with (DB_ROOT / "database_metadata.json").open("w") as metadata_file:
                json.dump([], metadata_file)
        return f"{DB_ROOT}/database_metadata.json"

    def insert_to_metadata(self, table_name):
        if table_name in self.metadata:
            raise Exception(f"{table_name} already exist")
        with (DB_ROOT / "database_metadata.json").open("w") as metadata_file:
            self.metadata.append(table_name)
            json.dump(self.metadata, metadata_file)

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        if key_field_name not in [x.name for x in fields]:
            raise ValueError
        self.insert_to_metadata(table_name)

        table = DBTable(table_name, fields, key_field_name)
        table.create_index(key_field_name)
        return table

    def num_tables(self) -> int:
        return len(self.metadata)

    def get_table(self, table_name: str) -> DBTable:
        if table_name not in self.metadata:
            raise Exception(f"{table_name} doesn't exist")
        return DBTable(table_name)

    def delete_table(self, table_name: str) -> None:
        if table_name in self.metadata:
            self.metadata.remove(table_name)
            with (DB_ROOT / "database_metadata.json").open("w") as metadata_file:
                json.dump(self.metadata, metadata_file)
            os.remove(f"{DB_ROOT}/{table_name}.json")
            os.remove(f"{DB_ROOT}/{table_name}_metadata.json")

    def get_tables_names(self) -> List[Any]:
        return self.metadata

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        # validte tables names
        # select id, name, type from g join j on pid=pid
        raise NotImplementedError
