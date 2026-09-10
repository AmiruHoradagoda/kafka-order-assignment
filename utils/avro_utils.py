import io
import json

from fastavro import (
    parse_schema,
    schemaless_reader,
    schemaless_writer
)


def load_schema(schema_path):
    with open(schema_path, "r") as schema_file:
        schema = json.load(schema_file)

    return parse_schema(schema)


def serialize_avro(data, schema):
    buffer = io.BytesIO()

    schemaless_writer(
        buffer,
        schema,
        data
    )

    return buffer.getvalue()


def deserialize_avro(data, schema):
    buffer = io.BytesIO(data)

    return schemaless_reader(
        buffer,
        schema
    )