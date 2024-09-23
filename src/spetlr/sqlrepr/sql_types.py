from typing import Union

from pyspark.sql.types import (
    ArrayType,
    DataType,
    DecimalType,
    MapType,
    StructField,
    StructType,
)


def repr_sql_types(obj: Union[StructField, DataType]):
    if isinstance(obj, StructField):
        return (
            (
                f"{obj.__class__.__name__}(name={repr(obj.name)}, "
                f"dataType={repr_sql_types(obj.dataType)}"
            )
            + (f", nullable={repr(obj.nullable)}" if not obj.nullable else "")
            + (f", metadata={repr(obj.metadata)}" if obj.metadata else "")
            + ")"
        )

    if isinstance(obj, ArrayType):
        return (
            f"{obj.__class__.__name__}("
            f"elementType={repr_sql_types(obj.elementType)})"
        )

    if isinstance(obj, StructType):
        fields_part = ", ".join(repr_sql_types(f) for f in obj.fields)
        return f"{obj.__class__.__name__}(" f"fields=[{fields_part}])"

    if isinstance(obj, MapType):
        return (
            f"{obj.__class__.__name__}("
            f"keyType={repr_sql_types(obj.keyType)}, "
            f"valueType={repr_sql_types(obj.valueType)}, "
            f"valueContainsNull={repr(obj.valueContainsNull)})"
        )

    if isinstance(obj, DecimalType):
        DecimalType()
        return (
            f"{obj.__class__.__name__}("
            f"precision={repr(obj.precision)}, "
            f"scale={repr(obj.scale)})"
        )

    # still here? Boring type
    return f"{obj.__class__.__name__}()"
