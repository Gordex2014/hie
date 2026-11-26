"""Resources that already are FHIR (NDJSON exports, Bundle files): pass them through.

Each resource must carry an `id` (delivered as PUT Type/id) or an `identifier`
(delivered as a conditional update), so re-sending a file updates instead of
duplicating.
"""

from hie import DataError


def transform(record, context):
    resource_type = record.get("resourceType")
    if not resource_type:
        raise DataError("record is not a FHIR resource (no resourceType)")
    if resource_type == "Bundle":
        raise DataError(
            "nested Bundle; read it as a Bundle file (format fhir_bundle, or a .json file with format fhir)"
        )
    if not record.get("id") and not record.get("identifier"):
        raise DataError(f"{resource_type} has neither id nor identifier")
    return record
