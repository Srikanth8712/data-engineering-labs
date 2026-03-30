import hashlib
import json


def mask_field(value: str) -> str:
    """SHA-256 hash a PII field — one-way, consistent, non-reversible."""
    return hashlib.sha256(str(value).encode()).hexdigest()[:12]


def mask_record(record: dict, pii_fields: list) -> dict:
    """Return a copy of record with specified fields masked."""
    masked = record.copy()
    for field in pii_fields:
        if field in masked:
            masked[field] = mask_field(masked[field])
    return masked


def mask_records(records: list, pii_fields: list) -> list:
    """Mask PII across a list of records."""
    return [mask_record(r, pii_fields) for r in records]


if __name__ == "__main__":
    sample = {"claimant_name": "John Smith", "ssn_last4": "4521", "claim_id": "CLM-001"}
    masked = mask_record(sample, ["claimant_name", "ssn_last4"])
    print("Original:", json.dumps(sample, indent=2))
    print("Masked:  ", json.dumps(masked, indent=2))