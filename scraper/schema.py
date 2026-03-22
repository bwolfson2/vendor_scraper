"""Unified output schema for all state spending data."""

from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class ContractRecord:
    """Normalized contract/spending record across all states."""
    state: str = ""
    state_abbr: str = ""
    agency_name: str = ""
    vendor_name: str = ""
    contract_id: str = ""
    contract_type: str = ""
    description: str = ""
    amount: Optional[float] = None
    start_date: str = ""
    end_date: str = ""
    procurement_method: str = ""
    commodity_category: str = ""
    source_url: str = ""
    raw_fields: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("raw_fields", None)
        return d

    @staticmethod
    def csv_headers() -> list[str]:
        return [
            "state", "state_abbr", "agency_name", "vendor_name",
            "contract_id", "contract_type", "description", "amount",
            "start_date", "end_date", "procurement_method",
            "commodity_category", "source_url",
        ]
