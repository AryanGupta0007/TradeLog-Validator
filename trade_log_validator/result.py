from dataclasses import dataclass
from typing import Any


@dataclass
class CheckResult:
    name: str
    segment: str
    status: str
    message: str
    details: Any = None
    issue_severity: dict = None  
