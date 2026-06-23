from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class MacroResult:
    data: pd.DataFrame
    metadata: dict[str, Any] = field(default_factory=dict)
    provider: str = ""
    dataset_id: str = ""
