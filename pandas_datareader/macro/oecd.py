from __future__ import annotations

from xml.etree import ElementTree as ET

import pandas as pd

from pandas_datareader.io.jsdmx import read_jsdmx
from pandas_datareader.macro.base import (
    MacroClientBase,
    MacroNotFoundError,
    MacroSchemaError,
)
from pandas_datareader.macro.result import MacroResult

_MESSAGE = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}"
_STRUCTURE = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}"
_COMMON = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}"
_XML = "{http://www.w3.org/XML/1998/namespace}"


def _preferred_text(parent, tag):
    nodes = list(parent.findall(tag))
    if not nodes:
        return None
    for node in nodes:
        lang = node.get(f"{_XML}lang", "")
        if lang.lower().startswith("en"):
            return node.text
    return nodes[0].text


class OECDClient(MacroClientBase):
    provider = "oecd"
    dataflow_url = "https://sdmx.oecd.org/public/rest/dataflow/all/all/latest"
    data_url_template = (
        "https://sdmx.oecd.org/public/rest/data/{dataset}/{key}?format=jsondata"
    )

    def _parse_dataflows_xml(self, xml_bytes):
        root = ET.fromstring(xml_bytes)
        rows = []
        for flow in root.iter(_STRUCTURE + "Dataflow"):
            dataflow_id = flow.get("id")
            agency_id = flow.get("agencyID")
            version = flow.get("version")
            structure_ref = flow.find(f".//{_STRUCTURE}Structure/Ref")
            rows.append(
                {
                    "dataset_id": f"{agency_id},{dataflow_id},{version}",
                    "dataflow_id": dataflow_id,
                    "agency_id": agency_id,
                    "version": version,
                    "title": _preferred_text(flow, f"{_COMMON}Name"),
                    "description": _preferred_text(flow, f"{_COMMON}Description"),
                    "structure_id": (
                        None if structure_ref is None else structure_ref.get("id")
                    ),
                }
            )
        return pd.DataFrame(rows)

    @staticmethod
    def _extract_dimensions(payload):
        structure = payload["data"]["structures"][0]
        dimensions = []
        labels = {}
        for group_name, dims in structure.get("dimensions", {}).items():
            for dim in dims:
                values = {value["id"]: value["name"] for value in dim.get("values", [])}
                dimensions.append(
                    {
                        "group": group_name,
                        "id": dim.get("id"),
                        "label": dim.get("name"),
                        "values": values,
                    }
                )
                labels[dim.get("id")] = values
        return dimensions, labels

    def _build_result_from_payload(self, dataset_id, payload, query=None):
        data = read_jsdmx(payload)
        dimensions, labels = self._extract_dimensions(payload)
        structure = payload["data"]["structures"][0]
        metadata = {
            "title": structure.get("name"),
            "description": structure.get("description"),
            "updated_at": payload.get("meta", {}).get("prepared"),
            "dimensions": dimensions,
            "attributes": structure.get("attributes", {}),
            "labels": labels,
            "source": "oecd-sdmx-json-v2",
            "query": query or {},
            "notes": [],
            "raw": {
                "meta": payload.get("meta", {}),
                "errors": payload.get("errors", []),
            },
        }
        return MacroResult(
            data=data,
            metadata=metadata,
            provider=self.provider,
            dataset_id=dataset_id,
        )

    def _resolve_dataset(self, dataset):
        if "," in dataset:
            return dataset
        dataflows = self.search_datasets()
        exact = dataflows[dataflows["dataflow_id"] == dataset]
        if len(exact) == 1:
            return exact.iloc[0]["dataset_id"]
        raise MacroNotFoundError(f"Could not resolve OECD dataset: {dataset}")

    def _dataset_url(self, dataset_id, key="all", start=None, end=None):
        url = self.data_url_template.format(dataset=dataset_id, key=key)
        params = []
        if start is not None:
            params.append(f"startPeriod={self._normalize_date(start)}")
        if end is not None:
            params.append(f"endPeriod={self._normalize_date(end)}")
        if params:
            url = f"{url}&{'&'.join(params)}"
        return url

    def search_datasets(self, query=None, **kwargs):
        response = self._get(self.dataflow_url)
        result = self._parse_dataflows_xml(response.content)
        if query:
            q = query.lower()
            mask = (
                result["dataflow_id"].fillna("").str.lower().str.contains(q)
                | result["title"].fillna("").str.lower().str.contains(q)
                | result["description"].fillna("").str.lower().str.contains(q)
            )
            result = result.loc[mask].reset_index(drop=True)
        return result

    def describe_dataset(self, dataset, **kwargs):
        dataset_id = self._resolve_dataset(dataset)
        payload = self._get(
            f"{self._dataset_url(dataset_id)}&firstNObservations=1"
        ).json()
        result = self._build_result_from_payload(
            dataset_id,
            payload,
            query={"firstNObservations": 1},
        )
        return {
            "dataset_id": dataset_id,
            "title": result.metadata["title"],
            "description": result.metadata["description"],
            "updated_at": result.metadata["updated_at"],
            "dimensions": result.metadata["dimensions"],
            "attributes": result.metadata["attributes"],
            "labels": result.metadata["labels"],
            "source": result.metadata["source"],
            "query": result.metadata["query"],
            "notes": result.metadata["notes"],
            "raw": result.metadata["raw"],
        }

    def read(self, dataset, start=None, end=None, filters=None, **kwargs):
        dataset_id = self._resolve_dataset(dataset)
        key = "all" if filters is None else filters
        if not isinstance(key, str):
            raise MacroSchemaError(
                "OECD filters must be a provider-specific key string."
            )
        payload = self._get(
            self._dataset_url(dataset_id, key=key, start=start, end=end)
        ).json()
        return self._build_result_from_payload(
            dataset_id,
            payload,
            query={"start": start, "end": end, "filters": filters},
        )
