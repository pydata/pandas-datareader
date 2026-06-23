from __future__ import annotations

from xml.etree import ElementTree as ET

import pandas as pd

from pandas_datareader.io.sdmx import _read_sdmx_dsd, read_sdmx
from pandas_datareader.macro.base import MacroClientBase
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


class EurostatClient(MacroClientBase):
    provider = "eurostat"
    base_url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1"

    def _parse_dataflows_xml(self, xml_bytes):
        root = ET.fromstring(xml_bytes)
        rows = []
        for flow in root.iter(_STRUCTURE + "Dataflow"):
            if flow.get("agencyID") != "ESTAT":
                continue
            dataflow_id = flow.get("id")
            version = flow.get("version")
            structure_ref = flow.find(f".//{_STRUCTURE}Structure/Ref")
            rows.append(
                {
                    "dataset_id": dataflow_id.lower(),
                    "dataflow_id": dataflow_id,
                    "agency_id": "ESTAT",
                    "version": version,
                    "title": _preferred_text(flow, f"{_COMMON}Name"),
                    "description": _preferred_text(flow, f"{_COMMON}Description"),
                    "structure_id": (
                        None if structure_ref is None else structure_ref.get("id")
                    ),
                }
            )
        return pd.DataFrame(rows)

    def _parse_dsd_xml(self, xml_bytes):
        root = ET.fromstring(xml_bytes)
        codelists = {}
        for codelist in root.iter(_STRUCTURE + "Codelist"):
            entries = {}
            for code in codelist.iter(_STRUCTURE + "Code"):
                entries[code.get("id")] = _preferred_text(code, f"{_COMMON}Name")
            codelists[codelist.get("id")] = entries
        return root, codelists

    def _parse_dsd_metadata(self, xml_bytes):
        root, codelists = self._parse_dsd_xml(xml_bytes)
        dimensions = []
        labels = {}
        for dim in root.findall(
            f".//{_STRUCTURE}DataStructure//{_STRUCTURE}DimensionList/*"
        ):
            dim_id = dim.get("id")
            enum_ref = dim.find(f".//{_STRUCTURE}Enumeration/Ref")
            values = {}
            if enum_ref is not None:
                values = codelists.get(enum_ref.get("id"), {})
            dimensions.append({"id": dim_id, "label": dim_id, "values": values})
            labels[dim_id] = values
        return {"dimensions": dimensions, "labels": labels, "codelists": codelists}

    @staticmethod
    def _format_columns(data):
        if not isinstance(data.columns, pd.MultiIndex):
            return data

        columns = data.columns
        order_priority = {
            "currency": 0,
            "statinfo": 1,
            "unit": 2,
            "siec": 3,
            "tax": 4,
            "nrg_cons": 5,
            "geo": 98,
            "freq": 99,
        }
        current = list(columns.names)
        order = sorted(
            range(len(current)), key=lambda i: (order_priority.get(current[i], 50), i)
        )
        columns = columns.reorder_levels(order)
        columns = columns.set_names([name.upper() for name in columns.names])
        data.columns = columns
        return data

    def _build_result_from_payload(self, dataset_id, data_xml, dsd_xml, query=None):
        dsd = _read_sdmx_dsd(dsd_xml)
        metadata = self._parse_dsd_metadata(dsd_xml)
        data = read_sdmx(data_xml, dsd=dsd)
        data = self._format_columns(data)

        root = ET.fromstring(data_xml)
        prepared = root.find(f".//{_MESSAGE}Prepared")
        result_metadata = {
            "title": dataset_id,
            "description": dataset_id,
            "updated_at": None if prepared is None else prepared.text,
            "dimensions": metadata["dimensions"],
            "attributes": {},
            "labels": metadata["labels"],
            "source": "eurostat-sdmx-2.1",
            "query": query or {},
            "notes": [],
            "raw": {"dataset_id": dataset_id},
        }
        return MacroResult(
            data=data,
            metadata=result_metadata,
            provider=self.provider,
            dataset_id=dataset_id,
        )

    def _dataflow_url(self, dataset=None):
        if dataset is None:
            return f"{self.base_url}/dataflow/all/all/latest"
        return f"{self.base_url}/dataflow/ESTAT/{dataset.upper()}/latest"

    def _dsd_url(self, dataset):
        return f"{self.base_url}/datastructure/ESTAT/{dataset}/latest?references=descendants"

    def _data_url(self, dataset, start=None, end=None):
        url = f"{self.base_url}/data/{dataset}"
        params = []
        if start is not None:
            params.append(f"startPeriod={pd.to_datetime(start).year}")
        if end is not None:
            params.append(f"endPeriod={pd.to_datetime(end).year}")
        if params:
            url = f"{url}?{'&'.join(params)}"
        return url

    def search_datasets(self, query=None, **kwargs):
        response = self._get(self._dataflow_url())
        result = self._parse_dataflows_xml(response.content)
        if query:
            q = query.lower()
            mask = (
                result["dataset_id"].fillna("").str.lower().str.contains(q)
                | result["title"].fillna("").str.lower().str.contains(q)
                | result["description"].fillna("").str.lower().str.contains(q)
            )
            result = result.loc[mask].reset_index(drop=True)
        return result

    def describe_dataset(self, dataset, **kwargs):
        dsd_xml = self._get(self._dsd_url(dataset)).content
        dataflow_xml = self._get(self._dataflow_url(dataset)).content
        dataflow = self._parse_dataflows_xml(dataflow_xml).iloc[0].to_dict()
        dsd_meta = self._parse_dsd_metadata(dsd_xml)
        return {
            "dataset_id": dataset,
            "title": dataflow["title"],
            "description": dataflow["description"],
            "updated_at": None,
            "dimensions": dsd_meta["dimensions"],
            "attributes": {},
            "labels": dsd_meta["labels"],
            "source": "eurostat-sdmx-2.1",
            "query": {},
            "notes": [],
            "raw": {"dataflow": dataflow},
        }

    def read(self, dataset, start=None, end=None, filters=None, **kwargs):
        if filters is not None:
            raise NotImplementedError(
                "Eurostat dict/string filters are not implemented in the new macro API yet."
            )
        data_xml = self._get(self._data_url(dataset, start=start, end=end)).content
        dsd_xml = self._get(self._dsd_url(dataset)).content
        result = self._build_result_from_payload(
            dataset,
            data_xml,
            dsd_xml,
            query={"start": start, "end": end, "filters": filters},
        )
        described = self.describe_dataset(dataset)
        result.metadata["title"] = described["title"]
        result.metadata["description"] = described["description"]
        return result
