import pandas as pd

from pandas_datareader.macro.oecd import OECDClient


def _fixture(*lines):
    return "\n".join(lines).encode()


OECD_DATAFLOW_XML = _fixture(
    '<?xml version="1.0" encoding="utf-8"?>',
    (
        '<message:Structure xmlns:message="http://www.sdmx.org/resources/'
        'sdmxml/schemas/v2_1/message"'
    ),
    ' xmlns:structure="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"',
    ' xmlns:common="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">',
    "  <message:Structures>",
    "    <structure:Dataflows>",
    (
        '      <structure:Dataflow id="DSD_TUD_CBC@DF_CBC" '
        'agencyID="OECD.ELS.SAE" version="1.0">'
    ),
    '        <common:Name xml:lang="en">Trade union density</common:Name>',
    '        <common:Description xml:lang="en">Trade union density</common:Description>',
    "        <structure:Structure>",
    (
        '          <Ref id="DSD_TUD_CBC" agencyID="OECD.ELS.SAE" '
        'package="datastructure" class="DataStructure" />'
    ),
    "        </structure:Structure>",
    "      </structure:Dataflow>",
    "    </structure:Dataflows>",
    "  </message:Structures>",
    "</message:Structure>",
)

OECD_PAYLOAD = {
    "meta": {"prepared": "2026-04-10T00:00:00Z"},
    "errors": [],
    "data": {
        "dataSets": [
            {
                "series": {
                    "0:0:0": {"observations": {"0": [1.5], "1": [2.5]}},
                    "1:0:0": {"observations": {"0": [3.5], "1": [4.5]}},
                }
            }
        ],
        "structures": [
            {
                "name": "Trade union density",
                "description": "Trade union density sample",
                "dimensions": {
                    "series": [
                        {
                            "id": "REF_AREA",
                            "name": "Reference area",
                            "values": [
                                {"id": "AUS", "name": "Australia"},
                                {"id": "USA", "name": "United States"},
                            ],
                        },
                        {
                            "id": "MEASURE",
                            "name": "Measure",
                            "values": [{"id": "TUD", "name": "Trade union density"}],
                        },
                        {
                            "id": "UNIT_MEASURE",
                            "name": "Unit of measure",
                            "values": [
                                {"id": "PT_SAL", "name": "Percentage of employees"}
                            ],
                        },
                    ],
                    "observation": [
                        {
                            "id": "TIME_PERIOD",
                            "name": "Time period",
                            "values": [
                                {"id": "2010", "name": "2010"},
                                {"id": "2011", "name": "2011"},
                            ],
                        }
                    ],
                },
            }
        ],
    },
}


def test_parse_dataflows_xml():
    result = OECDClient()._parse_dataflows_xml(OECD_DATAFLOW_XML)

    assert list(result.columns) == [
        "dataset_id",
        "dataflow_id",
        "agency_id",
        "version",
        "title",
        "description",
        "structure_id",
    ]
    assert result.loc[0, "dataset_id"] == "OECD.ELS.SAE,DSD_TUD_CBC@DF_CBC,1.0"
    assert result.loc[0, "title"] == "Trade union density"


def test_build_result_from_payload():
    result = OECDClient()._build_result_from_payload(
        "OECD.ELS.SAE,DSD_TUD_CBC@DF_CBC,1.0",
        OECD_PAYLOAD,
        query={"start": "2010-01-01", "end": "2011-01-01"},
    )

    expected_columns = pd.MultiIndex.from_product(
        [
            ["Australia", "United States"],
            ["Trade union density"],
            ["Percentage of employees"],
        ],
        names=["Reference area", "Measure", "Unit of measure"],
    )
    expected_index = pd.DatetimeIndex(["2010-01-01", "2011-01-01"], name="Time period")
    expected = pd.DataFrame(
        [[1.5, 3.5], [2.5, 4.5]],
        index=expected_index,
        columns=expected_columns,
    )

    pd.testing.assert_frame_equal(result.data, expected)
    assert result.provider == "oecd"
    assert result.metadata["title"] == "Trade union density"
    assert "dimensions" in result.metadata
