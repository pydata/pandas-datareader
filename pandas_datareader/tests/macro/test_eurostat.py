import pandas as pd

from pandas_datareader.macro.eurostat import EurostatClient


def _fixture(*lines):
    return "\n".join(lines).encode()


EUROSTAT_DATAFLOW_XML = _fixture(
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<m:Structure xmlns:m="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"',
    ' xmlns:s="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"',
    ' xmlns:c="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">',
    "  <m:Structures>",
    "    <s:Dataflows>",
    '      <s:Dataflow id="ERT_H_EUR_A" agencyID="ESTAT" version="22.0">',
    (
        '        <c:Name xml:lang="en">Former euro area national currencies '
        "vs. euro/ECU - annual data</c:Name>"
    ),
    '        <c:Description xml:lang="en">Sample Eurostat dataflow</c:Description>',
    "        <s:Structure>",
    (
        '          <Ref id="ERT_H_EUR_A" agencyID="ESTAT" '
        'package="datastructure" class="DataStructure" />'
    ),
    "        </s:Structure>",
    "      </s:Dataflow>",
    "    </s:Dataflows>",
    "  </m:Structures>",
    "</m:Structure>",
)

EUROSTAT_DSD_XML = _fixture(
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<m:Structure xmlns:m="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"',
    ' xmlns:s="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"',
    ' xmlns:c="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">',
    "  <m:Structures>",
    "    <s:Codelists>",
    '      <s:Codelist id="FREQ">',
    '        <c:Name xml:lang="en">Frequency</c:Name>',
    '        <s:Code id="A"><c:Name xml:lang="en">Annual</c:Name></s:Code>',
    "      </s:Codelist>",
    '      <s:Codelist id="STATINFO">',
    '        <c:Name xml:lang="en">Stat info</c:Name>',
    '        <s:Code id="AVG"><c:Name xml:lang="en">Average</c:Name></s:Code>',
    "      </s:Codelist>",
    '      <s:Codelist id="CURRENCY">',
    '        <c:Name xml:lang="en">Currency</c:Name>',
    '        <s:Code id="ITL"><c:Name xml:lang="en">Italian lira</c:Name></s:Code>',
    "      </s:Codelist>",
    "    </s:Codelists>",
    "    <s:DataStructures>",
    '      <s:DataStructure id="ERT_H_EUR_A">',
    "        <s:DataStructureComponents>",
    "          <s:DimensionList>",
    '            <s:Dimension id="freq">',
    (
        "              <s:LocalRepresentation><s:Enumeration>"
        '<Ref id="FREQ" agencyID="ESTAT" package="codelist" '
        'class="Codelist" /></s:Enumeration></s:LocalRepresentation>'
    ),
    "            </s:Dimension>",
    '            <s:Dimension id="statinfo">',
    (
        "              <s:LocalRepresentation><s:Enumeration>"
        '<Ref id="STATINFO" agencyID="ESTAT" package="codelist" '
        'class="Codelist" /></s:Enumeration></s:LocalRepresentation>'
    ),
    "            </s:Dimension>",
    '            <s:Dimension id="currency">',
    (
        "              <s:LocalRepresentation><s:Enumeration>"
        '<Ref id="CURRENCY" agencyID="ESTAT" package="codelist" '
        'class="Codelist" /></s:Enumeration></s:LocalRepresentation>'
    ),
    "            </s:Dimension>",
    '            <s:TimeDimension id="TIME_PERIOD" />',
    "          </s:DimensionList>",
    "        </s:DataStructureComponents>",
    "      </s:DataStructure>",
    "    </s:DataStructures>",
    "  </m:Structures>",
    "</m:Structure>",
)

EUROSTAT_DATA_XML = _fixture(
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<m:GenericData xmlns:m="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"',
    ' xmlns:g="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic"',
    ' xmlns:c="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">',
    "  <m:Header>",
    "    <m:Prepared>2026-01-08T11:00:00+0100</m:Prepared>",
    "  </m:Header>",
    '  <m:Structure structureRef="ERT_H_EUR_A" dimensionAtObservation="TIME_PERIOD" />',
    '  <m:DataSet structureRef="ERT_H_EUR_A" dimensionAtObservation="TIME_PERIOD">',
    "    <g:Series>",
    "      <g:SeriesKey>",
    '        <g:Value id="freq" value="A" />',
    '        <g:Value id="statinfo" value="AVG" />',
    '        <g:Value id="currency" value="ITL" />',
    "      </g:SeriesKey>",
    '      <g:Obs><g:ObsDimension value="2009" /><g:ObsValue value="1936.27" /></g:Obs>',
    '      <g:Obs><g:ObsDimension value="2010" /><g:ObsValue value="1936.27" /></g:Obs>',
    "    </g:Series>",
    "  </m:DataSet>",
    "</m:GenericData>",
)


def test_parse_dataflows_xml():
    result = EurostatClient()._parse_dataflows_xml(EUROSTAT_DATAFLOW_XML)

    assert result.loc[0, "dataset_id"] == "ert_h_eur_a"
    assert result.loc[0, "title"].startswith("Former euro area national currencies")


def test_parse_dsd_xml():
    result = EurostatClient()._parse_dsd_metadata(EUROSTAT_DSD_XML)

    assert result["dimensions"][0]["id"] == "freq"
    assert result["dimensions"][0]["values"]["A"] == "Annual"
    assert result["dimensions"][2]["id"] == "currency"
    assert result["dimensions"][2]["values"]["ITL"] == "Italian lira"


def test_build_result_from_payload():
    result = EurostatClient()._build_result_from_payload(
        "ert_h_eur_a",
        EUROSTAT_DATA_XML,
        EUROSTAT_DSD_XML,
        query={"start": "2009-01-01", "end": "2010-01-01"},
    )

    expected_columns = pd.MultiIndex.from_tuples(
        [("Italian lira", "Average", "Annual")],
        names=["CURRENCY", "STATINFO", "FREQ"],
    )
    expected_index = pd.DatetimeIndex(["2009-01-01", "2010-01-01"], name="TIME_PERIOD")
    expected = pd.DataFrame(
        [[1936.27], [1936.27]], index=expected_index, columns=expected_columns
    )

    pd.testing.assert_frame_equal(result.data, expected)
    assert result.provider == "eurostat"
    assert result.metadata["updated_at"] == "2026-01-08T11:00:00+0100"
