from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

import pandas as pd

from indico_orpheus.clients.insights import AsyncInsightsClient


flood_layers = [
    "FL_Fluvial_SwissRe",
    "FL_Pluvial_SwissRe",
    "FL_Coastal_Global_Fathom",
    "GEO_DistToCoast_Global_SwissRe",
]
eq_gt_layers = [
    "EQ_Bedrock_Global_SwissRe",
    "EQ_LocalSoilCondition_Global_SwissRe",
    "EQ_Tsunami_SwissRe",
    "DR_Subsidence_France_SwissRe",
]
wind_storm_layers = [
    "WS_Windspeed_Global_SwissRe",
    "FL_Surge_SwissRe",
]
scs_layers = [
    "CS_Hail_Global_SwissRe",
    "CS_Tornado_Global_SwissRe",
    "CS_Lightning_Global_SwissRe",
]
geo_ns_layers = [
    "EQ_Landslide_Global_SwissRe",
    "VO_AshThickness_Global_SwissRe",
]
wildfire_layers = [
    "WF_Wildfire_Global_SwissRe",
    "WF_DistToBush_AUS_SwissRe",
]

COVERAGE_LAYER_MAP = {
    "Flood Coverage Required": flood_layers,
    "Earthquake & Geotechnical Coverage Required": eq_gt_layers,
    "Fire & Wildland Peril Coverage Required": wildfire_layers,
    "Geological (Non-Seismic) Coverage Required": geo_ns_layers,
    "Severe Convective Storm (SCS) Coverage Required": scs_layers,
    "Wind & Storm Peril Coverage Required": wind_storm_layers,
}


async def query(insights_client: AsyncInsightsClient, query: str, variables: dict[str, Any]) -> Any:
    response = await insights_client.call_gql(query, variables=variables)
    print(f"Response:\n{response}")
    return response


async def push_table(insights_client: AsyncInsightsClient, variables: dict[str, Any]) -> Any:
    mutation = """
        mutation BaseTableViewer_SetTableValues($submissionId: ID!, $tableId: String!, $cellInputs: [TableCellInput!]!) {
          updateTableValues(
            submissionId: $submissionId
            tableId: $tableId
            cellInputs: $cellInputs
          ) {
            id
            ...ReviewSectionTable_TableInstanceRows
            __typename
          }
        }
        fragment UseDocumentViewerActions_FieldValue on FieldValue {
          ... on DocumentFieldValue {
            document {
              id
              __typename
            }
            locations {
              ... on SpanDocumentLocation {
                start
                end
                pageNumber
                __typename
              }
              ... on BoundDocumentLocation {
                top
                bottom
                left
                right
                pageNumber
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        fragment ReviewSectionTable_TableInstanceRows on TableInstance {
          id
          rows {
            cells {
              field {
                fieldId
                typeConfig {
                  type
                  __typename
                }
                __typename
              }
              value {
                id
                value
                ...UseDocumentViewerActions_FieldValue
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
    """

    response = await insights_client.call_gql(mutation, variables=variables)
    print(f"Response:\n{response}")
    return response


async def workspace_table(
    insights_client: AsyncInsightsClient,
    submission_id: int,
    table_id: str,
) -> pd.DataFrame:
    gql_query = """
        query Tables($submissionId: ID!, $tableIds: [String!]) {
          submission(id: $submissionId) {
            tables(tableIds: $tableIds) {
              table {
                id
              }
              rows {
                cells {
                  rowNumber
                  field {
                    fieldId
                  }
                  value {
                    value
                  }
                }
              }
            }
          }
        }
    """

    variables = {"submissionId": submission_id, "tableIds": [table_id]}
    response = await insights_client.call_gql(gql_query, variables=variables)

    tables: Iterable[Dict[str, Any]] = response.get("submission", {}).get("tables", [])

    selected_table: Optional[Dict[str, Any]] = None
    for tbl in tables:
        current_id = tbl.get("table", {}).get("id")
        if current_id == table_id:
            selected_table = tbl
            break

    if selected_table is None:
        raise ValueError(f"No table found for table_id={table_id!r}")

    row_map: Dict[int, Dict[str, Any]] = defaultdict(dict)
    for row in selected_table.get("rows", []):
        for cell in row.get("cells", []):
            row_number = cell.get("rowNumber")
            field_id = cell.get("field", {}).get("fieldId")
            value = cell.get("value", {}).get("value")
            if row_number is None or not field_id:
                continue
            row_map[row_number][field_id] = value

    df = pd.DataFrame.from_dict(row_map, orient="index")
    df.index.name = "rowNumber"
    return df.sort_index()

async def workspace_values(insights_client: AsyncInsightsClient, submission_id: int, field_ids: dict[str, Any]) -> Any:
    gql_query = """
        query Submissions($ids: [ID!]) {
          submissions(ids: $ids) {
            items {
              fields(fieldIds: [
                "flood_coverage_required",
                "earthquake_geotechnical_coverage_required",
                "fire_wildland_peril_coverage_required",
                "geological_non_seismic_coverage_required",
                "severe_convective_storm_scs_coverage_required",
                "wind_storm_peril_coverage_required"
              ]) {
                field {
                  displayName {
                    defaultTranslation
                  }
                }
                currentValues {
                  value
                }
              }
            }
          }
        }
    """

    variables = {"submissionId": submission_id, "tableIds": [table_id]}
    response = await insights_client.call_gql(gql_query, variables=variables)

    tables: Iterable[Dict[str, Any]] = response.get("submission", {}).get("tables", [])

    selected_table: Optional[Dict[str, Any]] = None
    for tbl in tables:
        current_id = tbl.get("table", {}).get("id")
        if current_id == table_id:
            selected_table = tbl
            break

    if selected_table is None:
        raise ValueError(f"No table found for table_id={table_id!r}")

    row_map: Dict[int, Dict[str, Any]] = defaultdict(dict)
    for row in selected_table.get("rows", []):
        for cell in row.get("cells", []):
            row_number = cell.get("rowNumber")
            field_id = cell.get("field", {}).get("fieldId")
            value = cell.get("value", {}).get("value")
            if row_number is None or not field_id:
                continue
            row_map[row_number][field_id] = value

    df = pd.DataFrame.from_dict(row_map, orient="index")
    df.index.name = "rowNumber"
    return df.sort_index()


async def sub_values(insights_client: AsyncInsightsClient, submission_id: int, column_names: list[str],) -> pd.DataFrame:
    gql_query = f"""
        query Submissions($ids: [ID!]) {{
            submissions(ids: $ids) {{
                items {{
                    fields(fieldIds: {json.dumps(column_names)}) {{
                        currentValues {{
                            text
                        }}
                    }}
                }}
            }}
        }}
    """
    variables = {"ids": [submission_id]}
    response = await insights_client.call_gql(gql_query, variables=variables)

    fields = response["submissions"]["items"][0]["fields"]

    columns = {}
    for col_name, field in zip(column_names, fields):
        values = field["currentValues"]
        columns[col_name] = [v["text"] for v in values] if values else []

    max_len = max((len(v) for v in columns.values()), default=0)
    for col in columns:
        columns[col] += [None] * (max_len - len(columns[col]))

    return pd.DataFrame(columns)

async def get_required_layers(insights_client: AsyncInsightsClient, submission_id: int) -> List[str]:
    gql_query = """
        query Submissions($ids: [ID!]) {
          submissions(ids: $ids) {
            items {
              fields(fieldIds: [
                "flood_coverage_required",
                "earthquake_geotechnical_coverage_required",
                "fire_wildland_peril_coverage_required",
                "geological_non_seismic_coverage_required",
                "severe_convective_storm_scs_coverage_required",
                "wind_storm_peril_coverage_required"
              ]) {
                field {
                  displayName {
                    defaultTranslation
                  }
                }
                currentValues {
                  value
                }
              }
            }
          }
        }
    """
    response = await insights_client.call_gql(gql_query, variables={"ids": [submission_id]})

    fields = response["submissions"]["items"][0]["fields"]

    layers: list[str] = []
    for field in fields:
        display_name = field["field"]["displayName"]["defaultTranslation"]
        raw_value = field["currentValues"][0]["value"] if field["currentValues"] else None
        value = raw_value.strip('"') if raw_value else None

        if value in ("Required", "All Risks") and display_name in COVERAGE_LAYER_MAP:
            layers.extend(COVERAGE_LAYER_MAP[display_name])

    return layers

def extract_agent_grouped(data: Union[str, Path, dict], agent_number: str) -> pd.DataFrame:
    if isinstance(data, (str, Path)):
        with open(data, "r", encoding="utf-8") as f:
            payload = json.load(f)
    elif isinstance(data, dict):
        payload = data
    else:
        raise TypeError("data must be a file path or a dict")

    groupindex_to_fields = defaultdict(dict)
    all_labels = set()

    for file_result in payload.get("submission_results", []):
        model_results = (file_result.get("model_results") or {}).get("ORIGINAL") or {}
        agent_items = model_results.get(str(agent_number), [])

        for item in agent_items:
            label = item.get("label")
            text_value = item.get("text")
            groupings = item.get("groupings") or []

            if not label:
                continue

            if not groupings:
                gi = -1
                existing = groupindex_to_fields[gi].get(label)
                if existing:
                    parts = set(p.strip() for p in existing.split("|"))
                    if text_value and text_value not in parts:
                        groupindex_to_fields[gi][label] = existing + " | " + text_value
                else:
                    groupindex_to_fields[gi][label] = text_value
                all_labels.add(label)
                continue

            for grouping in groupings:
                gi = grouping.get("group_index")
                existing = groupindex_to_fields[gi].get(label)
                if existing and text_value and text_value not in [p.strip() for p in existing.split("|")]:
                    groupindex_to_fields[gi][label] = existing + " | " + text_value
                else:
                    if not existing:
                        groupindex_to_fields[gi][label] = text_value
                all_labels.add(label)

    labels_sorted = sorted(all_labels)
    records = []
    for gi, fields in groupindex_to_fields.items():
        row = {"group_index": gi}
        for lab in labels_sorted:
            row[lab] = fields.get(lab)
        records.append(row)

    df = pd.DataFrame(records).sort_values(by=["group_index"]).reset_index(drop=True)

    cols = ["Location Reference", "Address", "Town", "County", "Post Code"]
    df["f_Address"] = df.apply(
        lambda row: ", ".join(
            str(row[c]) for c in cols if c in df.columns and pd.notna(row[c])
        ),
        axis=1,
    )
    return df