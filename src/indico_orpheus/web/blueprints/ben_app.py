import os
import re
from io import BytesIO, StringIO
from flask import Blueprint, request, jsonify, render_template, send_file
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import LiteralScalarString
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from git import Repo, GitCommandError
from dotenv import load_dotenv
from auth import login_required, section_required, SECTIONS
import boto3
from botocore.exceptions import ClientError
from werkzeug.utils import secure_filename
from indico import IndicoClient, IndicoConfig
from indico.client.request import GraphQLRequest
import subprocess
import copy
import asyncio

#from indico_orpheus.workflows.enrich_duns import run_duns_enrichment
#from indico_orpheus.workflows.enrich_swissre import run_swissre_enrichment

# Load env
load_dotenv(os.path.expanduser("~/.env"))
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

bp = Blueprint("ben_app", __name__, url_prefix="/ben-app")


# =============================================================================
# YAML FORMAT PRESERVATION SYSTEM
# =============================================================================

class YamlFormatManager:
    """
    Manages YAML format preservation by storing the complete original parsed data.
    The key principle: we MODIFY the original data in-place, never rebuild it.
    """

    def __init__(self):
        self.original_yaml_content = {}  # branch -> full parsed YAML (ArgoCD wrapper)
        self.original_helm_data = {}     # branch -> parsed HELM_VALUES content
        self.format_info = {}            # branch -> format metadata

    def store_original(self, branch: str, yaml_content, helm_values_str: str):
        """Store the complete original parsed YAML and HELM_VALUES."""
        # Store the full ArgoCD YAML structure
        self.original_yaml_content[branch] = yaml_content

        # Parse and store the HELM_VALUES content
        if helm_values_str:
            yaml = create_yaml_handler()
            try:
                self.original_helm_data[branch] = yaml.load(StringIO(helm_values_str))
            except Exception:
                self.original_helm_data[branch] = None

        # Analyze format for indent detection
        self.format_info[branch] = self._analyze_format(helm_values_str)

    def _analyze_format(self, helm_values_str: str) -> dict:
        """Analyze the formatting of the original HELM_VALUES."""
        info = {'inner_indent': 2}
        if helm_values_str:
            info['inner_indent'] = self._detect_indent(helm_values_str)
        return info

    def _detect_indent(self, yaml_str: str) -> int:
        """Detect the indentation size used in a YAML string."""
        lines = yaml_str.split('\n')
        indent_diffs = []
        prev_indent = 0

        for line in lines:
            if not line.strip() or line.strip().startswith('#'):
                continue
            leading = len(line) - len(line.lstrip())
            if leading > prev_indent:
                diff = leading - prev_indent
                if diff > 0:
                    indent_diffs.append(diff)
            prev_indent = leading

        if indent_diffs:
            from collections import Counter
            counter = Counter(indent_diffs)
            return counter.most_common(1)[0][0]
        return 2

    def get_format(self, branch: str) -> dict:
        return self.format_info.get(branch, {'inner_indent': 2})

    def get_original_helm_data(self, branch: str):
        """Get the original parsed helm data for a branch."""
        return self.original_helm_data.get(branch)

    def get_original_yaml_content(self, branch: str):
        """Get the original parsed ArgoCD YAML for a branch."""
        return self.original_yaml_content.get(branch)


# Global format manager
format_manager = YamlFormatManager()


def create_yaml_handler(indent: int = 2) -> YAML:
    """Create a configured YAML handler that preserves formatting."""
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.default_flow_style = False
    yaml.width = 4096  # Prevent line wrapping
    yaml.indent(mapping=indent, sequence=indent + 2, offset=indent)
    yaml.default_style = None
    # Allow duplicate keys (some YAML files have them, though it's technically invalid)
    yaml.allow_duplicate_keys = True
    return yaml


# =============================================================================
# CONFIGURATION
# =============================================================================

CLUSTERS = {
    "TRY": {"host": "try.indico.io", "token_path": "/home/indico/mysite/token_try.txt"},
    "TRY-EU": {"host": "try-eu.indico.io", "token_path": "/home/indico/mysite/token_try_eu.txt"},
    "DEV-CI": {"host": "dev-ci.us-east-2.indico-dev.indico.io", "token_path": "/home/indico/mysite/dev-ci-token.txt"},
}

CONFIG_FILENAME = "insights_application.yaml"
CLONE_BASE_DIR = "/home/indico/mysite-dev/insights_config"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_branch_dir(branch: str) -> str:
    safe_branch = secure_filename(branch.replace('/', '_'))
    return os.path.join(CLONE_BASE_DIR, safe_branch)


def auth_url(repo_url: str, token: str | None) -> str:
    if not token:
        return repo_url
    return repo_url.replace("https://", f"https://{token}:x-oauth-basic@")


def ensure_repo(clone_dir: str, repo_url: str, branch: str, token: str | None, reset: bool = True):
    os.environ["GIT_TERMINAL_PROMPT"] = "0"
    os.environ["GCM_INTERACTIVE"] = "Never"
    os.makedirs(clone_dir, exist_ok=True)
    authed = auth_url(repo_url, token)

    if not os.path.exists(os.path.join(clone_dir, ".git")):
        repo = Repo.clone_from(authed, clone_dir, branch=branch, depth=1)
        repo.config_writer().set_value("user", "name", "john-andel").release()
        repo.config_writer().set_value("user", "email", "john-andel@users.noreply.github.com").release()
        return repo

    repo = Repo(clone_dir)
    try:
        repo.remotes.origin.set_url(authed)
    except Exception:
        pass

    if reset:
        try:
            repo.git.fetch("origin", branch)
            repo.git.reset("--hard", f"origin/{branch}")
            repo.git.clean("-fd")
        except GitCommandError:
            pass

    try:
        repo.git.checkout(branch)
    except GitCommandError:
        try:
            repo.git.checkout("-b", branch, "--track", f"origin/{branch}")
        except GitCommandError:
            pass

    return repo


def find_config_path(root: str) -> str | None:
    candidate = os.path.join(root, CONFIG_FILENAME)
    if os.path.exists(candidate):
        return candidate
    for dirpath, _, filenames in os.walk(root):
        if CONFIG_FILENAME in filenames:
            return os.path.join(dirpath, CONFIG_FILENAME)
    return None


def extract_helm_values_string(yaml_content: dict) -> str | None:
    """Extract the raw HELM_VALUES string from parsed YAML."""
    try:
        envs = yaml_content.get("spec", {}).get("source", {}).get("plugin", {}).get("env", [])
        if isinstance(envs, dict):
            envs = [envs]

        for env in envs:
            if isinstance(env, dict) and env.get("name") == "HELM_VALUES":
                return env.get("value", "")
    except (AttributeError, TypeError):
        pass
    return None


def to_plain_dict(obj):
    """Convert ruamel.yaml types to plain Python types for the UI."""
    if isinstance(obj, (CommentedMap, dict)):
        return {k: to_plain_dict(v) for k, v in obj.items()}
    elif isinstance(obj, (CommentedSeq, list)):
        return [to_plain_dict(item) for item in obj]
    return obj


def extract_config(yaml_content: dict) -> dict:
    """Extract configuration from YAML object for the UI."""
    config = {
        "dataConfig": [],
        "workspaceConfig": {},
        "_configFieldType": "ingestion_config",
        "_formatVersion": "new"
    }

    yaml_handler = create_yaml_handler()

    try:
        envs = yaml_content.get("spec", {}).get("source", {}).get("plugin", {}).get("env", [])
        if isinstance(envs, dict):
            envs = [envs]
    except AttributeError:
        return config

    for env in envs:
        if isinstance(env, dict) and env.get("name") == "HELM_VALUES":
            val_str = env.get("value", "")
            try:
                helm_values = yaml_handler.load(StringIO(val_str)) or {}

                # Deep copy to plain dicts for the UI
                ws_config = to_plain_dict(helm_values.get("workspaceConfig", {}))
                config["workspaceConfig"] = ws_config

                if "fields" in ws_config:
                    config["dataConfig"] = ws_config["fields"]
                    config["_formatVersion"] = "new"
                elif "dataConfig" in helm_values:
                    config["dataConfig"] = to_plain_dict(helm_values["dataConfig"])
                    config["_formatVersion"] = "old"

                # Convert Sections contents -> field_ids for UI
                views = config["workspaceConfig"].get("views", {})
                if "review" in views:
                    # Extract fallback_section status
                    fallback_section = views["review"].get("fallback_section", {})
                    if fallback_section and fallback_section.get("enabled") == False:
                        config["fallback_section_disabled"] = True

                    for section in views["review"].get("sections", []):
                        if "contents" in section and "field_ids" not in section:
                            section["field_ids"] = [
                                c["id"] for c in section["contents"]
                                if c.get("type") in ("field", "table") and "id" in c
                            ]

                if config["dataConfig"]:
                    first = config["dataConfig"][0]
                    if "source_config" in first:
                        config["_configFieldType"] = "source_config"
                    else:
                        config["_configFieldType"] = "ingestion_config"

                # =============================================================
                # Merge orchestrator agent settings into workspaceConfig.agents
                # Many YAMLs store the real agent config (autospam thresholds,
                # automerge fields, suggested_actions, advisor_agent_config)
                # under workspaceConfig.orchestrator.agents rather than
                # workspaceConfig.agents directly.  Merge those values in so
                # the UI always reads from a single consistent location.
                # =============================================================
                orch_sua = (
                    ws_config
                    .get("orchestrator", {})
                    .get("agents", {})
                    .get("submission_update_agent", {})
                ) or {}

                if orch_sua:
                    # Ensure agents structure exists in ws_config
                    if "agents" not in ws_config:
                        ws_config["agents"] = {}
                    if "submission_update_agent" not in ws_config["agents"]:
                        ws_config["agents"]["submission_update_agent"] = {}

                    sua = ws_config["agents"]["submission_update_agent"]
                    if not isinstance(sua, dict):
                        sua = {}
                        ws_config["agents"]["submission_update_agent"] = sua

                    # Merge keys from orchestrator SUA that are missing or
                    # empty in the top-level SUA (orchestrator is authoritative)
                    for merge_key in (
                        "autospam_igo_percentage_min_threshold",
                        "automerge_associative_field_ids",
                        "suggested_actions",
                        "advisor_agent_config",
                    ):
                        if merge_key in orch_sua and not sua.get(merge_key):
                            sua[merge_key] = orch_sua[merge_key]

                # =============================================================
                # Detect advisor agent enabled state from global.features flag.
                # If advisorAgent is True but advisor_agent_config is missing,
                # initialise an empty config so the UI shows the toggle ON.
                # If advisorAgent is explicitly False, clear advisor_agent_config.
                # =============================================================
                advisor_feature_flag = (
                    to_plain_dict(helm_values.get("global", {}))
                    .get("features", {})
                    .get("advisorAgent")
                )

                if advisor_feature_flag is not None:
                    if "agents" not in ws_config:
                        ws_config["agents"] = {}
                    if "submission_update_agent" not in ws_config["agents"]:
                        ws_config["agents"]["submission_update_agent"] = {}

                    sua = ws_config["agents"]["submission_update_agent"]
                    if not isinstance(sua, dict):
                        sua = {}
                        ws_config["agents"]["submission_update_agent"] = sua

                    if advisor_feature_flag:
                        # Advisor is enabled — ensure config object is present
                        if not sua.get("advisor_agent_config"):
                            sua["advisor_agent_config"] = {"rules": []}
                    else:
                        # Advisor is explicitly disabled — remove config
                        sua.pop("advisor_agent_config", None)

                # Extract threshold configuration
                server_config = helm_values.get("server", {})
                services_config = server_config.get("services", {})
                lagoon_config = services_config.get("lagoon", {})
                env_config = lagoon_config.get("env", {})

                if env_config:
                    autoconfirm = env_config.get("FIELD_AUTOACCEPT_CONFIDENCE") or env_config.get("FIELD_AUTOCONFIRM_CONFIDENCE")
                    autoreject = env_config.get("FIELD_AUTOREJECT_CONFIDENCE")
                    if autoconfirm is not None or autoreject is not None:
                        config["thresholds"] = {
                            "enabled": True,
                            "autoconfirm": float(autoconfirm) if autoconfirm is not None else 0.9,
                            "autoreject": float(autoreject) if autoreject is not None else 0.4
                        }

            except Exception as e:
                print(f"Error parsing HELM_VALUES: {e}")
            break

    return config


# =============================================================================
# IN-PLACE MODIFICATION FUNCTIONS
# =============================================================================
# These functions modify the original data structure in-place to preserve
# all formatting, key ordering, comments, and structure from the original YAML.
# =============================================================================

def deep_update_value(target, key, value):
    """
    Update a value in target, preserving the key's position.
    For CommentedMap, this maintains key ordering.
    """
    if key in target:
        target[key] = value
    else:
        target[key] = value


def ensure_commented_seq(value):
    """Convert a list to CommentedSeq if needed."""
    if isinstance(value, CommentedSeq):
        return value
    if isinstance(value, list):
        seq = CommentedSeq(value)
        return seq
    return CommentedSeq([value])


def ensure_commented_map(value):
    """Convert a dict to CommentedMap if needed."""
    if isinstance(value, CommentedMap):
        return value
    if isinstance(value, dict):
        result = CommentedMap()
        for k, v in value.items():
            result[k] = v
        return result
    return CommentedMap()


def update_field_in_place(original_field, ui_field, config_field_type):
    """
    Update a field in-place, preserving its key ordering.
    Only updates keys that exist in the UI data.
    """
    # Update simple values
    if "display_name" in ui_field:
        original_field["display_name"] = ui_field["display_name"]
    if "field_id" in ui_field:
        original_field["field_id"] = ui_field["field_id"]
    if "igo_required" in ui_field:
        original_field["igo_required"] = ui_field["igo_required"]

    # Handle multi field
    if ui_field.get("multi"):
        original_field["multi"] = True
    elif "multi" in original_field:
        del original_field["multi"]

    # Update type_config
    if "type_config" in ui_field:
        ui_tc = ui_field["type_config"]
        if "type_config" not in original_field:
            original_field["type_config"] = CommentedMap()

        tc = original_field["type_config"]
        if tc is None:
            original_field["type_config"] = CommentedMap()
            tc = original_field["type_config"]

        if "type" in ui_tc:
            tc["type"] = ui_tc["type"]

        # Handle CATEGORICAL-specific fields
        if ui_tc.get("type") == "CATEGORICAL":
            if "color_palette" in ui_tc:
                tc["color_palette"] = ui_tc["color_palette"]
            if "options" in ui_tc:
                tc["options"] = ensure_commented_seq(ui_tc["options"])
        else:
            # Remove categorical fields if type changed
            if "color_palette" in tc:
                del tc["color_palette"]
            if "options" in tc:
                del tc["options"]

    # Update ingestion_config or source_config
    # Use whichever config key exists in the original field, or fall back to the default
    ui_config = ui_field.get("ingestion_config") or ui_field.get("source_config")

    if ui_config:
        # Determine which config key to use - prefer the one that exists in original
        if "ingestion_config" in original_field:
            config_key = "ingestion_config"
        elif "source_config" in original_field:
            config_key = "source_config"
        else:
            config_key = config_field_type  # Use the detected type from the file

        if config_key not in original_field:
            original_field[config_key] = CommentedMap()

        orig_config = original_field[config_key]
        if orig_config is None:
            original_field[config_key] = CommentedMap()
            orig_config = original_field[config_key]

        if "source" in ui_config:
            orig_config["source"] = ui_config["source"]
        if "intake_id" in ui_config:
            try:
                orig_config["intake_id"] = int(ui_config["intake_id"])
            except (ValueError, TypeError):
                orig_config["intake_id"] = ui_config["intake_id"]


def build_field_preserving_order(ui_field, template_field, config_field_type):
    """
    Build a new field using the key order from a template field.
    Used when adding new fields to maintain consistency with existing fields.
    """
    result = CommentedMap()

    # Get key order from template, or use default
    if template_field and hasattr(template_field, 'keys'):
        key_order = list(template_field.keys())
    else:
        key_order = ['display_name', 'field_id', 'igo_required', 'multi',
                     'type_config', config_field_type]

    # Ensure multi is in the right position if needed
    if ui_field.get('multi') and 'multi' not in key_order:
        try:
            idx = key_order.index('igo_required') + 1
        except ValueError:
            idx = 2
        key_order.insert(idx, 'multi')

    # Build in order
    for key in key_order:
        if key == 'display_name' and 'display_name' in ui_field:
            result['display_name'] = ui_field['display_name']
        elif key == 'field_id' and 'field_id' in ui_field:
            result['field_id'] = ui_field['field_id']
        elif key == 'igo_required':
            result['igo_required'] = ui_field.get('igo_required', False)
        elif key == 'multi' and ui_field.get('multi'):
            result['multi'] = True
        elif key == 'type_config' and 'type_config' in ui_field:
            tc = CommentedMap()
            ui_tc = ui_field['type_config']
            tc['type'] = ui_tc.get('type', 'TEXT')
            if ui_tc.get('type') == 'CATEGORICAL':
                if 'color_palette' in ui_tc:
                    tc['color_palette'] = ui_tc['color_palette']
                if 'options' in ui_tc:
                    tc['options'] = ensure_commented_seq(ui_tc['options'])
            result['type_config'] = tc
        elif key in ('ingestion_config', 'source_config'):
            ui_config = ui_field.get('ingestion_config') or ui_field.get('source_config')
            if ui_config or key == config_field_type:
                cfg = CommentedMap()
                cfg['source'] = (ui_config or {}).get('source', 'INTAKE')
                intake_id = (ui_config or {}).get('intake_id', '')
                try:
                    cfg['intake_id'] = int(intake_id) if intake_id else intake_id
                except (ValueError, TypeError):
                    cfg['intake_id'] = intake_id
                result[key] = cfg

    # Add any remaining keys not in template
    for key, value in ui_field.items():
        if key not in result and key not in ('ingestion_config', 'source_config', 'type_config'):
            result[key] = value

    return result


def update_helm_values_in_place(original_helm_data, ui_config: dict, branch: str):
    """
    Update the HELM_VALUES data in-place based on UI changes.

    CRITICAL PRINCIPLE: We modify the original data structure, never rebuild it.
    This preserves:
    - All key ordering
    - All flow-style formatting (e.g., [value])
    - All comments
    - All sections the UI doesn't touch
    """
    if original_helm_data is None:
        original_helm_data = CommentedMap()

    # Ensure ui_config is a dict
    if ui_config is None:
        ui_config = {}

    data = original_helm_data

    # Ensure workspaceConfig exists
    if "workspaceConfig" not in data:
        data["workspaceConfig"] = CommentedMap()

    ws_config = data["workspaceConfig"]
    if ws_config is None:
        data["workspaceConfig"] = CommentedMap()
        ws_config = data["workspaceConfig"]

    ui_ws = ui_config.get("workspaceConfig") or {}
    config_field_type = ui_config.get("_configFieldType", "ingestion_config")

    # ==========================================================================
    # 1. UPDATE FIELDS
    # ==========================================================================
    ui_fields = ui_config.get("dataConfig", [])

    if ui_fields:
        # Get template from original for new field key ordering
        template_field = None
        if "fields" in ws_config and ws_config["fields"]:
            template_field = ws_config["fields"][0]

        # Build field lookup by field_id from original
        original_fields_by_id = {}
        if "fields" in ws_config:
            for f in ws_config["fields"]:
                fid = f.get("field_id")
                if fid:
                    original_fields_by_id[fid] = f

        # Build new fields list
        new_fields = CommentedSeq()
        for ui_field in ui_fields:
            field_id = ui_field.get("field_id")

            if field_id and field_id in original_fields_by_id:
                # Update existing field in-place
                orig_field = original_fields_by_id[field_id]
                update_field_in_place(orig_field, ui_field, config_field_type)
                new_fields.append(orig_field)
            else:
                # New field - build with template order
                new_field = build_field_preserving_order(ui_field, template_field, config_field_type)
                new_fields.append(new_field)

        ws_config["fields"] = new_fields

    # ==========================================================================
    # 2. UPDATE DISPLAY_NAMES (in-place)
    # ==========================================================================
    ui_display_names = ui_ws.get("display_names")
    if ui_display_names is not None:
        if "display_names" not in ws_config:
            ws_config["display_names"] = CommentedMap()

        dn = ws_config["display_names"]
        for key in ["submission", "created_at", "display_id", "igo_percentage"]:
            if key in ui_display_names:
                dn[key] = ui_display_names[key]

        if "statuses" in ui_display_names:
            if "statuses" not in dn:
                dn["statuses"] = CommentedMap()
            for status in ["needs_attention", "in_good_order", "completed", "rejected", "spam"]:
                if status in ui_display_names["statuses"]:
                    dn["statuses"][status] = ui_display_names["statuses"][status]
    elif "display_names" in ws_config and ui_display_names is None and "display_names" in ui_ws:
        # Explicitly removed
        del ws_config["display_names"]

    # ==========================================================================
    # 3. UPDATE KEY_FIELD_IDS
    # ==========================================================================
    if "key_field_ids" in ui_ws:
        ws_config["key_field_ids"] = ensure_commented_seq(ui_ws["key_field_ids"])

    # ==========================================================================
    # 4. UPDATE DOCUMENT_TYPE (in-place)
    # ==========================================================================
    ui_doc_type = ui_ws.get("document_type")
    if ui_doc_type is not None:
        if "document_type" not in ws_config:
            ws_config["document_type"] = CommentedMap()

        dt = ws_config["document_type"]
        if "intake_field_id" in ui_doc_type:
            try:
                dt["intake_field_id"] = int(ui_doc_type["intake_field_id"])
            except (ValueError, TypeError):
                dt["intake_field_id"] = ui_doc_type["intake_field_id"]
        if "all_types" in ui_doc_type:
            dt["all_types"] = ensure_commented_seq(ui_doc_type["all_types"])
        if "types_required_for_igo" in ui_doc_type:
            dt["types_required_for_igo"] = ensure_commented_seq(ui_doc_type["types_required_for_igo"])

    # ==========================================================================
    # 5. UPDATE VIEWS (in-place)
    # ==========================================================================
    if "views" not in ws_config:
        ws_config["views"] = CommentedMap()

    views = ws_config["views"]

    # Update list view
    ui_list = ui_ws.get("views", {}).get("list", {})
    if ui_list:
        if "list" not in views:
            views["list"] = CommentedMap()
        if "title" in ui_list:
            views["list"]["title"] = ui_list["title"]
        if "columns" in ui_list:
            views["list"]["columns"] = ensure_commented_seq(ui_list["columns"])

    # Update review view
    ui_review = ui_ws.get("views", {}).get("review", {})
    ui_sections = ui_review.get("sections", [])

    if "review" not in views:
        views["review"] = CommentedMap()

    review = views["review"]

    # Handle fallback_section
    ui_fallback_disabled = ui_config.get("fallback_section_disabled", False)
    if ui_fallback_disabled:
        if "fallback_section" not in review:
            review["fallback_section"] = CommentedMap()
        review["fallback_section"]["enabled"] = False
    elif "fallback_section" in review:
        del review["fallback_section"]

    # Update sections
    if ui_sections:
        # Get content key order from original
        content_key_order = ['type', 'id']  # Default
        if "sections" in review and review["sections"]:
            first_section = review["sections"][0]
            if "contents" in first_section and first_section["contents"]:
                if hasattr(first_section["contents"][0], 'keys'):
                    content_key_order = list(first_section["contents"][0].keys())

        new_sections = CommentedSeq()
        for ui_sec in ui_sections:
            section = CommentedMap()
            section["section_name"] = ui_sec.get("section_name", "New Section")

            contents = CommentedSeq()
            if "contents" in ui_sec:
                for item in ui_sec["contents"]:
                    content_item = CommentedMap()
                    for key in content_key_order:
                        if key in item:
                            content_item[key] = item[key]
                    for key, val in item.items():
                        if key not in content_item:
                            content_item[key] = val
                    contents.append(content_item)
            elif "field_ids" in ui_sec:
                for fid in ui_sec["field_ids"]:
                    content_item = CommentedMap()
                    for key in content_key_order:
                        if key == 'type':
                            content_item['type'] = 'field'
                        elif key == 'id':
                            content_item['id'] = fid
                    contents.append(content_item)

            section["contents"] = contents
            new_sections.append(section)

        review["sections"] = new_sections

    # ==========================================================================
    # 6. UPDATE TABLES (in-place where possible)
    # ==========================================================================
    ui_tables = ui_ws.get("tables")

    if ui_tables is not None:
        # Build lookup of original tables
        original_tables_by_id = {}
        table_key_order = ['id', 'display_name', 'intake_group_name', 'column_field_ids']

        if "tables" in ws_config:
            for t in ws_config["tables"]:
                tid = t.get("id")
                if tid:
                    original_tables_by_id[tid] = t
            if ws_config["tables"]:
                table_key_order = list(ws_config["tables"][0].keys())

        new_tables = CommentedSeq()
        for ui_table in ui_tables:
            table_id = ui_table.get("id", "")

            if table_id and table_id in original_tables_by_id:
                # Update existing table in-place
                orig_table = original_tables_by_id[table_id]
                if "display_name" in ui_table:
                    orig_table["display_name"] = ui_table["display_name"]
                if "intake_group_name" in ui_table:
                    orig_table["intake_group_name"] = ui_table["intake_group_name"]
                if "column_field_ids" in ui_table:
                    orig_table["column_field_ids"] = ensure_commented_seq(ui_table["column_field_ids"])
                new_tables.append(orig_table)
            else:
                # New table
                table = CommentedMap()
                for key in table_key_order:
                    if key in ui_table:
                        if key == "column_field_ids":
                            table[key] = ensure_commented_seq(ui_table[key])
                        else:
                            table[key] = ui_table[key]
                for key, val in ui_table.items():
                    if key not in table:
                        if isinstance(val, list):
                            table[key] = ensure_commented_seq(val)
                        else:
                            table[key] = val
                new_tables.append(table)

        ws_config["tables"] = new_tables

        # If all tables were deleted, remove the key entirely
        if not new_tables and "tables" in ws_config:
            del ws_config["tables"]

    # ==========================================================================
    # 7. UPDATE AGENTS
    # ==========================================================================
    ui_agents = ui_ws.get("agents")
    if ui_agents is not None:
        if "agents" not in ws_config:
            ws_config["agents"] = CommentedMap()

        # Update submission_update_agent
        if "submission_update_agent" in ui_agents:
            if "submission_update_agent" not in ws_config["agents"]:
                ws_config["agents"]["submission_update_agent"] = CommentedMap()

            sua = ws_config["agents"]["submission_update_agent"]
            ui_sua = ui_agents["submission_update_agent"]

            if "autospam_igo_percentage_min_threshold" in ui_sua:
                sua["autospam_igo_percentage_min_threshold"] = ui_sua["autospam_igo_percentage_min_threshold"]

            if "automerge_associative_field_ids" in ui_sua:
                sua["automerge_associative_field_ids"] = ensure_commented_seq(ui_sua["automerge_associative_field_ids"])

            if "suggested_actions" in ui_sua:
                sua["suggested_actions"] = ensure_commented_map(ui_sua["suggested_actions"])

            if "advisor_agent_config" in ui_sua:
                aac = ui_sua["advisor_agent_config"]
                if "advisor_agent_config" not in sua:
                    sua["advisor_agent_config"] = CommentedMap()

                advisor_config = sua["advisor_agent_config"]

                if "rules" in aac:
                    rules = CommentedSeq()
                    for rule in aac["rules"]:
                        rule_map = CommentedMap()

                        # Build cases
                        if "cases" in rule:
                            cases_map = CommentedMap()
                            for case_name, case_actions in rule["cases"].items():
                                if isinstance(case_actions, list):
                                    actions_seq = CommentedSeq()
                                    for action in case_actions:
                                        action_map = CommentedMap()
                                        if "status" in action:
                                            action_map["status"] = action["status"]
                                        if "type" in action:
                                            action_map["type"] = action["type"]
                                        for k, v in action.items():
                                            if k not in action_map:
                                                action_map[k] = v
                                        actions_seq.append(action_map)
                                    cases_map[case_name] = actions_seq
                                else:
                                    cases_map[case_name] = case_actions
                            rule_map["cases"] = cases_map

                        # Build condition
                        if "condition" in rule:
                            cond = rule["condition"]
                            condition_map = CommentedMap()
                            condition_map["type"] = cond.get("type", "natural_language")
                            condition_map["value"] = str(cond.get("value", "")).strip()
                            rule_map["condition"] = condition_map

                        rule_map["type"] = rule.get("type", "rule")
                        rules.append(rule_map)

                    advisor_config["rules"] = rules

                advisor_config["type"] = "rules"
            elif "advisor_agent_config" in sua:
                # Remove if UI removed it
                del sua["advisor_agent_config"]

    # ==========================================================================
    # 7b. SYNC ORCHESTRATOR AGENTS (mirror workspaceConfig.agents changes)
    # ==========================================================================
    # Some YAML files have a duplicate agent config under orchestrator.agents
    # that must be kept in sync with workspaceConfig.agents
    orchestrator = ws_config.get("orchestrator")
    if orchestrator and isinstance(orchestrator, (dict, CommentedMap)):
        orch_agents = orchestrator.get("agents")
        if orch_agents and isinstance(orch_agents, (dict, CommentedMap)):
            orch_sua = orch_agents.get("submission_update_agent")
            if orch_sua is not None and isinstance(orch_sua, (dict, CommentedMap)):
                # Mirror all the same updates we made to ws_config.agents.submission_update_agent
                ws_sua = ws_config.get("agents", {}).get("submission_update_agent")
                if ws_sua:
                    # Sync simple values
                    if "autospam_igo_percentage_min_threshold" in ws_sua:
                        orch_sua["autospam_igo_percentage_min_threshold"] = ws_sua["autospam_igo_percentage_min_threshold"]
                    if "automerge_associative_field_ids" in ws_sua:
                        orch_sua["automerge_associative_field_ids"] = ws_sua["automerge_associative_field_ids"]
                    if "suggested_actions" in ws_sua:
                        orch_sua["suggested_actions"] = ws_sua["suggested_actions"]

                    # Sync advisor_agent_config
                    if "advisor_agent_config" in ws_sua:
                        orch_sua["advisor_agent_config"] = ws_sua["advisor_agent_config"]
                    elif "advisor_agent_config" in orch_sua:
                        del orch_sua["advisor_agent_config"]

    # ==========================================================================
    # 8. UPDATE THRESHOLDS (server.services.lagoon.env)
    # ==========================================================================
    ui_thresholds = ui_config.get("thresholds")

    if ui_thresholds and ui_thresholds.get("enabled"):
        if "server" not in data:
            data["server"] = CommentedMap()
        if "services" not in data["server"]:
            data["server"]["services"] = CommentedMap()
        if "lagoon" not in data["server"]["services"]:
            data["server"]["services"]["lagoon"] = CommentedMap()
        if "env" not in data["server"]["services"]["lagoon"]:
            data["server"]["services"]["lagoon"]["env"] = CommentedMap()

        env = data["server"]["services"]["lagoon"]["env"]
        env["FIELD_AUTOACCEPT_CONFIDENCE"] = float(ui_thresholds.get("autoconfirm", 0.9))
        env["FIELD_AUTOREJECT_CONFIDENCE"] = float(ui_thresholds.get("autoreject", 0.4))
        # Remove old key
        if "FIELD_AUTOCONFIRM_CONFIDENCE" in env:
            del env["FIELD_AUTOCONFIRM_CONFIDENCE"]
    elif ui_thresholds is not None and not ui_thresholds.get("enabled"):
        # Remove thresholds if disabled
        try:
            env = data["server"]["services"]["lagoon"]["env"]
            for key in ["FIELD_AUTOACCEPT_CONFIDENCE", "FIELD_AUTOREJECT_CONFIDENCE", "FIELD_AUTOCONFIRM_CONFIDENCE"]:
                if key in env:
                    del env[key]
        except (KeyError, TypeError):
            pass

    # ==========================================================================
    # 9. UPDATE ADVISOR AGENT FEATURE FLAG
    # ==========================================================================
    advisor_agent_enabled = False
    if ui_agents:
        sua = ui_agents.get("submission_update_agent", {})
        if sua.get("advisor_agent_config") is not None:
            advisor_agent_enabled = True

    if "global" in data and "features" in data["global"]:
        data["global"]["features"]["advisorAgent"] = advisor_agent_enabled
    elif advisor_agent_enabled:
        if "global" not in data:
            data["global"] = CommentedMap()
        if "features" not in data["global"]:
            data["global"]["features"] = CommentedMap()
        data["global"]["features"]["advisorAgent"] = True

    return data


def format_helm_values_string(data: dict, indent: int = 2) -> str:
    """Format HELM_VALUES data to a YAML string."""
    yaml = create_yaml_handler(indent)
    stream = StringIO()
    yaml.dump(data, stream)
    result = stream.getvalue()

    # Clean up trailing whitespace
    lines = result.split('\n')
    cleaned = [line.rstrip() for line in lines]
    return '\n'.join(cleaned)


def apply_helm_values_to_yaml(yaml_content: dict, helm_data: dict, format_info: dict) -> dict:
    """Apply new HELM_VALUES data to the parsed YAML content."""
    inner_indent = format_info.get('inner_indent', 2)
    helm_str = format_helm_values_string(helm_data, inner_indent)

    envs = yaml_content.get("spec", {}).get("source", {}).get("plugin", {}).get("env", [])
    if isinstance(envs, dict):
        envs = [envs]

    for env in envs:
        if env.get("name") == "HELM_VALUES":
            env["value"] = LiteralScalarString(helm_str)
            break

    return yaml_content


def dump_yaml_content(yaml_content: dict) -> bytes:
    """Dump YAML content to bytes with consistent formatting."""
    yaml_handler = create_yaml_handler()
    stream = BytesIO()
    yaml_handler.dump(yaml_content, stream)
    stream.seek(0)
    return stream.getvalue()


def dump_yaml_string(yaml_content: dict) -> str:
    """Dump YAML content to string."""
    return dump_yaml_content(yaml_content).decode('utf-8')


# =============================================================================
# ROUTES
# =============================================================================

@bp.route("/", methods=["GET"])
@login_required
@section_required("ben_app")
def index():
    return render_template(
        "ben_app.html",
        sections=SECTIONS,
        section="ben_app",
        title="Ben's App",
        clusters=list(CLUSTERS.keys())
    )


@bp.route("/api/workspace-config/clone", methods=["POST"])
@login_required
@section_required("ben_app")
def clone_and_load():
    try:
        data = request.get_json(force=True) or {}
        repo_url = (data.get("repoUrl") or "").strip()
        branch = (data.get("branch") or "").strip()
        token = (data.get("token") or "").strip() or None

        clone_dir = get_branch_dir(branch)
        ensure_repo(clone_dir, repo_url, branch, token)

        config_path = find_config_path(clone_dir)
        if not config_path:
            return jsonify(success=False, error="Config file not found"), 404

        # Read and parse the file
        yaml_handler = create_yaml_handler()
        with open(config_path, "r", encoding="utf-8") as f:
            yaml_content = yaml_handler.load(f)

        # Extract HELM_VALUES and store EVERYTHING for format preservation
        helm_values_str = extract_helm_values_string(yaml_content)
        if helm_values_str:
            format_manager.store_original(branch, yaml_content, helm_values_str)

        return jsonify(success=True, config=extract_config(yaml_content))

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify(success=False, error=str(e)), 500


@bp.route("/api/workspace-config/save", methods=["POST"], endpoint="save_local")
@login_required
@section_required("ben_app")
def save_local():
    try:
        data = request.get_json(force=True) or {}
        branch = data.get("branch")
        config = data.get("config") or {}

        if not branch:
            return jsonify(success=False, error="Branch is required"), 400

        clone_dir = get_branch_dir(branch)
        config_path = find_config_path(clone_dir)
        if not config_path:
            return jsonify(success=False, error="Config file not found"), 404

        # Read current file fresh
        yaml_handler = create_yaml_handler()
        with open(config_path, "r", encoding="utf-8") as f:
            yaml_content = yaml_handler.load(f)

        # Extract current HELM_VALUES
        helm_values_str = extract_helm_values_string(yaml_content)
        if helm_values_str:
            original_helm_data = yaml_handler.load(StringIO(helm_values_str))
        else:
            original_helm_data = CommentedMap()

        format_info = format_manager.get_format(branch)

        # Update HELM_VALUES in-place
        helm_data = update_helm_values_in_place(original_helm_data, config, branch)

        # Apply to YAML content
        yaml_content = apply_helm_values_to_yaml(yaml_content, helm_data, format_info)

        # Write back
        with open(config_path, "w", encoding="utf-8") as f:
            yaml_handler.dump(yaml_content, f)

        # Track changes
        repo = Repo(clone_dir)
        repo.git.add(config_path)
        changed = bool(repo.index.diff("HEAD")) or bool(repo.index.diff(None))

        return jsonify(success=True, message="Saved successfully", changed=changed)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify(success=False, error=str(e)), 500


@bp.route("/api/yaml-preview", methods=["POST"])
@login_required
@section_required("ben_app")
def yaml_preview():
    try:
        data = request.get_json(force=True) or {}
        branch = data.get("branch")
        config = data.get("config")

        if not branch:
            return jsonify(success=False, error="Branch is required"), 400

        clone_dir = get_branch_dir(branch)
        config_path = find_config_path(clone_dir)

        if not config_path:
            return jsonify(success=False, error="Config file not found. Please load a config first."), 404

        yaml_handler = create_yaml_handler()
        with open(config_path, "r", encoding="utf-8") as f:
            yaml_content = yaml_handler.load(f)

        helm_values_str = extract_helm_values_string(yaml_content)
        if helm_values_str:
            original_helm_data = yaml_handler.load(StringIO(helm_values_str))
        else:
            original_helm_data = CommentedMap()

        format_info = format_manager.get_format(branch)

        helm_data = update_helm_values_in_place(original_helm_data, config or {}, branch)
        yaml_content = apply_helm_values_to_yaml(yaml_content, helm_data, format_info)

        return jsonify(success=True, yaml=dump_yaml_string(yaml_content))

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify(success=False, error=str(e)), 500


@bp.route("/api/workspace-config/download", methods=["POST"])
@login_required
@section_required("ben_app")
def download_yaml_argocd():
    try:
        data = request.get_json(force=True) or {}
        branch = data.get("branch")
        config = data.get("config") or {}

        if not branch:
            return jsonify(success=False, error="Branch is required"), 400

        clone_dir = get_branch_dir(branch)
        config_path = find_config_path(clone_dir)

        if not config_path:
            return jsonify(success=False, error="Config file not found"), 404

        yaml_handler = create_yaml_handler()
        with open(config_path, "r", encoding="utf-8") as f:
            yaml_content = yaml_handler.load(f)

        helm_values_str = extract_helm_values_string(yaml_content)
        if helm_values_str:
            original_helm_data = yaml_handler.load(StringIO(helm_values_str))
        else:
            original_helm_data = CommentedMap()

        format_info = format_manager.get_format(branch)

        helm_data = update_helm_values_in_place(original_helm_data, config, branch)
        yaml_content = apply_helm_values_to_yaml(yaml_content, helm_data, format_info)

        output = BytesIO(dump_yaml_content(yaml_content))

        return send_file(
            output,
            mimetype="application/x-yaml",
            as_attachment=True,
            download_name="insights_application.yaml"
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify(success=False, error=str(e)), 500


# =============================================================================
# GIT OPERATIONS
# =============================================================================

@bp.route("/api/workspace-config/commit-push", methods=["POST"])
@login_required
@section_required("ben_app")
def commit_and_push():
    try:
        data = request.get_json(force=True) or {}
        repo_url = (data.get("repoUrl") or "").strip()
        branch = (data.get("branch") or "").strip()
        token = (data.get("token") or "").strip() or None
        commit_message = (data.get("commitMessage") or "").strip() or "Update workspace configuration"
        config = data.get("config")

        clone_dir = get_branch_dir(branch)
        repo = ensure_repo(clone_dir, repo_url, branch, token, reset=False)

        config_path = find_config_path(clone_dir)
        if not config_path:
            return jsonify(success=False, error="Config file not found"), 404

        # Regenerate YAML from UI config before pushing
        if config:
            yaml_handler = create_yaml_handler()
            with open(config_path, "r", encoding="utf-8") as f:
                yaml_content = yaml_handler.load(f)

            helm_values_str = extract_helm_values_string(yaml_content)
            if helm_values_str:
                original_helm_data = yaml_handler.load(StringIO(helm_values_str))
            else:
                original_helm_data = CommentedMap()

            format_info = format_manager.get_format(branch)
            helm_data = update_helm_values_in_place(original_helm_data, config, branch)
            yaml_content = apply_helm_values_to_yaml(yaml_content, helm_data, format_info)

            with open(config_path, "w", encoding="utf-8") as f:
                yaml_handler.dump(yaml_content, f)

        # Add and commit
        if config_path:
            try:
                repo.git.add(config_path)
            except Exception:
                pass

        changed = bool(repo.index.diff("HEAD")) or bool(repo.index.diff(None)) or bool(repo.untracked_files)
        committed = False

        if changed:
            repo.config_writer().set_value("user", "name", "john-andel").release()
            repo.config_writer().set_value("user", "email", "john-andel@users.noreply.github.com").release()
            repo.git.add(".")
            repo.index.commit(commit_message)
            committed = True

        authed = auth_url(repo_url, token)
        try:
            repo.remotes.origin.set_url(authed)
        except Exception:
            pass

        repo.git.push("origin", branch)

        return jsonify(success=True, message="Pushed successfully", details={"committed": committed})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify(success=False, error=str(e)), 500


@bp.route("/api/workspace-config/status", methods=["POST"])
@login_required
@section_required("ben_app")
def git_status():
    try:
        data = request.get_json(force=True) or {}
        repo_url = (data.get("repoUrl") or "").strip()
        branch = (data.get("branch") or "").strip()
        token = (data.get("token") or "").strip() or None

        clone_dir = get_branch_dir(branch)
        repo = ensure_repo(clone_dir, repo_url, branch, token, reset=False)

        status_output = repo.git.status()
        diff_output = repo.git.diff() if repo.index.diff(None) else "No changes"

        return jsonify(success=True, status=status_output, diff=diff_output)

    except Exception as e:
        return jsonify(success=False, error=str(e)), 500


@bp.route("/api/workspace-config/branches", methods=["POST"])
def get_branches():
    try:
        data = request.get_json(force=True) or {}
        repo_url = (data.get("repoUrl") or "").strip()
        token = (data.get("token") or "").strip() or None

        temp_dir = os.path.join(CLONE_BASE_DIR, "_temp_ls_remote")
        os.makedirs(temp_dir, exist_ok=True)
        os.environ["GIT_TERMINAL_PROMPT"] = "0"
        authed_url = auth_url(repo_url, token)

        result = subprocess.run(
            ["git", "ls-remote", "--heads", authed_url],
            capture_output=True, text=True, cwd=temp_dir, timeout=30
        )

        remote_branches = []
        if result.returncode == 0:
            for line in result.stdout.strip().split('\n'):
                if line:
                    parts = line.split('\t')
                    if len(parts) == 2 and parts[1].startswith('refs/heads/'):
                        remote_branches.append(parts[1].replace('refs/heads/', ''))

        return jsonify(success=True, branches=sorted(list(set(remote_branches))) or ['main'])
    except Exception as e:
        return jsonify(success=False, error=str(e)), 500


# =============================================================================
# S3 & UTILITY ROUTES
# =============================================================================

@bp.route("/list-s3-buckets", methods=["GET"])
@login_required
@section_required("ben_app")
def list_s3_buckets():
    try:
        s3_client = boto3.client('s3')
        response = s3_client.list_buckets()
        buckets = [b['Name'] for b in response.get('Buckets', []) if b['Name'].startswith('indico-')]
        return jsonify(success=True, buckets=sorted(buckets))
    except Exception as e:
        return jsonify(success=False, error=str(e)), 500


@bp.route("/upload-logos", methods=["POST"])
@login_required
@section_required("ben_app")
def upload_logos():
    try:
        bucket_name = request.form.get('bucketName', '').strip()
        if not bucket_name:
            return jsonify(success=False, error="Bucket name required"), 400

        s3_client = boto3.client('s3')
        uploaded = []

        files_map = {
            'favicon': 'favicon.ico',
            'logo-light-icon': 'logo-light-icon.png',
            'logo-dark-icon': 'logo-dark-icon.png',
            'logo-light-full': 'logo-light-full.png',
            'logo-dark-full': 'logo-dark-full.png'
        }

        for field, fname in files_map.items():
            if field in request.files and request.files[field].filename:
                f = request.files[field]
                key = f"static/logos/{fname}"
                ct = 'image/x-icon' if fname.endswith('.ico') else 'image/png'
                s3_client.put_object(Bucket=bucket_name, Key=key, Body=f.read(), ContentType=ct, ACL='public-read')
                uploaded.append(fname)

        return jsonify(success=True, bucket=bucket_name, count=len(uploaded))
    except Exception as e:
        return jsonify(success=False, error=str(e)), 500


@bp.route("/check-logos", methods=["POST"])
@login_required
@section_required("ben_app")
def check_logos():
    try:
        bucket = request.json.get('bucketName', '').strip()
        s3 = boto3.client('s3')
        logos = {}
        for f in ['favicon.ico', 'logo-light-icon.png', 'logo-dark-icon.png', 'logo-light-full.png', 'logo-dark-full.png']:
            try:
                s3.head_object(Bucket=bucket, Key=f"static/logos/{f}")
                logos[f] = {'exists': True}
            except ClientError:
                logos[f] = {'exists': False}
        return jsonify(success=True, logos=logos)
    except Exception as e:
        return jsonify(success=False, error=str(e)), 500


@bp.route("/api/get-workflow-fields", methods=["POST"])
@login_required
@section_required("ben_app")
def get_workflow_fields():
    try:
        p = request.get_json()
        wid = p.get("workflow_id")
        if not wid:
            return jsonify(success=False, error="Workflow ID required"), 400

        host = (p.get("host") or "").replace("https://", "").rstrip("/")
        token = p.get("api_token")

        if not host:
            env = CLUSTERS.get(p.get("environment"))
            if not env:
                return jsonify(success=False, error="Invalid env"), 400
            host = env["host"]
            with open(env["token_path"]) as f:
                token = f.read().strip()

        client = IndicoClient(IndicoConfig(host=host, api_token=token))
        q = """query($wid: Int!) { fields(workflowId: $wid) { id name datatype } }"""
        res = client.call(GraphQLRequest(query=q, variables={"wid": int(wid)}))
        return jsonify(success=True, fields=res.get("fields", []))
    except Exception as e:
        return jsonify(success=False, error=str(e)), 500


@bp.route("/list-clusters", methods=["GET"])
@login_required
@section_required("ben_app")
def list_clusters():
    return jsonify(success=True, clusters=list(CLUSTERS.keys()))

# =============================================================================
# ENRICHMENT ROUTES
# =============================================================================


def _parse_int(value, field_name: str):
    if value is None or str(value).strip() == "":
        raise ValueError(f"{field_name} is required")
    try:
        return int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc


def _parse_required_str(value, field_name: str) -> str:
    parsed = str(value).strip() if value is not None else ""
    if not parsed:
        raise ValueError(f"{field_name} is required")
    return parsed


def _parse_column_names(value) -> list[str]:
    if isinstance(value, list):
        columns = [str(item).strip() for item in value if str(item).strip()]
    else:
        raw = str(value).strip() if value is not None else ""
        columns = [item.strip() for item in raw.split(",") if item.strip()]

    if not columns:
        raise ValueError("column_names must contain at least one value")
    return columns


@bp.route("/api/enrich-duns", methods=["POST"])
@login_required
@section_required("ben_app")
def enrich_duns():
    try:
        payload = request.get_json(force=True) or {}

        submission_id = _parse_int(payload.get("submission_id"), "submission_id")
        workflow_option = _parse_int(payload.get("workflow_option"), "workflow_option")
        workspace_option = _parse_int(payload.get("workspace_option"), "workspace_option")
        insured_name_id = _parse_required_str(payload.get("insured_name_id"), "insured_name_id")
        workspace_duns_id = _parse_required_str(payload.get("workspace_duns_id"), "workspace_duns_id")

        duns = asyncio.run(
            run_duns_enrichment(
                submission_id=submission_id,
                workflow_option=workflow_option,
                workspace_option=workspace_option,
                insured_name_id=insured_name_id,
                workspace_duns_id=workspace_duns_id,
            )
        )

        return jsonify(success=True, enrichment_type="duns", result={"duns": duns})

    except ValueError as e:
        return jsonify(success=False, error=str(e)), 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify(success=False, error=str(e)), 500


@bp.route("/api/enrich-swissre", methods=["POST"])
@login_required
@section_required("ben_app")
def enrich_swissre():
    try:
        payload = request.get_json(force=True) or {}

        submission_id = _parse_int(payload.get("submission_id"), "submission_id")
        workflow_option = _parse_int(payload.get("workflow_option"), "workflow_option")
        workspace_option = _parse_int(payload.get("workspace_option"), "workspace_option")
        column_names = _parse_column_names(payload.get("column_names"))
        table_id = str(payload.get("table_id", "CatNet")).strip() or "CatNet"

        result = asyncio.run(
            run_swissre_enrichment(
                submission_id=submission_id,
                workflow_option=workflow_option,
                workspace_option=workspace_option,
                column_names=column_names,
                table_id=table_id,
            )
        )

        return jsonify(success=True, enrichment_type="swissre_catnet", result=result)

    except ValueError as e:
        return jsonify(success=False, error=str(e)), 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify(success=False, error=str(e)), 500
