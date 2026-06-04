import json
from pathlib import Path

import jsonschema
import pytest

from orchestrator.core.entities import WorkflowDefinition
from orchestrator.core.validator import WorkflowDefinitionValidator

ROOT = Path(__file__).resolve().parents[3]
SCHEMA_DIR = ROOT / "schemas"
EXAMPLES_DIR = ROOT / "examples"

EXAMPLES = sorted(EXAMPLES_DIR.glob("*.json"))


def _pipeline_schema() -> dict:
    return json.loads((SCHEMA_DIR / "pipeline-1.0.schema.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("example_path", EXAMPLES, ids=lambda p: p.name)
def test_example_matches_json_schema(example_path: Path):
    payload = json.loads(example_path.read_text(encoding="utf-8"))
    jsonschema.validate(payload, _pipeline_schema())


@pytest.mark.parametrize("example_path", EXAMPLES, ids=lambda p: p.name)
def test_example_parses_and_validates(example_path: Path):
    payload = json.loads(example_path.read_text(encoding="utf-8"))
    definition = WorkflowDefinition.model_validate(payload)
    WorkflowDefinitionValidator().validate(definition)


def test_pipeline_schema_is_valid_draft():
    jsonschema.Draft202012Validator.check_schema(_pipeline_schema())
