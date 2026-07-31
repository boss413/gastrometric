"""Shared value types used across knowledge builders."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SourceRecord:
    source_id: int
    name: str
    description: Optional[str] = None


@dataclass
class ObservationRecord:
    observation_id: int
    raw_text: str
    normalized_text: str
    source_id: int
    source_record_id: Optional[str]
    field_name: str


@dataclass
class VocabularyRecord:
    vocabulary_id: int
    term: str
    vocabulary_class: str
    observation_id: Optional[int]


@dataclass
class AliasRecord:
    alias_id: int
    alias_text: str
    vocabulary_id: int


@dataclass
class BuildResult:
    """Summary statistics for a single builder run, used for logging."""

    builder_name: str
    distinct_inputs: int = 0
    observations_inserted: int = 0
    vocabulary_created: int = 0
    aliases_created: int = 0
    unknown_concepts: int = 0
    extra_lines: list[str] = field(default_factory=list)

    def render(self) -> str:
        lines = [
            self.builder_name,
            "",
            f"Distinct modifiers: {self.distinct_inputs:,}",
            "",
            f"Observations inserted: {self.observations_inserted:,}",
            f"Vocabulary entries created: {self.vocabulary_created:,}",
            f"Aliases created: {self.aliases_created:,}",
            f"Unknown concepts: {self.unknown_concepts:,}",
        ]
        lines.extend(self.extra_lines)
        return "\n".join(lines)
