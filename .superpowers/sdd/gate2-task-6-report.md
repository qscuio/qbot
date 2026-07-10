# Gate 2 Task 6 Report: Add strict extraction Schema and adapter

## What I implemented

- Added `src/analysis/events/extraction.rs` with the strict extraction schema:
  - `EventExtractionV1`
  - `ExtractedClaim`
  - `ExtractedEntity`
  - `ExtractedAmount`
  - `ExtractedDate`
  - `ClaimType`
- Applied `#[serde(deny_unknown_fields)]` to the extraction structs so unknown JSON fields are rejected at deserialize time.
- Implemented deterministic validation for Task 6 requirements:
  - `fact` claims must include evidence ids;
  - claim confidence must stay within `[0,1]`;
  - every claim evidence id must belong to the extraction input;
  - amount/date `raw_text` must appear in the extraction input content;
  - direct stock codes must resolve against an explicit known-stock directory;
  - rumor and journalist-interpretation claims cannot be promoted to `fact` claims with the same normalized text.
- Added a narrow stock-code validation boundary with `StockCodeLookup` and `StockCodeDirectory` instead of hard-coding database behavior into schema validation.
- Added fixture-backed schema coverage in `tests/fixtures/event_extraction_v1.json`.
- Replaced the private placeholder extractor trait usage in `src/analysis/events/mod.rs` with the real crate-visible extraction surface from `events::extraction`, while keeping the public module interface small.
- Added `src/analysis/adapters/llm_event_extractor.rs` and exposed it from `src/analysis/adapters/mod.rs`.
- Implemented `LlmEventExtractor` with:
  - OpenAI-compatible `chat/completions` HTTP settings from `Config`;
  - dedicated extraction system prompt;
  - `temperature: 0`;
  - JSON-only request shape via `response_format.type = "json_object"`;
  - parse-once behavior per response;
  - a single repair retry with a repair prompt that includes validation failures;
  - validation error return after the second failed response;
  - prompt version, model name, schema version, and model parameters in adapter output metadata.
- Kept the work scoped to candidate-claim extraction only. No publishing, ranking, GDELT, EventCluster, ImpactHypothesisGraph, or beneficiary-expansion wiring was added.

## TDD RED/GREEN evidence

### RED for schema/validation tests

Command:

```bash
cargo test analysis::events::extraction -- --nocapture
```

RED output excerpt before implementation:

```text
error[E0422]: cannot find struct, variant or union type `EventExtractionV1` in this scope
 --> src/analysis/events/extraction.rs:7:26
  |
7 |         let extraction = EventExtractionV1 {
  |                          ^^^^^^^^^^^^^^^^^ not found in this scope

error[E0433]: failed to resolve: use of undeclared type `ClaimType`
  --> src/analysis/events/extraction.rs:11:29
   |
11 |                 claim_type: ClaimType::Fact,
   |                             ^^^^^^^^^ use of undeclared type `ClaimType`

error: could not compile `qbot` (bin "qbot" test) due to 6 previous errors; 1 warning emitted
```

### GREEN for schema/validation tests

Command:

```bash
cargo test analysis::events::extraction -- --nocapture
```

GREEN result:

```text
running 9 tests
test analysis::events::extraction::tests::claim_evidence_ids_must_belong_to_the_extraction_input ... ok
test analysis::events::extraction::tests::confidence_outside_unit_interval_fails_validation ... ok
test analysis::events::extraction::tests::direct_stock_codes_must_map_to_known_stock_info ... ok
test analysis::events::extraction::tests::date_and_amount_values_must_appear_in_source_content ... ok
test analysis::events::extraction::tests::fact_claim_requires_evidence_ids ... ok
test analysis::events::extraction::tests::flexible_stock_codes_resolve_against_known_directory ... ok
test analysis::events::extraction::tests::rumor_and_journalist_interpretation_cannot_be_promoted_to_facts ... ok
test analysis::events::extraction::tests::unknown_json_fields_are_rejected ... ok
test analysis::events::extraction::tests::fixture_round_trips ... ok

test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 266 filtered out; finished in 0.02s
```

### Extra RED/GREEN cycle for the LLM adapter

RED command:

```bash
cargo test analysis::adapters::llm_event_extractor -- --nocapture
```

RED output excerpt before implementation:

```text
error[E0433]: failed to resolve: use of undeclared type `LlmEventExtractor`
  --> src/analysis/adapters/llm_event_extractor.rs:17:25
   |
17 |         let extractor = LlmEventExtractor::new(
   |                         ^^^^^^^^^^^^^^^^^ use of undeclared type `LlmEventExtractor`
```

GREEN result after implementation:

```text
running 4 tests
test analysis::adapters::llm_event_extractor::tests::extracts_valid_json_with_zero_temperature_and_prompt_metadata ... ok
test analysis::adapters::llm_event_extractor::tests::from_config_uses_openai_compatible_settings ... ok
test analysis::adapters::llm_event_extractor::tests::returns_validation_error_after_second_failure ... ok
test analysis::adapters::llm_event_extractor::tests::retries_once_with_a_repair_prompt_after_invalid_first_response ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 271 filtered out; finished in 0.20s
```

## Verification run

- `cargo fmt --all -- --check`
  - Passed.
- `cargo test analysis::events::extraction -- --nocapture`
  - Passed: 9 tests.
  - Warning noise present from existing unused/dead-code items in unrelated modules; output was not pristine.
- `cargo test analysis::adapters::llm_event_extractor -- --nocapture`
  - Passed: 4 tests.
  - Same existing warning noise present.
- `git diff --check`
  - Passed.

## Files changed

- `src/analysis/events/mod.rs`
- `src/analysis/events/extraction.rs`
- `src/analysis/adapters/mod.rs`
- `src/analysis/adapters/llm_event_extractor.rs`
- `tests/fixtures/event_extraction_v1.json`
- `.superpowers/sdd/gate2-task-6-report.md`

## Self-review findings

- The extraction surface is crate-visible, not publicly exported from `analysis::events`, so the public module interface stays small.
- The adapter only returns candidate-claim extraction output and metadata. It does not publish claims or change ranking behavior.
- Validation happens after every parse attempt, and the retry path stops after exactly one repair request.
- The stock-code validation seam is explicit and testable via `StockCodeLookup`/`StockCodeDirectory`; no hidden DB fallback was added.
- The persisted-metadata pieces required by the brief are present in `EventExtractionMetadata`:
  - `schema_version`
  - `prompt_version`
  - `model_name`
  - `model_parameters`

## Issues / concerns

- Focused test runs emit existing dead-code and unused-item warnings from unrelated parts of the crate. The required commands passed, but the output is not warning-free.
