# Gate 2 Task 8 Report

## Summary

Implemented the Task 8 event API surface and Telegram command surface on branch `feat/point-in-time-data-foundation` in the `point-in-time-data-foundation` worktree.

Added:

- `POST /api/analysis/events/manual`
- `GET /api/analysis/events`
- `GET /api/analysis/events/:id`
- `POST /api/analysis/events/:id/review`
- `GET /api/analysis/events/daily-brief`
- Telegram commands: `/event`, `/events`, `/event_detail`, `/event_review`, `/market_facts`

## Planned Files

- Created `src/api/event_routes.rs`
- Modified `src/api/mod.rs`
- Modified `src/api/routes.rs`
- Modified `src/main.rs`
- Added focused route tests in `src/api/event_routes.rs`

## Extra Files Needed

- `src/analysis/events/contracts.rs`
  - Added event list/detail/review/daily-brief contracts so API and Telegram reads stay on the `EventIntelligence` side instead of exposing storage rows.
- `src/analysis/events/evidence.rs`
  - Switched manual validation failures to `BadRequest` so Task 8 validation errors are real client errors.
- `src/analysis/events/mod.rs`
  - Added `EventIntelligence` read/review methods and reviewed-version creation logic to keep event business logic out of API handlers.
- `src/storage/event_repository.rs`
  - Added read helpers for latest/detail/daily-brief plus revision persistence needed by the new routes.
- `src/error.rs`
  - Added `BadRequest` to represent Task 8 validation failures without faking internal-server errors.

## Behavior Notes

- Manual submissions keep the persisted internal status at `pending`, but the external Task 8 response translates just-submitted evidence to `processingStatus: "collected"`.
- Manual submission `sourceReadable` is only returned as known when trimmed content is actually present; URL-only/manual title submissions return `null` until content is fetched or read.
- Manual submission `manualReviewNeeded` remains `null` unless the domain exposes a concrete review-required outcome.
- Review appends a new immutable evidence version with `status = "publishable"` or `status = "rejected"` and `supersedes_evidence_id` set to the reviewed version.
- Review persistence now saves the reviewed evidence row and revision row in one SQL transaction; repository coverage includes a rollback regression when revision persistence fails.
- HTTP review now requires an explicit `action` of `publish` or `reject`; missing, blank, and unsupported actions are rejected as `unauthorized review action`.
- Daily brief reads persisted brief rows only; default lookup now prefers the latest `trade_date` before `generated_at`, and no synthetic brief generation was added in Task 8.
- Content-only REST manual submissions now derive a persisted non-empty title from normalized content, keep `sourceReadable: true`, and hash against the persisted normalized title/content pair.

## Verification

- `DATABASE_URL=postgresql://qbot:qbot@127.0.0.1:5432/qbot cargo test api::event_routes -- --nocapture`
- `cargo fmt --all`
- `cargo fmt --all -- --check`
- `git diff --check`
