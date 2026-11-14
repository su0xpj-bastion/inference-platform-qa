# inference-platform-qa — agent guardrails

Carried over from api-metrics-analyzer, adapted for a platform-QA repo.

## API
Chat Completions ONLY: `client.chat.completions.create()` (or `.with_raw_response.create()` to read headers).
NEVER: Assistants, Responses, Batch, or the `.stream()` helper.
Streaming: `stream=True` + `stream_options={"include_usage": True}`, consumed inside `with ... as stream:`.

## Chunk rules
Guard `chunk.choices` before `chunk.choices[0]`. Three separate if-blocks: content / finish_reason / usage.
NEVER `hasattr` / `getattr` / `isinstance` on a chunk.
A non-stop finish produces `[WARN] stream ended with finish_reason=...`.

## Errors
```
except openai.RateLimitError:   # FIRST: retry with 2**attempt + jitter
except openai.APIStatusError:   # SECOND: has .status_code
except openai.APIError:         # THIRD: base class
```
Use bare `raise`. `MAX_ATTEMPTS`, never `MAX_RETRIES`. Construct the SDK client with `max_retries=0` so attempts are ours and countable.

## Hygiene
- `from __future__ import annotations` on line 1 of every module.
- `MODEL`, `ENCODING`: module-level constants.
- Clients are built in `main()` or in fixtures, never at module level.
- Cost/usage: `usage.prompt_tokens` attribute access, never dict access.

## Tests
- Every test docstring names the platform property it protects.
- Never add sleeps to make a flaky test pass; wait on a condition (`wait_http`, `wait_until`).
- Anything that mutates a cluster is scoped to the `inference` namespace and waits for recovery.
- `cluster` tests must skip cleanly without kubectl.

## Validation before claiming done
`make test && make go && make smoke`. Cluster tests are claimed only when a kind or EKS run actually executed them.
