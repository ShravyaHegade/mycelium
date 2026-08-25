# Provider reconciliation

Use reconciliation only for consequential operations with a stable external
operation handle and a read-only provider query.

Implement these outcomes narrowly:

- `COMPLETED`: provider evidence uniquely proves the effect happened.
- `NOT_EXECUTED`: positive provider evidence proves the effect cannot have
  happened. Absence is insufficient unless the provider offers authoritative,
  strongly consistent negative proof.
- `UNKNOWN`: zero results under possible lag, multiple matches, intermediate
  status, malformed responses, timeouts, permission failures, or uncertainty.

Record the external handle before the ambiguous provider boundary whenever the
provider protocol permits it. Never make the reconciler send, retry, modify,
delete, cancel, or repair an operation.

For a new adapter:

1. Implement the `Reconciler` lookup and canonicalize/reject malformed handles.
2. Implement `ProviderConformanceFixture` with scripted observations and a
   `ProviderCallAudit`-instrumented provider double. Expose every mutation method
   the adapter could reach so forbidden calls are recorded and fail the suite.
3. Run `run_provider_conformance_cases` during development.
4. In trusted CI, set `MYCELIUM_ADAPTER_REPORT_SIGNING_KEY` from a secret manager
   and call `create_adapter_verification_report`, or use the shipped provider CLI
   when the adapter is registered.
5. Verify the signed report against the installed source and keep live provider
   credentials restricted to read-only scopes.

Do not place signing keys in YAML, source, fixtures, reports, shell history, or
test snapshots. A signed synthetic report binds test results to source; it does
not attest to live provider consistency or credentials.
