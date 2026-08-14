# Fleek Airbyte Custom Patch Catalog

Last verified: 2026-08-14

This file is the canonical inventory of Fleek-maintained Airbyte connector changes. The `personal-fork` remote (`snktagarwal/airbyte-upstream`) is the source-code home; upstream Airbyte pull requests are optional and are not part of the deployment path.

Never add source or destination credentials to this file. Runtime credentials remain in Airbyte/Kubernetes secrets.

## Status summary

| Patch | Source branch / commits | Production image | Airbyte definition | Status |
| --- | --- | --- | --- | --- |
| Zendesk inaccessible side-conversation handling | `codex/zendesk-side-conversations-403` / `ba7f202e50`, `bc978deb63`, `a0a2d65fbf` | `source-zendesk-support:5.5.0-side403fix.1` (`sha256:da38cc95a1fe60b12fe9d634919d52a736964f4e8d4aca826f4a3fa08d54f85a`) | `79c1aa37-dae3-42ae-b333-d1c105477715` | Deployed and verified |
| BigQuery preserve soft-delete values | `codex/preserve-soft-delete-values` / `4c0841607a`, `86f41c3f6c` | `destination-bigquery:3.0.17-preserve-soft-delete.3` (`sha256:aabbbaa6bcf77e61d8b4fcabbe2851694655d5e42d38c40e0d6f4c6f47c8449b`) | `0369de09-6a13-4908-b92a-68446b776246` | Deployed and verified |
| Shopify client credentials and grouped-stream checkpoint fixes | `codex/shopify-client-credentials` / working-tree snapshot based on `bed1670bc6` | `source-shopify:3.5.1-collectionsfix.2` (`sha256:6873b1e50452dd74d75c29b9f4e04a64ee15dbca217ec38e64f35610535dec30`) | `bf4d6c1a-6f5a-40fe-8341-02c2fa526b66` | Deployed; source changes must still be committed |

Artifact registry prefix for all images above:

`us-central1-docker.pkg.dev/dogwood-baton-345622/airbyte-connectors/`

## Zendesk: inaccessible side conversations

Problem:

- Zendesk returns `403 Forbidden` for side-conversation child endpoints on some tickets even when the parent ticket is readable.
- Treating those ticket-scoped authorization failures as fatal caused the entire incremental sync to fail repeatedly.

Behavior:

- Skip only the inaccessible ticket-scoped side-conversation response.
- Continue syncing accessible tickets and preserve normal failures for unrelated HTTP errors.
- Connector metadata version is `5.5.2`; the deployed Fleek image retains the historical deployment tag `5.5.0-side403fix.1`.

Source files:

- `airbyte-integrations/connectors/source-zendesk-support/source_zendesk_support/manifest.yaml`
- `airbyte-integrations/connectors/source-zendesk-support/unit_tests/test_side_conversations.py`
- `airbyte-integrations/connectors/source-zendesk-support/metadata.yaml`

Verification:

- Targeted tests: 8 passed.
- Full connector unit suite: 218 passed.
- Production: multiple consecutive successful incremental jobs with zero rejected records after deployment.

Production connections:

| Connection | ID | Schedule | Streams |
| --- | --- | --- | --- |
| `zendesk-all-streams-incremental` | `4b05eec4-2430-44c8-91a6-c04c2beaad13` | Every 30 minutes | 41 |
| `zendesk-ticket-audits-incremental` | `d8134366-ad2b-42c6-939a-15ac5db4fa00` | Every 30 minutes | 1 |

The manual `zendesk-side-conversations-quarantine` connection (`7378106a-ac76-4e22-9d81-8e15e857dd54`) is retained only as a diagnostic fallback.

## BigQuery: preserve values on soft deletes

Problem:

- CDC tombstones can contain only keys and delete metadata.
- Default destination merge behavior replaces non-key values with `NULL`/defaults, destroying the last known row contents.
- Same-batch insert/update plus delete requires preserving the values from the preceding record in that batch, not only the already-finalized table.

Behavior:

- Mark the row as deleted while retaining its latest known non-key field values.
- Preserve same-batch values when the tombstone follows a fuller record before finalization.

Source files:

- `airbyte-integrations/connectors/destination-bigquery/src/main/kotlin/io/airbyte/integrations/destination/bigquery/typing_deduping/BigqueryDirectLoadSqlGenerator.kt`
- Corresponding direct-load SQL generator tests.

Production connection:

| Connection | ID | Schedule | Streams |
| --- | --- | --- | --- |
| `postgres-prod-cdc-pilot` | `73ea68db-1ab3-44c5-9ef2-eb01b7f19e09` | Every 30 minutes | 242 |

Destination instance: `bigquery-postgres-airbyte` (`1022a945-b372-42f7-8795-2803c1d4ccaf`).

## Shopify: client credentials and grouped checkpoint safety

Problem set:

- Shopify's Dev Dashboard client-credentials flow requires exchanging client ID and secret for a short-lived Admin API access token.
- Grouped GraphQL BULK streams can checkpoint an incomplete parent group if partial canceled results are consumed.
- Collection metafield child records need to advance state using the parent collection cursor, including when a parent produces no child records.

Behavior:

- Exchange client credentials at `/admin/oauth/access_token`, cache the token, and refresh it when needed.
- Disable partial checkpoint consumption for grouped record-component streams.
- Group collection metafields under `Collections` and carry parent state through empty child results.

Source files currently modified in `/Users/sanket/workspace/airbyte`:

- `airbyte-integrations/connectors/source-shopify/source_shopify/auth.py`
- `airbyte-integrations/connectors/source-shopify/source_shopify/shopify_graphql/bulk/job.py`
- `airbyte-integrations/connectors/source-shopify/source_shopify/shopify_graphql/bulk/query.py`
- `airbyte-integrations/connectors/source-shopify/source_shopify/source.py`
- `airbyte-integrations/connectors/source-shopify/source_shopify/spec.json`
- `airbyte-integrations/connectors/source-shopify/source_shopify/streams/base_streams.py`
- `airbyte-integrations/connectors/source-shopify/source_shopify/streams/streams.py`
- `airbyte-integrations/connectors/source-shopify/unit_tests/test_auth.py`
- `airbyte-integrations/connectors/source-shopify/unit_tests/graphql_bulk/test_grouped_checkpointing.py`
- `docs/integrations/sources/shopify.md`

Production connection:

| Connection | ID | Schedule | Streams |
| --- | --- | --- | --- |
| `shopify-historical-master` | `a34caacc-ce7a-40a2-9218-ae4e79ff2d65` | Every 30 minutes | 31 |

Source instance: `Shopify Client Credentials - Full Historical` (`40b66e17-b648-4586-a6e9-affb6c135851`).

Important: the production image is immutable and deployed, but the latest cumulative Shopify source is still uncommitted in the local worktree. Commit and push that branch to `personal-fork` before attempting any source upgrade or local cleanup.

Earlier image checkpoints retained for rollback/history:

| Tag | Digest | Scope |
| --- | --- | --- |
| `client-credentials-20260805` | `sha256:29537b8472f0b28690ffe6da20c0c1ef4f2a33f07b0002bcfba6ed3d037b0c91` | Client-credentials authentication |
| `3.5.1-productsfix.1` | `sha256:8a197e17e62cf906e752f9a27536eefed9e584be4e65791a347b56b1122c3117` | Product grouped-checkpoint fix |
| `3.5.1-collectionsfix.1` | `sha256:2d823153630a2febb22b47da805ce1b470a6594a9c41819a1c4def9368ffe2b5` | Initial collection-metafield state fix |

## Upgrade and deployment procedure

For every Airbyte upgrade:

1. Create a new patch-refresh branch from the intended upstream Airbyte revision. Do not develop directly on the fork's default branch.
2. Cherry-pick the commits listed above. For Shopify, first create a permanent commit from the currently deployed working-tree snapshot.
3. Resolve conflicts by preserving the documented behavior, not by blindly retaining an old implementation.
4. Run targeted regression tests, then the connector's full unit suite.
5. Build an immutable, Fleek-specific image tag and record both tag and digest in this file.
6. Test the image with a manual canary connection against a disposable dataset or isolated stream set.
7. Compare emitted, committed, rejected, row-count, freshness, key uniqueness, and delete behavior against the current production connector.
8. Update the Airbyte definition only after the canary passes. Keep the previous tag available for immediate rollback.
9. Run at least two scheduled production checkpoints, then update the status and verification date here.

## Repository policy

- `origin` is official `airbytehq/airbyte` and is read-only for Fleek's deployment workflow.
- `personal-fork` is `snktagarwal/airbyte-upstream` and is the durable source-code backup.
- Connector deployment always uses Fleek's Artifact Registry images, never a mutable branch tip.
- A GitHub upstream PR may be opened for community contribution, but its CI, review, merge, or closure does not block Fleek deployment.
- Never force-push a deployed patch branch. Create a new refresh branch for the next upstream version.

## Open maintenance items

- [ ] Commit the cumulative Shopify working-tree changes and targeted regression test.
- [ ] Push `codex/shopify-client-credentials` to `personal-fork`.
- [ ] Push `codex/preserve-soft-delete-values` to `personal-fork` if it is not already present.
- [ ] Close the optional upstream Zendesk PR once no further upstream feedback is desired.
- [ ] Re-verify this catalog whenever a connector definition or image tag changes.
