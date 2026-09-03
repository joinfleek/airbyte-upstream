# Attio source

Declarative Airbyte source for the Attio REST API v2.

The connector performs full-refresh reads of objects, object attributes and
their select options/statuses, records, lists, list attributes and their select
options, entries, notes, and workspace members. Configuration streams include
archived records so the downstream layer can preserve the Fivetran history
contract. It is designed to land append-only snapshots; deletion detection
belongs in the downstream compatibility layer and must only use completed
snapshots.

Never store an Attio token in this directory. Supply `api_token` through the
Airbyte secret configuration at runtime.
