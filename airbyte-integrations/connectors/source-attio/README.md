# Attio source

Declarative Airbyte source for the Attio REST API v2.

The connector performs full-refresh reads of objects, object attributes,
records, lists, list attributes, entries, notes, and workspace members. It is
designed to land append-only snapshots; deletion detection belongs in the
downstream compatibility layer and must only use completed snapshots.

Never store an Attio token in this directory. Supply `api_token` through the
Airbyte secret configuration at runtime.

