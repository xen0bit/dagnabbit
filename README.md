# dagnabbit

Takes an arbitrary number and arbitrarily structured stream of JSON objects in from stdin.

Because JSON doesn't support pointers, recursively walks structure with depth-first search, expanding the branches as a relational database with schema constraints that uniquely represent duplicate keys at different levels of the structure as distinct nodes.

Serializes the resultant directed acyclic graph into [sqlite, json, csv] for easy import into graph visualization tooling.

TODO: backpropogate the count `ct` from the `dst` of edges to facilitate visually appealing graph representations (sankey)

```bash
jq -c . < test.json | python3 dagnabbit.py csv
```

