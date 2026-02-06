# Reachable-From Demo Model

This example demonstrates how to use LinkML `reachable_from` to drive enum validation from an ontology.

Key idea:
- The input schema defines an enum whose permissible values are **expanded at registration time**
  based on the ontology terms reachable from a root concept.

In this example:
- `source_ontology` points to the public PATO ontology (downloaded on demand).
- `source_nodes` contains the root term `PATO:0000047` ("biological sex").
- The expansion yields `PATO:0000047`, `PATO:0000383` (female), and `PATO:0000384` (male).
- A custom `Unknown` value is included as a local extension using the `CHARM:` prefix.

This model just echoes the input fields into a simple output so validation behavior is easy to see.
