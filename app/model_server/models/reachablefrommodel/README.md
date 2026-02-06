# ReachableFromModel

This demo model exercises LinkML `reachable_from` enum expansion during registration.
It accepts a biological sex term and age in years, then echoes a normalized output.

The input schema uses the public PATO ontology (downloaded on demand):
`http://purl.obolibrary.org/obo/pato.obo`

The root term `PATO:0000047` (biological sex) has children like
`PATO:0000383` (female) and `PATO:0000384` (male), so reachable_from expansion
should permit those values.
