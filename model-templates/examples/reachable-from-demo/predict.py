#!/usr/bin/env python3
import json
import sys

def main() -> int:
    if len(sys.argv) not in (2, 3):
        print("Usage: predict <input.json> [output.json]", file=sys.stderr)
        return 1

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) == 3 else None

    with open(input_file, "r") as f:
        records = json.load(f)

    outputs = []
    for record in records:
        age = record.get("age_years")
        outputs.append({
            "normalized_sex": record.get("biological_sex"),
            "is_adult": bool(age is not None and age >= 18),
        })

    if output_file:
        with open(output_file, "w") as f:
            json.dump(outputs, f, indent=2)
    else:
        print(json.dumps(outputs))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
