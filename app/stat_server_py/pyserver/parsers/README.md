# FHIR Resource Parsers

This directory contains resource-type-specific parsers that extract clinically relevant information from FHIR resources into clean, human-readable dataframes.

## Architecture

Each parser is responsible for:
1. Extracting clinically relevant fields from raw FHIR resources
2. Extracting cohort information from `meta.tag`
3. Converting complex FHIR structures into simple, readable values
4. Removing FHIR metadata and technical fields
5. Returning a clean pandas DataFrame

## Available Parsers

### ObservationParser

Parses FHIR Observation resources into a clean format with the following fields:

- **cohort**: Extracted from `meta.tag` (defaults to "Default")
- **id**: Observation ID
- **status**: Observation status (e.g., "final")
- **category**: Category display/code (e.g., "Vital signs")
- **code_text**: Human-readable code description (e.g., "Body Height")
- **code**: The actual code (e.g., "8302-2" for LOINC)
- **code_system**: Code system URL (e.g., "http://loinc.org")
- **patient_id**: Patient ID (extracted from reference)
- **encounter_id**: Encounter ID (extracted from reference)
- **effective_date**: Date of observation (YYYY-MM-DD)
- **issued**: Date issued (YYYY-MM-DD)
- **value**: The observation value
- **unit**: The unit of measurement
- **value_with_unit**: Combined display (e.g., "106.3 cm")

## Usage

Parsers are automatically used when the `compact=true` parameter is set on the endpoint:

```
GET /Patient/{patient_id}/Observation?compact=true
```

If a resource-specific parser is available, it will be used. Otherwise, the endpoint falls back to generic compact formatting.

## Adding New Parsers

To add a new parser:

1. Create a new file `{resource_type}_parser.py` (e.g., `condition_parser.py`)
2. Create a parser class with a static `parse(df: pd.DataFrame) -> pd.DataFrame` method
3. Add the parser to `__init__.py`
4. Register it in `main.py` in the `resource_parsers` dictionary

Example structure:

```python
class ConditionParser:
    @staticmethod
    def parse(df: pd.DataFrame) -> pd.DataFrame:
        # Extract relevant fields
        parsed_rows = []
        for idx, row in df.iterrows():
            parsed_row = {
                'cohort': ConditionParser._extract_cohort(row),
                'id': row.get('id'),
                # ... more fields
            }
            parsed_rows.append(parsed_row)
        return pd.DataFrame(parsed_rows)
    
    @staticmethod
    def _extract_cohort(row):
        # Helper method
        pass
```

## Design Principles

1. **Resource-specific**: Each parser knows the structure and important fields for its resource type
2. **Clean output**: Remove all FHIR metadata and technical details
3. **Human-readable**: Extract display names, text descriptions, and codes
4. **Cohort awareness**: Always extract and include cohort information
5. **Null-safe**: Handle missing fields gracefully
6. **Date formatting**: Simplify dates to YYYY-MM-DD format


