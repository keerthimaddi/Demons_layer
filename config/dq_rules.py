DQ_RULES = {
    "DQ1": {
        "name": "Table Exists",
        "description": "Checks whether the table exists and can be accessed.",
        "weight": 1
    },

    "DQ2": {
        "name": "Row Count",
        "description": "Checks whether the table contains records.",
        "weight": 1
    },

    "DQ3": {
        "name": "Duplicate Records",
        "description": "Checks for duplicate records in the table.",
        "weight": 1
    },

    "DQ4": {
        "name": "Null Values",
        "description": "Checks for unexpected null values.",
        "weight": 1
    },

    "DQ5": {
        "name": "Schema Availability",
        "description": "Checks whether the table schema can be read successfully.",
        "weight": 1
    }
}