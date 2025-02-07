from functools import partial

class ValidationError(Exception):
    """Exception raised for validation errors."""

    def __init__(self, errors):
        """
        :param errors: Dictionary of validation failures {field_name: error_message}
        """
        self.errors = errors
        super().__init__(self.__str__())  # Pass a readable message to Exception

    def __str__(self):
        """Convert errors dictionary into a readable string."""
        return f"Validation failed: {self.errors}"

def validator(data, schema, rules):
    """
    Validates input data against a schema and global validation rules.
    Raises ValidationError if validation fails.
    """
    errors = {}

    # Type validation
    for key, expected_type in schema.items():
        if key not in data:
            errors[key] = f"Missing key: {key}"
            continue
        if not isinstance(data[key], expected_type):
            errors[key] = f"Expected {expected_type.__name__}, got {type(data[key]).__name__}"
            continue

    # Rule validation
    for rule in rules:
        try:
            if not eval(rule["condition"], {}, {"data": data}):
                errors[rule["description"]] = f"Validation failed: {rule['condition']}"
        except Exception as e:
            errors[rule["description"]] = f"Rule evaluation error: {e}"

    if errors:
        raise ValidationError(errors)  # Stop execution

    return None  # No errors (execution continues)
