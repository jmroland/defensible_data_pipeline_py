import pytest
from functools import partial

class ValidationError(Exception):
    """Custom exception for validation errors."""
    def __init__(self, errors):
        self.errors = errors
        super().__init__(str(errors))  # Store errors as a readable string

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

# Sample validation config for testing
validation_config = {
    "schema": {"age": int, "name": str, "email": str},
    "rules": [
        {"description": "Age must be positive", "condition": "data['age'] > 0"},
        {"description": "Name must be at least 3 characters", "condition": "len(data['name']) > 2"},
        {"description": "Email must contain @", "condition": "'@' in data['email']"},
    ]
}

validate = partial(validator, schema=validation_config["schema"], rules=validation_config["rules"])

# ✅ 1. Test Valid Data
def test_valid_data():
    data = {"age": 25, "name": "Alice", "email": "alice@example.com"}
    assert validate(data) is None  # Should not raise an error

# ✅ 2. Test Missing Keys
def test_missing_keys():
    data = {"age": 25, "email": "alice@example.com"}  # Missing "name"
    with pytest.raises(ValidationError) as exc_info:
        validate(data)
    assert "Missing key: name" in str(exc_info.value.errors)

# ✅ 3. Test Type Errors
def test_wrong_types():
    data = {"age": "twenty-five", "name": "Alice", "email": "alice@example.com"}  # age is a string
    with pytest.raises(ValidationError) as exc_info:
        validate(data)
    assert "Expected int, got str" in str(exc_info.value.errors["age"])

# ✅ 4. Test Rule Violations
def test_rule_violation():
    data = {"age": -5, "name": "Jo", "email": "user@example.com"}  # Invalid age, too short name
    with pytest.raises(ValidationError) as exc_info:
        validate(data)

    errors = exc_info.value.errors
    assert "Age must be positive" in errors
    assert "Name must be at least 3 characters" in errors

# ✅ 5. Test Invalid Email Format
def test_invalid_email():
    data = {"age": 30, "name": "John", "email": "userexample.com"}  # Missing "@"
    with pytest.raises(ValidationError) as exc_info:
        validate(data)

    assert "Email must contain @" in exc_info.value.errors

# ✅ 6. Test Rule Evaluation Exception (e.g., Invalid Condition)
def test_rule_exception():
    bad_config = {
        "schema": {"age": int},
        "rules": [{"description": "Invalid rule", "condition": "data['nonexistent'] > 0"}]  # Key does not exist
    }
    bad_validate = partial(validator, schema=bad_config["schema"], rules=bad_config["rules"])

    data = {"age": 25}
    with pytest.raises(ValidationError) as exc_info:
        bad_validate(data)

    assert "Rule evaluation error" in str(exc_info.value.errors["Invalid rule"])
