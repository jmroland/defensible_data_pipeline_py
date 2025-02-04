
from typing import TypedDict, Any


class InputData(TypedDict):
    row_id: str
    start_value: float
    end_value: float


def validate_input(data: InputData) -> None:
    """Validates input data."""
    if not isinstance(data["start_value"], (int, float)) or not isinstance(data["end_value"], (int, float)):
        raise TypeError("start_value and end_value must be numeric.")
    if data["start_value"] <= 0:
        raise ValueError("start_value must be greater than 0.")


def atomic_function_template(input_data: InputData) -> float:
    """
    Template atomic function with explicit error handling for specific error types.

    Args:
        input_data (InputData): Structured input data.

    Returns:
        float: The computed result.

    Raises:
        KeyError: Raised when a required key is missing.
        ValueError: Raised when a value is invalid or outside acceptable range.
        TypeError: Raised when input types are incorrect.
    """
    try:
        # Validate inputs
        validate_input(input_data)

        # Perform the calculation
        return (input_data["end_value"] - input_data["start_value"]) / input_data["start_value"]

    except KeyError as e:
        print(f"KeyError: Missing key {e}. Input data: {input_data}")
        raise

    except ValueError as e:
        print(f"ValueError: {e}. Input data: {input_data}")
        raise

    except TypeError as e:
        print(f"TypeError: {e}. Input data: {input_data}")
        raise

    except Exception as e:
        print(f"Unexpected error: {e}. Input data: {input_data}")
        raise
