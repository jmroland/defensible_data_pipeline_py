# 📌 Defensible Data Pipeline

This project provides a structured approach to building functional data pipelines in Python, adhering to a defensible methodology and emphasizing readability in the style of literate programming but using code to "talk" whenever possible. It automates common data processing tasks according to a playbook of best practices for writing idiomatic, functional Python code.

### 🎯 Key Principles:
- Readability & Testability: The core logic is expressed as clear, idiomatic, functional Python code. This is achieved by isolating atomic functions from boilerplate, decorators, or other abstractions.
- Immutability & Traceability: Every transformation and error is logged, ensuring a transparent audit trail from raw input to final output.
- Automated Best Practices: Standardized patterns make it easy to maintain, extend, and integrate with existing workflows.

The goal is for the pipeline to read like a pipeline, making its transformations explicit and intuitive:

```transformation_one | transformation_two | transformation_three```

Errors are propagated, lifted, and logged without interrupting execution, ensuring the pipeline continues processing while capturing all failures. Each data point maintains a complete lineage, tracking every transformation alongside any errors encountered.

Data remains immutable—new columns are always created from transformations, preserving original values and ensuring full traceability.

Designed for data engineers, analysts, and functional programmers, this pipeline structure offers a clean, reproducible, and error-traceable approach to data handling.


# Functional Programming Playbook for Python Pipelines

## **Step 1: Writing a Function**

### **1.1 Naming**
- Use descriptive `verb_noun` names (e.g., `filter_positive_growth`).
- Avoid unclear abbreviations (e.g., use `df` for DataFrame, but avoid non-standard abbreviations).
- Ensure function names communicate intent clearly.

---

### **1.2 Parameters & Return Values**
- Add explicit type annotations for all inputs and outputs (e.g., `Callable`, `pd.DataFrame`).
- Return structured, reusable, and composition-friendly outputs (e.g., `NamedTuple`, `dict`, `DataFrame`).
- Prefer immutable data types for inputs (`tuple`, `frozenset`) when feasible.
- Ensure outputs are directly usable as inputs for subsequent functions.

---

### **1.3 Function Purpose**
- Write small, single-purpose functions focused on specific tasks.
- Extract atomic, reusable operations (e.g., row- or column-level transformations).
- Keep functions pure; avoid side effects unless explicitly documented.
- Design outputs to work seamlessly with other functions.

---

### **1.4 Immutability**
- Treat inputs as immutable; never modify them in place.
- Use `.copy()` for DataFrames to avoid modifying the original object.
- Always return new objects for transformations.
- Avoid mutating global state.

---

### **1.5 Validation**
- Validate inputs explicitly at the start of the function.
- Fail fast: raise exceptions for invalid inputs to prevent downstream errors.
- Check for:
  - Required fields or keys.
  - Correct data types.
  - Non-empty or non-NaN values where applicable.
- Avoid silently ignoring invalid inputs.

---

### **1.6 Error Handling**
- Use specific exceptions for clarity:
  - `KeyError`: Missing keys or columns.
  - `ValueError`: Invalid or unexpected values.
  - `TypeError`: Incorrect data types.
  - `ZeroDivisionError`: Division by zero.
- Propagate or lift errors into structured formats (e.g., return exceptions alongside results).
- Avoid catching generic `Exception` unless re-raising.
- Log errors with structured details (e.g., function name, row ID, exception).

---

### **1.7 Traceability**
- Assign unique IDs (e.g., `UUIDs`) to rows for tracking.
- Track lineage across transformations, retaining original and derived relationships.
- For shape-changing operations:
  - Use `source_row_ids` for aggregations.
  - Use `parent_row_id` for exploded rows.
  - Retain hierarchical indices to facilitate debugging and backtracking.

---

### **1.8 Testing Considerations**
- Write unit tests for atomic functions and pipeline steps.
- Test common edge cases:
  - Empty inputs (e.g., empty DataFrame, lists).
  - Missing or null data (`None`, NaN).
  - Invalid data types (e.g., strings for numeric fields).
  - Extreme values (e.g., `1e10`, `-1e10`, zero).
- Validate that error handling and fallback mechanisms behave as expected.

---

## **Step 2: Reviewing a Function (Examples Only)**

### **2.1 Review Checklist**

#### **Naming**
- `filter_positive_growth`, not `fpg`.

#### **Type Annotations**
- `def fn(df: pd.DataFrame) -> pd.DataFrame`, not `def fn(df)`.

#### **Single Responsibility**
- `def calculate_mean(col):`, not `def calculate_and_filter(col, threshold)`.

#### **Immutability**
- `df = df.copy()`, not `df["col"] = ...`.

#### **Validation**
- `if "col" not in df.columns: raise KeyError`, not `df["col"].mean()`.

#### **Error Handling**
- `try: fn(row) except KeyError: log_error(row_id)`, not `except Exception`.

#### **Traceability**
- `df["row_id"] = uuid.uuid4()`, not overwriting indices.

#### **Testing**
- `assert atomic_fn([1, 2, 3]) == 2`, not skipping edge cases like `[]`.

---

## **Step 3: Efficiency and Logging**

### **3.1 Efficiency**
- Use efficient structures: `defaultdict`, `itertools`.
- Stream data: `for chunk in pd.read_csv(file, chunksize=1000): yield chunk`.
- Avoid unnecessary computations or redundant transformations.

---

### **3.2 Logging**
- Centralize structured logs:
  - Transformation names: `calculate_growth`.
  - Row counts: `df.shape[0]`.
  - Errors: `log_error(fn_name, row_id, error)`.
- Use decorators to toggle logging features.

---

## **Step 4: Pipeline Design**

### **4.1 Pipeline Structure**
- Keep steps stateless: `def filter_growth(df): return df[df.growth > 0]`.
- Delegate state (e.g., lineage, logs) to centralized tools like `MetadataManager`.
- Use declarative specifications:
  - `[calculate_growth_rate, filter_positive_growth, aggregate_with_details]`.

---

## **Final Notes**
- Use **Step 1** to write functions and **Step 2** to review them.
- Incorporate traceability, immutability, and error handling in every step.
- Test all edge cases to ensure reliability.
