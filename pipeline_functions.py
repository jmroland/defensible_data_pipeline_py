import pandas as pd
import uuid
import logging
import time
from functools import wraps, partial
from typing import Callable, List, Dict, Any
from collections import defaultdict

logging.basicConfig(level=logging.INFO)

# --- MetadataManager: Centralized Logging, Lineage, and Error Tracking ---
class MetadataManager:
    def __init__(self):
        self.lineage: defaultdict = defaultdict(list)  # Tracks transformation history
        self.errors: List[Dict[str, Any]] = []         # Tracks errors
        self.logs: List[Dict[str, Any]] = []           # General transformation logs

    def initialize(self, df: pd.DataFrame) -> None:
        for row_id in df["row_id"]:
            self.lineage[row_id] = []

    def update_lineage(self, df: pd.DataFrame, transformation: str, new_rows: List[str] = None) -> None:
        for row_id in df["row_id"]:
            self.lineage[row_id].append(transformation)
        if new_rows:
            for row_id in new_rows:
                self.lineage[row_id] = [transformation]

    def log_transformation(self, transformation: str, start_time: float, details: Dict = None) -> None:
        duration = time.time() - start_time
        self.logs.append({
            "transformation": transformation,
            "time": duration,
            "details": details or {}
        })
        logging.info(f"Transformation '{transformation}' completed in {duration:.2f} seconds.")

    def log_error(self, row_id: str, transformation: str, error: Exception) -> None:
        self.errors.append({
            "row_id": row_id,
            "transformation": transformation,
            "error": str(error)
        })
        logging.error(f"Error in transformation '{transformation}' for row '{row_id}': {error}")

    def get_lineage(self, row_id: str) -> List[str]:
        return self.lineage.get(row_id, [])

    def get_errors(self) -> List[Dict[str, Any]]:
        return self.errors

    def get_logs(self) -> List[Dict[str, Any]]:
        return self.logs


# --- Decorators ---
def handle_row_errors(on_error: Callable[[str, str, Exception], None] = None, column_name=None, fallback_value=None):
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(row, *args, **kwargs):
            try:
                return func(row, *args, **kwargs)
            except ValidationError as e:
                error_with_context = {f"{context} -> {key}": msg for key, msg in e.errors.items()}
                raise ValidationError(error_with_context) from e
            except Exception as e:
                if on_error and "row_id" in row:
                    on_error(row["row_id"], f"{func.__name__} (column: {column_name})", e)
                return fallback_value
        return wrapper
    return decorator


def manage_metadata(metadata_manager: MetadataManager):
    """
    A decorator to integrate MetadataManager functionality into transformations.
    Handles logging, lineage updates, and error tracking automatically using the function name.
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(df: pd.DataFrame, *args, **kwargs):
            start_time = time.time()
            transformation_name = func.__name__  # Use the function name as the transformation name

            try:
                # Execute the transformation
                result = func(df, *args, **kwargs)

                # Update lineage for all rows in the result
                metadata_manager.update_lineage(result, transformation_name)

                # Log the transformation success
                metadata_manager.log_transformation(transformation_name, start_time, details={"row_count": len(result)})
                return result

            except Exception as e:
                # Log global transformation errors
                metadata_manager.log_error("global", transformation_name, e)
                raise e  # Re-raise to avoid silently failing
        return wrapper
    return decorator


# --- Pipeline Components ---
# modify to detect and use uuid if unique else add uuid
def add_row_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a unique row ID to each row for traceabilitay.
    """
    df = df.copy()
    df["row_id"] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df

@manage_metadata(metadata_manager)
def aggregate_with_details(df: pd.DataFrame, group_by_cols: list, agg_funcs: dict) -> pd.DataFrame:
    """
    Aggregates the DataFrame by group and includes additional metadata.
    """
    grouped = df.groupby(group_by_cols).agg(agg_funcs).reset_index()
    grouped["row_id"] = [str(uuid.uuid4()) for _ in range(len(grouped))]

    # Add additional metadata
    grouped["source_row_ids"] = df.groupby(group_by_cols)["row_id"].apply(list).reset_index(drop=True)
    grouped["contribution_counts"] = df.groupby(group_by_cols).size().values
    return grouped


@manage_metadata(metadata_manager)
def explode_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    exploded_df = df.explode(column).reset_index(drop=True)
    exploded_df["parent_row_id"] = exploded_df.index.map(lambda idx: df.loc[idx, "row_id"])
    return exploded_df

@manage_metadata(metadata_manager)
def filter_with_details(df: pd.DataFrame, filter_condition: Callable[[pd.DataFrame], pd.Series], transformation_name: str) -> pd.DataFrame:
    """
    Filters the DataFrame based on a condition and logs removed rows.

    Args:
        df (pd.DataFrame): The input DataFrame.
        filter_condition (Callable): A function that returns a Boolean Series indicating rows to keep.
        transformation_name (str): Name of the transformation (used for metadata tracking).
    
    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    original_df = df.copy()
    filtered_df = df[filter_condition(df)].reset_index(drop=True)

    # Log removed rows separately
    removed_df = original_df[~original_df["row_id"].isin(filtered_df["row_id"])]
    if not removed_df.empty:
        removed_df["removal_reason"] = transformation_name
        metadata_manager.log_removed_rows(removed_df, transformation_name)

    return filtered_df

# --- Pipeline Builder ---
def build_pipeline(pipeline_specs: List[Callable], metadata_manager: MetadataManager) -> Callable:
    """
    Builds a pipeline where transformations are applied in sequence.
    MetadataManager integration is handled via decorators.
    """
    def pipeline(df: pd.DataFrame) -> pd.DataFrame:
        df = add_row_id(df)
        metadata_manager.initialize(df)

        for transformation in pipeline_specs:
            df = transformation(df)

        return df

    return pipeline

################################################################################
# Examples
# # --- Transformations ---
# def calculate_growth(row: pd.Series) -> float:
#     """
#     Calculates the growth rate for a single row.
#     """
#     if row["start_value"] > 0:
#         return (row["end_value"] - row["start_value"]) / row["start_value"]
#     raise ValueError("Invalid start_value")
# 
# 
# @manage_metadata(metadata_manager)
# def calculate_growth_rate(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
#     """
#     Applies the calculate_growth function row-wise to compute growth rate.
#     """
#     @handle_row_errors(on_error=metadata_manager.log_error, column_name=column_name, fallback_value=None)
#     def safe_calculate_growth(row):
#         return calculate_growth(row)
# 
#     df[column_name] = df.apply(safe_calculate_growth, axis=1)
#     return df
# 
# 
# @manage_metadata(metadata_manager)
# def filter_positive_growth(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Filters rows with positive growth rate.
# 
#     Example:
#         Input:
#         id  growth_rate
#         1   0.1
#         2  -0.2
# 
#         Output:
#         id  growth_rate
#         1   0.1
#     """
#     return df[df["growth_rate"] > 0]
# 
# 
# # --- Example Usage ---
# if __name__ == "__main__":
#     # Sample data
#     data = pd.DataFrame({
#         "id": [1, 2, 3],
#         "start_value": [100, 0, 200],
#         "end_value": [110, 20, 240],
#     })
# 
#     # Initialize MetadataManager
#     metadata_manager = MetadataManager()
# 
#     # Define transformations with partials
#     calculate_growth_rate_partial = partial(calculate_growth_rate, column_name="growth_rate")
#     aggregate_with_details_partial = partial(
#         aggregate_with_details,
#         group_by_cols=["id"],
#         agg_funcs={
#             "start_value": "mean",
#             "end_value": "sum",
#             "growth_rate": "mean"
#         }
#     )
# 
#     # Pipeline specs
#     pipeline_specs = [
#         calculate_growth_rate_partial,
#         filter_positive_growth,
#         aggregate_with_details_partial,
#     ]
# 
#     # Build and execute pipeline
#     pipeline = build_pipeline(pipeline_specs, metadata_manager)
#     result = pipeline(data)
# 
#     # Output results
#     print(result)
#     print("\nLineage Metadata:")
#     print(metadata_manager.lineage)
#     print("\nErrors:")
#     print(metadata_manager.get_errors())
#     print("\nLogs:")
#     print(metadata_manager.get_logs())
