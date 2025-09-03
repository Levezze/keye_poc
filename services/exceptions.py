"""
Custom exceptions for service layer operations.
"""


class DatasetNotFoundError(Exception):
    """Raised when a requested dataset does not exist."""

    def __init__(self, dataset_id: str, message: str = None):
        self.dataset_id = dataset_id
        if message is None:
            message = f"Dataset {dataset_id} not found"
        super().__init__(message)


class DatasetOperationError(Exception):
    """Raised when an operation on a dataset fails."""

    def __init__(self, dataset_id: str, operation: str, message: str = None):
        self.dataset_id = dataset_id
        self.operation = operation
        if message is None:
            message = f"Failed to {operation} dataset {dataset_id}"
        super().__init__(message)


class SchemaNotFoundError(Exception):
    """Raised when a schema for a dataset is not found."""

    def __init__(self, dataset_id: str, message: str = None):
        self.dataset_id = dataset_id
        if message is None:
            message = f"Schema not found for dataset {dataset_id}"
        super().__init__(message)
