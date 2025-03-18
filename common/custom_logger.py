import logging


# Function to add batch_id in log messages
class BatchLoggerAdapter(logging.LoggerAdapter):
    # Custom LoggerAdapter that injects batch_id into log messages
    def __init__(self, logger, batch_id="N/A"):
        super().__init__(logger, {"batch_id": batch_id})

    def process(self, msg, kwargs):
        return f"[Batch ID: {self.extra.get('batch_id', 'N/A')}] {msg}", kwargs


class SafeFormatter(logging.Formatter):
    # Custom Formatter that ensures batch_id is always present in logs
    def format(self, record):
        if isinstance(self._style, logging.PercentStyle):  # Standard formatter
            if "batch_id" not in record.__dict__:  # If batch_id is missing
                record.__dict__["batch_id"] = "N/A"
        return super().format(record)


# Global logger instance
global_logger = None


def logger_init(batch_id=None):
    # Initializes the logger globally and ensures all modules reuse it
    global global_logger

    if global_logger is None:
        logger = logging.getLogger("dq_framework_logger")
        logger.setLevel(logging.INFO)

        if not logger.hasHandlers():
            log_format = "[%(asctime)s] [%(levelname)s] %(message)s"
            formatter = SafeFormatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        global_logger = BatchLoggerAdapter(logger, batch_id or "N/A")
    return global_logger


def get_logger():
    # Returns the initialized logger. If not initialized, raises an error.
    if global_logger is None:
        raise RuntimeError(
            "Logger has not been initialized.Call logger_init(batch_id) first."
        )
    return global_logger
