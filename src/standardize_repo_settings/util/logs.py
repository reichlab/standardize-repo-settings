import sys

import structlog


def add_custom_info(logger, method_name, event_dict):
    # placeholder for custom log info
    return event_dict


def setup_logging():
    shared_processors = [
        add_custom_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.add_log_level,
    ]

    if sys.stderr.isatty():
        # If we're in a terminal, pretty print the logs.
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(),
        ]
    else:
        # Otherwise, output logs in JSON format
        processors = shared_processors + [
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ]

    structlog.configure(
        processors=processors,
        cache_logger_on_first_use=True,
    )
