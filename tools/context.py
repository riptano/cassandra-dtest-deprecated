"""
Home for functionality that provides context managers, and anything related to
making those context managers function.
"""
import logging


class silencing_of(object):
    """
    Context manager which allows silencing logs until it exits.

    Log records matching expected_strings will be filtered out of logging.

    If expected_strings is not provided, everything is filtered for that log.
    """
    def __init__(self, log_id, expected_strings=None):
        self.logger = logging.getLogger(log_id)
        self._filter_class = self._build_filter_class(expected_strings)

    def __enter__(self):
        self.logger.addFilter(self._filter_class)

    def __exit__(self, exc_type, exc_value, traceback):
        self.logger.removeFilter(self._filter_class)

    def _build_filter_class(self, expected_strings):
        class logfilter(object):
            """
            We're just using a class here as a one-off object with a filter method, for
            use as a filter object on the desired log.
            """
            @staticmethod
            def filter(record):
                if expected_strings is None:
                    return False

                for s in expected_strings:
                    if s in record.msg or s in record.name:
                        return False
                return True

        return logfilter
