"""
Home for functionality that provides context managers, and anything related to
making those context managers function.
"""
import logging

from six import print_


class silencing_of(object):
    """
    Context manager which allows silencing logs until exit.

    Log records matching expected_strings will be filtered out of logging.

    If expected_strings is not provided, everything is filtered for that log.
    """
    def __init__(self, log_id, expected_strings=None):
        self.logger = logging.getLogger(log_id)
        self._filter = self._get_filter_obj(expected_strings)

    def __enter__(self):
        self.logger.addFilter(self._filter)

    def __exit__(self, exc_type, exc_value, traceback):
        if self._filter.records_silenced > 0:
            print_("Logs were filtered to remove messages deemed unimportant, total count: {}".format(self._filter.records_silenced))

        self.logger.removeFilter(self._filter)

    def _get_filter_obj(self, expected_strings):
        """
        Builds an anon-ish filtering class and returns an instance of it.
        """
        class logfilter(object):
            """
            We're just using a class here as a one-off object with a filter method, for
            use as a filter object on the desired log.
            """
            records_silenced = 0

            @classmethod
            def increment_filtered(cls):
                cls.records_silenced += 1

            @staticmethod
            def filter(record):
                if expected_strings is None:
                    logfilter.increment_filtered()
                    return False

                for s in expected_strings:
                    if s in record.msg or s in record.name:
                        logfilter.increment_filtered()
                        return False

                return True

        return logfilter()
