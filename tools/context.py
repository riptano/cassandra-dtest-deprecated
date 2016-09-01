"""
Home for functionality that provides context managers, and anything related to
making those context managers function.
"""
import logging
import os
from contextlib import contextmanager

from six import print_

ALLOW_NOISY_LOGGING = os.environ.get('ALLOW_NOISY_LOGGING', '').lower() in ('yes', 'true')


@contextmanager
def silencing_of(log_id, expected_strings=None):
    """
    Context manager which allows silencing logs until exit.
    Log records matching expected_strings will be filtered out of logging.
    If expected_strings is not provided, everything is filtered for that log.
    """
    logger = logging.getLogger(log_id)
    log_filter = _make_filter_class(expected_strings)
    logger.addFilter(log_filter)
    yield
    if log_filter.records_silenced > 0:
            print_("Logs were filtered to remove messages deemed unimportant, total count: {}".format(log_filter.records_silenced))
    logger.removeFilter(log_filter)


def _make_filter_class(expected_strings):
    """
    Builds an anon-ish filtering class and returns it.

    Returns a logfilter if filtering should take place, otherwise a nooplogfilter.

    We're just using a class here as a one-off object with a filter method, for
    use as a filter object on the desired log.
    """
    class nooplogfilter(object):
        records_silenced = 0

        @staticmethod
        def filter(record):
            return True

    class logfilter(object):
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

    if ALLOW_NOISY_LOGGING:
        return nooplogfilter
    else:
        return logfilter
