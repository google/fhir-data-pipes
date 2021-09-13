"""Logging class."""

import logging


class LoggingMixin:

  @property
  def logger(self):
    name = self.__class__.__module__ + '.' + self.__class__.__name__
    return logging.getLogger(name)
