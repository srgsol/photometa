# -*- coding: utf8 -*-
"""
Logging configuration
"""
import logging


def logger_factory(logger_name, level=logging.DEBUG, propagate=False):

    logger = logging.getLogger(logger_name)

    formatter = logging.Formatter(
        fmt='[%(levelname)s] %(asctime)s - %(filename)s (%(lineno)d): %(message)s ',
        datefmt='%m/%d/%Y %H:%M:%S')

    file_handler = logging.FileHandler('log/' + logger_name + '.log')
    file_handler.setFormatter(formatter)

    logger.setLevel(level)
    logger.addHandler(file_handler)
    logger.propagate = propagate