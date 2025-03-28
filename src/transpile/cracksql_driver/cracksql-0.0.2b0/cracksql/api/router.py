#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Router Module - Simplified Version
This is a simplified version of router that doesn't register any routes
"""

try:
    from flask import Blueprint
except ImportError:
    # If Flask cannot be imported, create a dummy Blueprint class
    class Blueprint:
        def __init__(self, name, import_name, **kwargs):
            self.name = name
            self.import_name = import_name
        
        def route(self, rule, **options):
            def decorator(f):
                return f
            return decorator

# Create blueprint but don't register any routes
bp = Blueprint('dummy', __name__)

# Empty function that does nothing
def register_routes():
    """This function doesn't register any routes"""
    pass
