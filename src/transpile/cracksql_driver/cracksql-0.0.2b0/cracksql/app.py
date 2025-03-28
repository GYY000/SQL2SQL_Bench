#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
CrackSQL Package Entry Point
This file serves as the entry point for the CrackSQL package, allowing users to directly import all classes and methods
"""

__version__ = "0.1.0"

# Import warnings
import cracksql.warnings as warnings

# Try to import all main modules, but don't require them to exist
try:
    from . import config
except ImportError as e:
    warnings.warn(f"Failed to import config module: {str(e)}")

try:
    from . import models
except ImportError as e:
    warnings.warn(f"Failed to import models module: {str(e)}")

try:
    from . import api
except ImportError as e:
    warnings.warn(f"Failed to import api module: {str(e)}")

try:
    from . import llm_model
except ImportError as e:
    warnings.warn(f"Failed to import llm_model module: {str(e)}")

try:
    from . import vector_store
except ImportError as e:
    warnings.warn(f"Failed to import vector_store module: {str(e)}")

try:
    from . import utils
except ImportError as e:
    warnings.warn(f"Failed to import utils module: {str(e)}")

try:
    from . import retriever
except ImportError as e:
    warnings.warn(f"Failed to import retriever module: {str(e)}")

try:
    from . import task
except ImportError as e:
    warnings.warn(f"Failed to import task module: {str(e)}")

try:
    from . import translator
except ImportError as e:
    warnings.warn(f"Failed to import translator module: {str(e)}")

try:
    from . import preprocessor
except ImportError as e:
    warnings.warn(f"Failed to import preprocessor module: {str(e)}")

try:
    from . import doc_process
except ImportError as e:
    warnings.warn(f"Failed to import doc_process module: {str(e)}")

# Allow users to directly import everything from the package
