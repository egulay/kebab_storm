import importlib


def class_for_name(module_name, class_name):
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    return cls
