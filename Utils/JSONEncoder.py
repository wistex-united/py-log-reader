# from _ctypes import PyObj_FromPtr  # type: ignore
from abc import ABCMeta
from collections import deque
from enum import EnumMeta
import json
import re

import numpy as np

class NoIndent(object):
    """Value wrapper."""

    def __init__(self, value):
        self.value = value


class NumpyEncoder(json.JSONEncoder):
    """Special json encoder for numpy types"""

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        else:
            return str(obj)


class SpecialEncoder(json.JSONEncoder):
    FORMAT_SPEC = "@@{}@@"
    regex = re.compile(FORMAT_SPEC.format(r"(\d+)"))

    def __init__(self, **kwargs):
        # Save copy of any keyword argument values needed for use here.
        self.__sort_keys = kwargs.get("sort_keys", None)
        super(SpecialEncoder, self).__init__(**kwargs)
        self.cache = {}

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            self.cache[id(obj)] = obj.tolist()
            return self.FORMAT_SPEC.format(id(obj))
        elif isinstance(obj, NoIndent):
            self.cache[id(obj)] = obj.value
            return self.FORMAT_SPEC.format(id(obj))
        elif isinstance(obj, ABCMeta):
            return f"Class Type: {obj.__name__}"
        elif isinstance(obj, EnumMeta):
            return f"Enum Type: {obj.__name__}"
        else:
            return str(obj)

    def encode(self, obj):
        format_spec = self.FORMAT_SPEC  # Local var to expedite access.
        json_repr = super(SpecialEncoder, self).encode(obj)  # Default JSON.

        # Replace any marked-up object ids in the JSON repr with the
        # value returned from the json.dumps() of the corresponding
        # wrapped Python object.
        for match in self.regex.finditer(json_repr):
            # see https://stackoverflow.com/a/15012814/355230
            id = int(match.group(1))
            no_indent = self.cache[id]
            self.cache[id] = None  # Remove the reference from cache
            json_obj_repr = json.dumps(
                no_indent, sort_keys=self.__sort_keys, cls=NumpyEncoder
            )

            # Replace the matched id string with json formatted representation
            # of the corresponding Python object.
            json_repr = json_repr.replace(
                '"{}"'.format(format_spec.format(id)), json_obj_repr
            )

        return json_repr


if __name__ == "__main__":
    from string import ascii_lowercase as letters

    data_structure = {
        "layer1": {
            "layer2": {
                "layer3_1": NoIndent(
                    [
                        {"x": 1, "y": 7},
                        {"x": 0, "y": 4},
                        {"x": 5, "y": 3},
                        {"x": 6, "y": 9},
                        {k: v for v, k in enumerate(letters)},
                    ]
                ),
                "layer3_2": "string",
                "layer3_3": NoIndent(
                    [
                        {"x": 2, "y": 8, "z": 3},
                        {"x": 1, "y": 5, "z": 4},
                        {"x": 6, "y": 9, "z": 8},
                    ]
                ),
                "layer3_4": NoIndent(list(range(20))),
            }
        }
    }

    print(json.dumps(data_structure, cls=SpecialEncoder, sort_keys=True, indent=2))
