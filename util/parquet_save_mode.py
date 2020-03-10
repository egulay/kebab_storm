from enum import Enum


class ParquetSaveMode(Enum):
    overwrite = "overwrite"
    append = "append"
    ignore = "ignore"
    error_if_exist = "errorifexists"

    def __str__(self):
        return '%s' % self.value
