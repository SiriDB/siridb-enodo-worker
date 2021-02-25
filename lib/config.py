import os
from configparser import RawConfigParser, _UNSET

class EnodoConfigParser(RawConfigParser):

    def get(self, section, option, *, raw=False, vars=None, fallback=_UNSET):
        """Edited default get func from RawConfigParser
        """
        env_value = os.getenv(option.upper())
        if env_value is not None:
            return env_value

        return super(EnodoConfigParser, self).get(
            section, option, raw=False, vars=None, fallback=_UNSET)