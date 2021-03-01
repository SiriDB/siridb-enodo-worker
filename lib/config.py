import os
from configparser import RawConfigParser, _UNSET, SectionProxy

class EnodoConfigParser(RawConfigParser):

    def __getitem__(self, key):
        if key != self.default_section and not self.has_section(key):
            return SectionProxy(self, key)
        return self._proxies[key]

    def has_option(self, section, option):
        return True

    def get(self, section, option, *, raw=False, vars=None, fallback=_UNSET):
        """Edited default get func from RawConfigParser
        """
        env_value = os.getenv(option.upper())
        if env_value is not None:
            return env_value

        try:
            return super(EnodoConfigParser, self).get(
                section, option, raw=False, vars=None, fallback=_UNSET)
        except Exception as _:
            raise Exception(f'Invalid config, missing option "{option}" in section "{section}" or environment variable "{option.upper()}"')