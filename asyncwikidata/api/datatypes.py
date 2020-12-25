from __future__ import annotations
from datetime import datetime
from typing import Optional
import re


class DataType(object):
    """Base class for Wikidata data types.

    For reference see https://www.wikidata.org/wiki/Help:Data_type
    `LD_NAME` class atribute is used to match the class with datatype from JSON representation of data.
    Each class contains set of attributes specific for the data type.
    """
    subclasses = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if hasattr(cls, 'LD_NAME'):
            cls.subclasses[cls.LD_NAME] = cls

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__,
                               ', '.join(f'{k}={v}' for k, v in self.__dict__.items()))

class Globe(DataType):
    LD_NAME = 'globe-coordinate'
    def __init__(self, datavalue: dict):
        self.latitude = datavalue['value'].get('latitude', None)
        self.longitude = datavalue['value'].get('longitude', None)
        self.altitude = datavalue['value'].get('altitude', None)
        self.precision = datavalue['value'].get('precision', None)
        self.globe = datavalue['value'].get('globe', None)


class Quantity(DataType):
    LD_NAME = 'quantity'
    def __init__(self, datavalue: dict):
        self.amount = datavalue['value'].get('amount', None)
        self.unit = datavalue['value'].get('unit', None)
        self.upperBound = datavalue['value'].get('upperBound', None)
        self.lowerBound = datavalue['value'].get('lowerBound', None)


class Time(DataType):
    LD_NAME = 'time'
    pattern = re.compile(r'(?P<era>[+-])(?P<year>\d{4,})-(?P<month>\d{2})-(?P<day>\d{2})T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})Z')

    def parse_time(self, time: str) -> None:
        '''Method for parsing dates in the form they are stored in JSON representation of the data
        '''
        match = self.pattern.match(time)
        if match:
            for group_name, value in match.groupdict().items():
                setattr(self, group_name, value)

    def __init__(self, datavalue: dict):
        self.timezone = datavalue['value'].get('timezone', None)
        self.before = datavalue['value'].get('before', None)
        self.after = datavalue['value'].get('after', None)
        self.precision = int(datavalue['value']['precision']) if 'precision' in datavalue['value'] else None
        self.calendar = datavalue['value'].get('calendarmodel', None)
        self.time = datavalue['value'].get('time', None)
        self.parse_time(self.time)


class WikiBase(DataType):
    def __init__(self, datavalue: dict):
        self.item = datavalue['value']['numeric-id']


class WikiBaseItem(WikiBase):
    LD_NAME = 'wikibase-item'
    def __init__(self, datavalue: dict):
        super().__init__(datavalue=datavalue)
        self.item = "Q" + str(self.item)


class WikiBaseProperty(WikiBase):
    LD_NAME = 'wikibase-property'
    def __init__(self, datavalue: dict):
        super().__init__(datavalue=datavalue)
        self.item = "P" + str(self.item)


class Text(DataType):
    LD_NAME = 'string'
    def __init__(self, datavalue: dict):
        self.value = datavalue.get('value', None)

    @classmethod
    def from_value(cls, value) -> Text:
        return cls({"value": value})


class Media(DataType):
    LD_NAME = 'commonsMedia'
    def __init__(self, datavalue: dict):
        self.value = datavalue.get('value', None)
        self.url = self.file_url(self.value)

    @staticmethod
    def file_url(val):
        return f'https://commons.wikimedia.org/wiki/Special:FilePath/{val}'


class Monolingual(DataType):
    LD_NAME = 'monolingualtext'
    def __init__(self, datavalue: dict):
        self.value = datavalue['value'].get('text', None)
        self.language = datavalue['value'].get('language', None)

    @classmethod
    def from_values(cls, value: str, language: str):
        return cls({'value': {'text': value, 'language': language}})


class SiteLink(object):
    def __init__(self, title: Optional[str] = None, url: Optional[str] = None, **kwargs) -> None:
        super().__init__()
        self.title = title
        self.url = url


UnknownValue = Text.from_value('Unknown value')
NoValue = Text.from_value('No value')
