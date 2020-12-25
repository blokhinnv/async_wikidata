from __future__ import annotations
from typing import Optional

from asyncwikidata.api.datatypes import DataType, UnknownValue, NoValue


class Claim(object):
    '''Class to represent claim
    For reference see https://www.wikidata.org/wiki/Wikidata:Glossary#:~:text=Claim%20is%20a%20piece%20of,%22%20and%20%22unknown%20value%22.
    '''
    @staticmethod
    def get_data_obj(datatype: str, datavalue: Optional[dict] = None, **kwargs):
        '''
            datatype - тип элемента (string, quantity, ...)
            dv - значение элемента
            file_type - для какого типа файла преобразуется сущность
            Преобразует концевую сущность/литерал в текст
        '''
        item = None

        # example of no value https://www.wikidata.org/wiki/Q46222
        # example of unknown value https://www.wikidata.org/wiki/Q42

        if kwargs.get('snaktype', None) == 'novalue':
            return NoValue
        elif kwargs.get('snaktype', None) == 'somevalue' or datavalue is None:
            return UnknownValue

        if datatype in DataType.subclasses:
            item = DataType.subclasses[datatype](datavalue=datavalue)
        return item

    def __init__(self, mainsnak: dict, qualsdict: Optional[dict] = None) -> None:
        """
        Args:
            mainsnak (dict): dictionary with the value of the claim
            qualsdict (dict, optional): dictionary with the qualifiers of the claim. Defaults to None.
        """
        self.object = self.get_data_obj(**mainsnak)
        if qualsdict:
            self.qualifiers = {pid: [self.get_data_obj(**qualsnak) for qualsnak in qualsnaks]
                                for pid, qualsnaks in qualsdict.items()}
