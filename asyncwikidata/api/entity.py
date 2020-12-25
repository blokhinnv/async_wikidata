from collections import defaultdict

from asyncwikidata.api.datatypes import Monolingual, SiteLink
from asyncwikidata.api.claim import Claim


class Entity(object):
    """Object representing Wikidata entity (item or property)"""
    def __init__(self, entity_dict: dict, repr_lang: str = 'en') -> None:
        """
        Args:
            entity_dict (dict): representation of entity obtained via linked data interface
            repr_lang (str, optional): languages of labels, descriptions and aliases which will be
                                       printed when the __repr__ method is called. Defaults to 'en'.
        """
        self.entity_dict = entity_dict
        self.repr_lang = repr_lang

        self.id = entity_dict['id']
        self.labels = {lang: Monolingual.from_values(**label)
                         for lang, label in entity_dict['labels'].items()}
        self.descriptions = {lang: Monolingual.from_values(**d)
                         for lang, d in entity_dict['descriptions'].items()}
        self.aliases = {lang: [Monolingual.from_values(**alias) for alias in aliases]
                         for lang, aliases in entity_dict['aliases'].items()}
        self.claims = defaultdict(list)
        for pid, claims_list in entity_dict['claims'].items():
            for claim_dict in claims_list:
                claim = Claim(claim_dict['mainsnak'], claim_dict.get('qualifiers', None))
                self.claims[pid].append(claim)

        self.sitelinks = {site: SiteLink(**sl_dict) for site, sl_dict in entity_dict['sitelinks'].items()
                           if site.endswith('wiki')}

    def __repr__(self) -> str:
        return '{}(id={}, label={}, description={}, aliases={})'.format(self.__class__.__name__,
                                                                        self.id,
                                                                        self.labels.get(self.repr_lang, None),
                                                                        self.descriptions.get(self.repr_lang, None),
                                                                        self.aliases.get(self.repr_lang, None))
