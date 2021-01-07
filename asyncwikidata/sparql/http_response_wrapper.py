from http.client import HTTPResponse

class HTTPResponseWrapper():
    '''Wrapper around HTTPResponse which allows us to read response multiple times.'''
    def __init__(self, response: HTTPResponse):
        self.response = response
        self.__data = None

    def read(self):
        if self.__data is None:
            self.__data = self.response.read()
        return self.__data

    def __getattr__(self, name: str):
        value = self.__dict__.get(name)
        if not value:
            return getattr(self.response, name)
        return value
