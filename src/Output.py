import json
import abc


class Output(metaclass=abc.ABCMeta):

    @classmethod
    def load_data(cls, data):
        pass
