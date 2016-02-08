import json

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import Column, Unicode, String
from sqlalchemy.orm import sessionmaker

from .db_utils import model_name

Base = declarative_base()


class Model(Base):
    __tablename__ = 'models'
    id = Column(Unicode, primary_key=True)
    model = Column(Unicode)
    package = Column(Unicode)
    origin_url = Column(String)


class ModelRegistry(object):

    def __init__(self,engine):
        self.engine = engine
        Base.metadata.create_all(engine)
        self._session = sessionmaker(bind=engine)()

    @staticmethod
    def table_name_for_package(datapackage_name):
        return model_name(datapackage_name)

    def save_model(self, name, datapackage_url, datapackage, model):
        """
        Save a model in the registry
        :param name: name for the model
        :param datapackage_url: origin URL for the datapackage which is the source for this model
        :param datapackage: datapackage object from which this model was derived
        :param model: model to save
        """
        rec = self._session.query(Model).filter(Model.id == name).first()
        if rec is None:
            rec = Model(id=name)
            self._session.add(rec)
        rec.model = json.dumps(model)
        rec.package = json.dumps(datapackage.metadata)
        rec.origin_url = datapackage_url
        self._session.commit()

    def list_models(self):
        """
        List all available models in the DB
        :return: A generator yielding strings (one per model)
        """
        for instance in self._session.query(Model.id).order_by(Model.id):
            yield instance[0]

    def has_model(self, name):
        """
        Check if a model exists in the registry
        :param name: model name to test
        :return: True if yes
        """
        return self._session.query(Model).filter(Model.id == name).first() is not None

    def get_model(self, name):
        """
        Return the model associated with a specific name.
        Raises KeyError in case the model doesn't exist.
        :param name: model name to fetch
        :return: Python object representing the model
        """
        if not self.has_model(name):
            raise KeyError(name)
        return json.loads(self._session.query(Model).filter(Model.id == name).first().model)

    def get_package(self, name):
        """
        Return the original package contents associated with a specific name.
        Raises KeyError in case the model doesn't exist.
        :param name: model name to fetch
        :return: Python object representing the package
        """
        if not self.has_model(name):
            raise KeyError(name)
        model = self._session.query(Model).filter(Model.id == name).first()
        ret = json.loads(model.package)
        ret['__origin_url'] = model.origin_url
        return ret
