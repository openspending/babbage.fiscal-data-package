import json

from sqlalchemy.ext.declarative import declarative_base

from babbage import CubeManager, BabbageException
from sqlalchemy import Column, Unicode, String
from sqlalchemy.orm import sessionmaker

from .db_utils import model_name

Base = declarative_base()


class Model(Base):
    __tablename__ = 'models'
    _id = Column(Unicode, primary_key=True)
    model = Column(Unicode)
    origin_url = Column(String)


class SQLCubeManager(CubeManager):

    def __init__(self,engine):
        super(SQLCubeManager, self).__init__(engine)
        Base.metadata.create_all(engine)
        self._session = sessionmaker(bind=engine)()

    def table_name_for_package(self, datapackage_name):
        return model_name(datapackage_name)

    def save_model(self, datapackage_name, datapackage_url, model):
        rec = self._session.query(Model).filter(Model._id==datapackage_name).first()
        if rec is None:
            rec = Model(_id=datapackage_name)
            self._session.add(rec)
        rec.model=json.dumps(model)
        rec.origin_url=datapackage_url
        self._session.commit()

    def list_cubes(self):
        """ List all available models in the DB """
        for instance in self._session.query(Model._id).order_by(Model._id):
            yield instance[0]

    def has_cube(self, name):
        """ Check if a cube exists. """
        return self._session.query(Model).filter(Model._id==name).first() is not None

    def get_cube_model(self, name):
        if not self.has_cube(name):
            raise BabbageException('No such cube: %r' % name)
        return json.loads(self._session.query(Model).filter(Model._id==name).first().model)