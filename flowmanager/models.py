import os
import json
import datetime
from hashlib import md5

from contextlib import contextmanager

from sqlalchemy import DateTime, types
from sqlalchemy import inspect, desc
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import Column, Unicode, String, Integer, create_engine
from sqlalchemy.orm import sessionmaker

# ## SQL DB
Base = declarative_base()

# ## Json as string Type
class JsonType(types.TypeDecorator):
    impl = types.Unicode

    def process_bind_param(self, value, dialect):
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        if value:
            return json.loads(value)
        else:
            return None

    def copy(self, **kw):
        return JsonType(self.impl.length)


class Dataset(Base):
    __tablename__ = 'dataset'
    identifier = Column(String(128), primary_key=True)
    owner = Column(String(128))
    spec = Column(JsonType)
    updated_at = Column(DateTime)


class DatasetRevision(Base):
    __tablename__ = 'dataset-revision'
    revision_id = Column(String(128), primary_key=True)
    dataset_id = Column(String(128))
    revision = Column(Integer)
    created_at = Column(DateTime)
    status = Column(String(16))
    errors = Column(JsonType)


class Pipelines(Base):
    __tablename__ = 'pipelines'
    pipeline_id = Column(String(128), primary_key=True)
    flow_id = Column(String(128))
    pipeline_details = Column(JsonType)
    status = Column(String(16))
    errors = Column(JsonType)
    updated_at = Column(DateTime)


class FlowRegistry:

    def __init__(self, db_connection_string):
        self._db_connection_string = db_connection_string
        self._engine = None
        self._session = None


    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine(self._db_connection_string)
            Base.metadata.create_all(self._engine)
        return self._engine


    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        if self._session is None:
            self._session = sessionmaker(bind=self.engine)
        session = self._session()
        try:
            yield session
            session.commit()
        except: #noqa
            session.rollback()
            raise
        finally:
            session.expunge_all()
            session.close()

    @staticmethod
    def object_as_dict(obj):
        return {c.key: getattr(obj, c.key)
                for c in inspect(obj).mapper.column_attrs}

    @staticmethod
    def format_identifier(parent, child, *args):
        if len(args):
            return '{}/{}/'.format(parent, child) + '/'.join(str(arg) for arg in args)
        return '{}/{}'.format(parent, child)

    # datasets
    def save_dataset(self, dataset):
        with self.session_scope() as session:
            dataset = Dataset(**dataset)
            session.add(dataset)

    def get_dataset(self, identifier):
        with self.session_scope() as session:
            ret = session.query(Dataset).filter_by(identifier=identifier).first()
            if ret is not None:
                return FlowRegistry.object_as_dict(ret)
        return None

    def list_datasets(self):
        with self.session_scope() as session:
            all = session.query(Dataset).all()
            session.expunge_all()
            yield from all


    def update_dataset(self, identifier, doc):
        with self.session_scope() as session:
            ret = session.query(Dataset).filter_by(identifier=identifier).first()
            if ret is not None:
                for key, value in doc.items():
                    setattr(ret, key, value)
            session.commit()

    def create_or_update_dataset(self, identifier, owner, spec, updated_at):
        dataset = self.get_dataset(identifier)
        document = {
            'identifier': identifier,
            'owner': owner,
            'spec': spec,
            'updated_at': updated_at
        }
        if dataset is None:
            self.save_dataset(document)
        else:
            self.update_dataset(identifier, document)


    # Revisions
    def save_dataset_revision(self, dataset_revision):
        with self.session_scope() as session:
            dataset_revision = DatasetRevision(**dataset_revision)
            session.add(dataset_revision)

    def get_revision_by_dataset_id(self, dataset):
        with self.session_scope() as session:
            ret = session.query(DatasetRevision).filter_by(dataset_id=dataset)\
                .order_by(desc(DatasetRevision.revision)).first()
            if ret is not None:
                return FlowRegistry.object_as_dict(ret)
        return None

    def get_revision_by_revision_id(self, revision_id):
        with self.session_scope() as session:
            ret = session.query(DatasetRevision).filter_by(
                revision_id=revision_id).first()
            if ret is not None:
                return FlowRegistry.object_as_dict(ret)
        return None

    def create_revision(self, dataset_id, created_at, status, errors):
        ret = self.get_revision_by_dataset_id(dataset_id)
        revision = 1 if ret is None else ret['revision'] + 1
        document = {
            'revision_id': self.format_identifier(dataset_id, revision),
            'dataset_id': dataset_id,
            'revision': revision,
            'created_at': created_at,
            'status': status,
            'errors': errors
        }
        self.save_dataset_revision(document)
        return document

    def update_revision(self, revision_id, doc):
        ret = self.get_revision_by_revision_id(revision_id)
        with self.session_scope() as session:
            ret = session.query(DatasetRevision).filter_by(
                revision_id=revision_id).first()
            if ret is not None:
                for key, value in doc.items():
                    setattr(ret, key, value)
            session.commit()

    # Pipelines
    def save_pipeline(self, pipelines):
        with self.session_scope() as session:
            pipelines = Pipelines(**pipelines)
            session.add(pipelines)

    def get_pipeline(self, p_identifier):
        with self.session_scope() as session:
            ret = session.query(Pipelines).filter_by(
                pipeline_id=p_identifier).first()
            if ret is not None:
                return FlowRegistry.object_as_dict(ret)
        return None

    def get_flow_id(self, id):
        ret = self.get_pipeline(id)
        if ret is not None:
            return  ret['flow_id']
        return None

    def list_pipelines_by_id(self, flow_id):
        with self.session_scope() as session:
            all = session.query(Pipelines).filter_by(
                flow_id=flow_id).all()
            session.expunge_all()
            yield from all

    def list_pipelines(self):
        with self.session_scope() as session:
            all = session.query(Pipelines).all()
            session.expunge_all()
            for pipeline in all:
                yield pipeline.pipeline_details

    def check_flow_status(self, flow_id):
        with self.session_scope() as session:
            ret = session.query(Pipelines).filter_by(
                flow_id=flow_id, status='pending').first()
            if ret is not None:
                return 'pending'
            ret = session.query(Pipelines).filter_by(
                flow_id=flow_id, status='failed').first()
            if ret is not None:
                return 'failed'
            return 'success'

    def update_pipeline(self, identifier, doc):
        with self.session_scope() as session:
            ret = session.query(Pipelines).filter_by(
                pipeline_id=identifier).first()
            if ret is not None:
                for key, value in doc.items():
                    setattr(ret, key, value)
            session.commit()

    def create_or_update_pipeline(self, p_id, **args):
        pipeline = self.get_pipeline(p_id)
        if pipeline is None:
            self.save_pipeline(args)
        else:
            self.update_pipeline(p_id, args)


    def delete_pipelines(self, flow_id):
        with self.session_scope() as session:
            ret = session.query(Pipelines).filter_by(
                flow_id=flow_id).delete()
            session.commit()
