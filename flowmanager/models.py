import os
import json
import datetime
from hashlib import md5

from contextlib import contextmanager

import logging
from sqlalchemy import DateTime, types
from sqlalchemy import inspect, desc
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import Column, Unicode, String, Integer, create_engine
from sqlalchemy.orm import sessionmaker

# ## SQL DB
from flowmanager.schedules import calculate_new_schedule

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

STATE_SUCCESS = 'success'
STATE_FAILED = 'failed'
STATE_PENDING = 'pending'
STATE_RUNNING = 'running'


class Dataset(Base):
    __tablename__ = 'dataset'
    identifier = Column(String, primary_key=True)
    owner = Column(String)
    spec = Column(JsonType)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    scheduled_for = Column(DateTime, index=True)


class DatasetRevision(Base):
    __tablename__ = 'dataset_revision'
    revision_id = Column(String, primary_key=True)
    dataset_id = Column(String)
    revision = Column(Integer)
    status = Column(String(16))
    errors = Column(JsonType)
    stats = Column(JsonType)
    logs = Column(JsonType)
    pipelines = Column(JsonType)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


class Pipelines(Base):
    __tablename__ = 'pipelines'
    pipeline_id = Column(String(256), primary_key=True)
    flow_id = Column(String(256))
    title = Column(String(256))
    pipeline_details = Column(JsonType)
    status = Column(String(16))
    errors = Column(JsonType)
    stats = Column(JsonType)
    logs = Column(JsonType)
    created_at = Column(DateTime)
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
    def format_identifier(*args):
        return '/'.join(str(arg) for arg in args)

    # Datasets
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
            document['created_at'] = updated_at
            self.save_dataset(document)
        else:
            self.update_dataset(identifier, document)

    def update_dataset_schedule(self, identifier, period_in_seconds, now):
        dataset = self.get_dataset(identifier)
        update = dict(
            scheduled_for=calculate_new_schedule(dataset['scheduled_for'], period_in_seconds, now)
        )
        self.update_dataset(identifier, update)

    def get_expired_datasets(self, now):
        with self.session_scope() as session:
            all = session.query(Dataset).filter(Dataset.scheduled_for <= now).all()
            session.expunge_all()
            yield from all

    # Revisions
    def save_dataset_revision(self, dataset_revision):
        with self.session_scope() as session:
            dataset_revision = DatasetRevision(**dataset_revision)
            session.add(dataset_revision)

    def get_revision(self, dataset, revision_id='latest'):
        with self.session_scope() as session:
            if revision_id == 'latest':
                ret = session.query(DatasetRevision).filter_by(dataset_id=dataset)\
                    .order_by(desc(DatasetRevision.revision)).first()
            elif revision_id == 'successful':
                ret = session.query(DatasetRevision).filter_by(
                    dataset_id=dataset, status=STATE_SUCCESS)\
                    .order_by(desc(DatasetRevision.revision)).first()
            else:
                if not isinstance(revision_id, int):
                    return None
                ret = session.query(DatasetRevision).filter_by(
                    dataset_id=dataset, revision=revision_id).first()
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
        ret = self.get_revision(dataset_id)
        revision = 1 if ret is None else ret['revision'] + 1
        assert status in (STATE_FAILED, STATE_PENDING, STATE_RUNNING, STATE_SUCCESS)
        document = {
            'revision_id': self.format_identifier(dataset_id, revision),
            'dataset_id': dataset_id,
            'revision': revision,
            'created_at': created_at,
            'updated_at': created_at,
            'status': status,
            'errors': errors
        }
        self.save_dataset_revision(document)
        return document

    def update_revision(self, revision_id, doc):
        with self.session_scope() as session:
            ret = session.query(DatasetRevision).filter_by(
                revision_id=revision_id).first()
            if ret is not None:
                for key, value in doc.items():
                    setattr(ret, key, value)
            session.commit()
            return FlowRegistry.object_as_dict(ret)


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
            return ret['flow_id']
        return None

    def list_pipelines_by_id(self, flow_id):
        with self.session_scope() as session:
            all = session.query(Pipelines).filter_by(
                flow_id=flow_id).all()
            session.expunge_all()
            yield from all

    def list_pipelines_by_flow_and_status(self, flow_id, status=STATE_PENDING):
        with self.session_scope() as session:
            all = session.query(Pipelines).filter_by(
                flow_id=flow_id, status=status).all()
            session.expunge_all()
            yield from all

    def list_pipelines(self):
        with self.session_scope() as session:
            all = session.query(Pipelines).all()
            session.expunge_all()
            yield from all

    def check_flow_status(self, flow_id):
        with self.session_scope() as session:
            running = session.query(Pipelines).filter_by(
                flow_id=flow_id, status=STATE_RUNNING).first()
            if running is not None:
                return STATE_RUNNING

            success = session.query(Pipelines).filter_by(
                flow_id=flow_id, status=STATE_SUCCESS).first()
            pending = session.query(Pipelines).filter_by(
                flow_id=flow_id, status=STATE_PENDING).first()
            failed = session.query(Pipelines).filter_by(
                flow_id=flow_id, status=STATE_FAILED).first()

            if pending is not None:
              if (success is not None) or (failed is not None):
                 return STATE_RUNNING
              else:
                 return STATE_PENDING
            else:
              if failed is not None:
                  return STATE_FAILED
            return STATE_SUCCESS

    def update_pipeline(self, identifier, doc):
        with self.session_scope() as session:
            ret = session.query(Pipelines).filter_by(
                pipeline_id=identifier).first()
            if ret is not None:
                for key, value in doc.items():
                    setattr(ret, key, value)
            else:
                logging.warning('Failed to find pipeline %s to update', identifier)
            session.commit()
            return ret is not None

    def create_or_update_pipeline(self, p_id, **args):
        pipeline = self.get_pipeline(p_id)
        if pipeline is None:
            self.save_pipeline(args)
        else:
            self.update_pipeline(p_id, args)


    def delete_pipelines(self, flow_id):
        with self.session_scope() as session:
            session.query(Pipelines).filter_by(
                flow_id=flow_id).delete()
            session.commit()
