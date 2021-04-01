import logging
from typing import Any, Generic, Iterator, List, TypeVar

from boto3.resources.base import ServiceResource
from boto3.resources.factory import ResourceFactory
from boto3.resources.model import Collection
from boto3.resources.response import ResourceHandler
from boto3.utils import ServiceContext
from botocore.hooks import HierarchicalEmitter

logger: logging.Logger

ResourceCollectionType = TypeVar("ResourceCollectionType")
ServiceResourceType = TypeVar("ServiceResourceType", bound="ServiceResource")

class ResourceCollection(Generic[ServiceResourceType]):
    def __init__(
        self, model: Collection, parent: ServiceResource, handler: ResourceHandler, **kwargs: Any
    ) -> None: ...
    def __repr__(self) -> str: ...
    def __iter__(self) -> Iterator[Any]: ...
    def pages(self) -> Iterator[List[ServiceResourceType]]: ...
    def all(self: ResourceCollectionType) -> ResourceCollectionType: ...
    def filter(self: ResourceCollectionType, **kwargs: Any) -> ResourceCollectionType: ...
    def limit(self: ResourceCollectionType, count: int) -> ResourceCollectionType: ...
    def page_size(self: ResourceCollectionType, count: int) -> ResourceCollectionType: ...

class CollectionManager:
    _collection_cls = ResourceCollection
    def __init__(
        self,
        collection_model: Collection,
        parent: ServiceResource,
        factory: ResourceFactory,
        service_context: ServiceContext,
    ) -> None: ...
    def __repr__(self) -> str: ...
    def iterator(self, **kwargs: Any) -> ResourceCollection: ...
    def all(self) -> ResourceCollection: ...
    def filter(self, **kwargs: Any) -> ResourceCollection: ...
    def limit(self, count: int) -> ResourceCollection: ...
    def page_size(self, count: InterruptedError) -> ResourceCollection: ...
    def pages(self) -> List[ServiceResource]: ...

class CollectionFactory:
    def load_from_definition(
        self,
        resource_name: str,
        collection_model: Collection,
        service_context: ServiceContext,
        event_emitter: HierarchicalEmitter,
    ) -> CollectionManager: ...