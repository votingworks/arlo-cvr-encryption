import sys
from typing import Any, List, Optional, Union, overload

import boto3
import boto3.utils
import botocore.session
from boto3.exceptions import ResourceNotExistsError, UnknownAPIVersionError
from boto3.resources.factory import ResourceFactory
from botocore.client import Config
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import DataNotFoundError, UnknownServiceError
from botocore.loaders import Loader
from botocore.model import ServiceModel
from mypy_boto3_ec2.client import EC2Client
from mypy_boto3_ec2.service_resource import EC2ServiceResource
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.service_resource import S3ServiceResource

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

class Session:
    def __init__(
        self,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        aws_session_token: str = None,
        region_name: str = None,
        botocore_session: Session = None,
        profile_name: str = None,
    ):
        self._session: ServiceModel
        self.resource_factory: ResourceFactory
        self._loader: Loader
    def __repr__(self) -> str: ...
    @property
    def profile_name(self) -> str: ...
    @property
    def region_name(self) -> str: ...
    @property
    def events(self) -> List[Any]: ...
    @property
    def available_profiles(self) -> List[Any]: ...
    def _setup_loader(self) -> None: ...
    def get_available_services(self) -> List[str]: ...
    def get_available_resources(self) -> List[str]: ...
    def get_available_partitions(self) -> List[str]: ...
    def get_available_regions(
        self,
        service_name: str,
        partition_name: str = "aws",
        allow_non_regional: bool = False,
    ) -> List[str]: ...
    def get_credentials(self) -> Credentials: ...
    def _register_default_handlers(self) -> None: ...
    @overload
    def client(
        self,
        service_name: Literal["s3"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> S3Client: ...
    @overload
    def client(
        self,
        service_name: Literal["ec2"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> EC2Client: ...
    @overload
    def resource(
        self,
        service_name: Literal["s3"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> S3ServiceResource: ...
    @overload
    def resource(
        self,
        service_name: Literal["ec2"],
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: Optional[bool] = None,
        verify: Union[bool, str, None] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> EC2ServiceResource: ...
