import base64
from typing import Any, TypeVar
import aiohttp  # type: ignore
from langfuse import Langfuse  # type: ignore
from langfuse.parse_error import Error
from pydantic import BaseModel  # type: ignore

from src.configmodels.config_types import pydantic_config



class LangfuseClient:
    def __init__(self, langfuse: type[Langfuse], configuration: pydantic_config) -> None:

        self.configuration = configuration

        try:
            self.client  = langfuse(**self.configuration.client_parameters)

        except (TypeError, KeyError, Error) as e:
            raise Error(f"Unexpected error: {e}")

        except Exception as e:
            raise Error(f"Unexpected error: {e}")

    async def request_to_langfuse(self):
        """
        This Method makes a request to the langfuse API. alternative to use the client.
        :return: the response in a json transformed format.
        """
        auth_header = base64.b64encode(f"{self.configuration.public_key}:{self.configuration.secret_key}".encode()).decode()
        headers = {
            "Authorization": f"Basic {auth_header}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(self.configuration.url) as response:
                response = await response.json()
                return response

    async def langfuse_observation_fetch(self,)-> dict[str, Any]:
        """
        Small utility function to fetch langfuse observation.

        :return: a dictionary with the observations fetched.
        """

        try:
            return self.client.fetch_observations(**self.configuration.fetch_filters)
        except Exception as e:
            raise Error(f"Unexpected error: {e}")