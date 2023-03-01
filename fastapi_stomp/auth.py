"""
Authentication providers.
"""
import abc


class AsyncAuthenticator(abc.ABC):
    """ Abstract base class for authenticators. """

    @abc.abstractmethod
    async def authenticate_from_token(self, token: str):
        """
        Authenticate with the Alpha Zulu token
        """
