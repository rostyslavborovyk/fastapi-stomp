"""
Authentication providers.
"""
import abc
import typing as t

TUser = t.TypeVar("TUser")


class AsyncAuthenticator(abc.ABC, t.Generic[TUser]):
    """ Abstract base class for authenticators. """
    user: TUser

    def __init__(self):
        self.user: TUser | None = None

    @abc.abstractmethod
    async def authenticate_from_token(self, token: str) -> bool:
        """
        Authenticate with the Alpha Zulu token
        """
