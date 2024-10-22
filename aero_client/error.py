"""DSaaS Client error definition module"""


class ClientError(Exception):
    """
    Error for the AERO client.
    """

    def __init__(self, *args: object, **kwargs) -> None:
        self.code = kwargs.get("code", 500)
        self.message = kwargs.get("message", "Unexpected error")
        super().__init__(*args)

    def __repr__(self) -> str:
        return f"ClientError({self.code}) : {self.message}"
