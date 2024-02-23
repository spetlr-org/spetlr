class PowerBiClient:
    def __init__(
        self,
        *,
        client_id: str = None,
        client_secret: str = None,
        tenant_id: str = None,
    ):
        """
        Creates PowerBI client credentials.

        :param str client_id: PowerBI client ID.
        :param str client_secret: PowerBI client secret.
        :param str tenant_id: PowerBI tenant ID.
        """

        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
