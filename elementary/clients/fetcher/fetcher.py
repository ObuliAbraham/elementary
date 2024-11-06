from collections import defaultdict
from typing import Any, DefaultDict, Optional

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner


class FetcherClient:
    """A client that interacts with a DBT runner and caches results."""

    def __init__(self, dbt_runner: BaseDbtRunner):
        """
        Initialize the FetcherClient with a DBT runner.

        Args:
            dbt_runner (BaseDbtRunner): An instance of BaseDbtRunner for executing DBT commands.
        """
        self.dbt_runner = dbt_runner
        self.run_cache: DefaultDict[str, Optional[Any]] = defaultdict(lambda: None)

    def set_run_cache(self, key: str, value: Any) -> None:
        """
        Set a value in the run cache.

        Args:
            key (str): The key under which the value is stored.
            value (Any): The value to be cached.
        """
        self.run_cache[key] = value

    def get_run_cache(self, key: str) -> Optional[Any]:
        """
        Retrieve a value from the run cache.

        Args:
            key (str): The key for which the value is to be retrieved.

        Returns:
            Optional[Any]: The cached value, or None if the key does not exist.
        """
        return self.run_cache[key]