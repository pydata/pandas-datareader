#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import List, Tuple, Optional

import asyncio
import requests


class ApiTest:
    """ Tests several api responses concurrently."""

    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self.futures = []

    def create_futures(self, urls: List):
        """ Creates a list of awaitable futures.

        @param urls: List of urls to request.
        """

        self.futures = [self.loop.run_in_executor(None, requests.get, url) for url in urls]

    async def perform_request(self) -> Tuple:
        """ Performs the request.

        @return: Tuple of all collected response codes.
        """

        return await asyncio.gather(*future.status for future in self.futures)

    async def run(self) -> Optional[bool]:
        """ Starts the event loop.

        @return: True if all request status codes are okay.
        """

        status_list = self.loop.run_until_complete(self.perform_request())
        assert all(status_list)

        return True
