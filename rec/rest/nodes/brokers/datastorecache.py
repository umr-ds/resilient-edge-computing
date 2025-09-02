import threading
import time
from typing import Optional
from uuid import UUID

from fastapi_pagination import Page
from pydantic import BaseModel

from rec.rest.nodetypes.datastore import Datastore


class CachePage(BaseModel):
    datastore: Datastore
    page: Page
    time: float


class DatastoreCache:
    cache: dict[Optional[UUID], dict[str, list[CachePage]]]
    lock: threading.Lock

    def __init__(self):
        self.cache = {}
        self.lock = threading.Lock()

    def get(self, name: str, job_id: Optional[UUID] = None) -> Optional[Datastore]:
        with self.lock:
            res = self.__get_cache_place(name, job_id)
            if res is not None:
                cache_page, _, _ = res
                return cache_page.datastore
            return None

    def set(
        self,
        page: Page,
        datastore: Datastore,
        search: str = "",
        job_id: Optional[UUID] = None,
    ) -> None:
        with self.lock:
            job_dict = self.cache.get(job_id, None)
            if job_dict is None:
                job_dict = {}
                self.cache[job_id] = job_dict
            search_list = job_dict.get(search, None)
            if search_list is None:
                search_list = []
                job_dict[search] = search_list
            search_list.append(
                CachePage(datastore=datastore, page=page, time=time.time())
            )

    def invalidate(self, name: str, job_id: UUID) -> None:
        with self.lock:
            res = self.__get_cache_place(name, job_id)
            if res is not None:
                cache_page, job_id, search = res
                self.cache[job_id][search].remove(cache_page)

    def __get_cache_place(
        self, name: str, job_id: Optional[UUID]
    ) -> Optional[tuple[CachePage, Optional[UUID], str]]:
        page = False
        job_dict = self.cache.get(job_id, None)
        if job_dict is None:
            return None
        for search, cache_pages in job_dict.items():
            remove_list = []
            if search in name:
                for cache_page in cache_pages:
                    if time.time() - cache_page.time > 60:
                        remove_list.append(cache_page)
                    if name in cache_page.page.items:
                        page = cache_page
                        break
                if len(remove_list):
                    self.cache[job_id][search] = [
                        cache_page
                        for cache_page in cache_pages
                        if cache_page not in remove_list
                    ]
            if page:
                return page, job_id, search
