"""Ray utilities."""

from more_itertools import chunked

import ray
from ray.util.dask import ray_dask_get

from dask import config as dask_config


def setup_ray_on_dask():
    dask_config.set(scheduler=ray_dask_get)


@ray.remote
def _ray_map_kwargs_chunk(func, kwargs_list_chunk):
    results = [func(**kwargs) for kwargs in kwargs_list_chunk]
    return results


def ray_map_kwargs(
    func,
    kwargs_list: list[dict],
    chunksize: int,
    **ray_options_kwargs,
):
    ray_map_kwargs_chunk = _ray_map_kwargs_chunk.options(**ray_options_kwargs)  # type: ignore

    kwargs_list_chunks = list(chunked(kwargs_list, chunksize))
    tasks = [
        ray_map_kwargs_chunk.remote(func, kwargs_list_chunk)
        for kwargs_list_chunk in kwargs_list_chunks
    ]
    results = ray.get(tasks)

    ret = []
    for result in results:
        ret.extend(result)

    return ret


@ray.remote
def _ray_apply_kwargs_chunk(func, kwargs_list_chunk):
    for kwargs in kwargs_list_chunk:
        func(**kwargs)


def ray_apply_kwargs(
    func,
    kwargs_list: list[dict],
    chunksize: int,
    **ray_options_kwargs,
):
    ray_apply_kwargs_chunk = _ray_apply_kwargs_chunk.options(**ray_options_kwargs)  # type: ignore

    kwargs_list_chunks = list(chunked(kwargs_list, chunksize))
    tasks = [
        ray_apply_kwargs_chunk.remote(func, kwargs_list_chunk)
        for kwargs_list_chunk in kwargs_list_chunks
    ]
    _ = ray.get(tasks)
