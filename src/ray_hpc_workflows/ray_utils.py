"""Ray utilities."""

from more_itertools import chunked

import ray
from ray.util.dask import ray_dask_get

from dask import config as dask_config


def setup_ray_on_dask():
    dask_config.set(scheduler=ray_dask_get)


@ray.remote
def _ray_map_kwargs_chunk(func, kwargs_list_chunk):
    results = []
    for kwargs in kwargs_list_chunk:
        results.append(func(**kwargs))
    return results


def ray_map_kwargs(
    func,
    kwargs_list: list[dict],
    chunksize: int,
    **ray_options_kwargs,
):
    tasks = []
    for idx, kwargs_list_chunk in enumerate(chunked(kwargs_list, chunksize)):
        name = f"map-{func.__name__}-chunk-{idx}"
        task = _ray_map_kwargs_chunk.options(name=name, **ray_options_kwargs)  # type: ignore
        task = task.remote(func, kwargs_list_chunk)
        tasks.append(task)
    results = ray.get(tasks)

    final_result = []
    for result in results:
        final_result.extend(result)

    return final_result


@ray.remote
def _ray_apply_kwargs_chunk(func, kwargs_list_chunk):
    for kwargs in kwargs_list_chunk:
        func(**kwargs)

    return True


def ray_apply_kwargs(
    func,
    kwargs_list: list[dict],
    chunksize: int,
    **ray_options_kwargs,
):
    tasks = []
    for idx, kwargs_list_chunk in enumerate(chunked(kwargs_list, chunksize)):
        name = f"apply-{func.__name__}-chunk-{idx}"
        task = _ray_apply_kwargs_chunk.options(name=name, **ray_options_kwargs)  # type: ignore
        task = task.remote(func, kwargs_list_chunk)
        tasks.append(task)

    results = ray.get(tasks)
    assert all(results), "Unexpected result."

    return True
