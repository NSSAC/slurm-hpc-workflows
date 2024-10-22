"""Ray utilities."""

import math
import random
from typing import Callable
from more_itertools import chunked

import ray
from ray.util.dask import ray_dask_get

from dask import config as dask_config


def setup_ray_on_dask():
    dask_config.set(scheduler=ray_dask_get)


def compute_chunksize(
    args_list: list[tuple],
    num_workers: int | None,
    over_decomp_factor: int,
) -> int:
    if num_workers is None:
        return 1

    args_list_len = len(args_list)

    chunksize = args_list_len / (num_workers * over_decomp_factor)
    chunksize = int(math.ceil(chunksize))
    chunksize = max(1, chunksize)
    return chunksize


@ray.remote
def _ray_map_chunk(func, args_list_chunk, extra_args):
    results = []
    for args in args_list_chunk:
        if extra_args is not None:
            args = list(args)
            args.extend(extra_args)
        results.append(func(*args))
    return results


def ray_map(
    func: Callable,
    args_list: list[tuple],
    extra_args: tuple | None = None,
    chunksize: int | None = None,
    num_workers: int | None = None,
    over_decomp_factor: int = 10,
    shuffle: bool = False,
    **ray_options_kwargs,
):
    if chunksize is None:
        chunksize = compute_chunksize(args_list, num_workers, over_decomp_factor)

    if shuffle:
        random.shuffle(args_list)

    tasks = []
    for idx, args_list_chunk in enumerate(chunked(args_list, chunksize)):
        name = f"map-{func.__name__}-chunk-{idx}"
        task = _ray_map_chunk.options(name=name, **ray_options_kwargs)  # type: ignore
        task = task.remote(func, args_list_chunk, extra_args)
        tasks.append(task)
    results = ray.get(tasks)

    final_result = []
    for result in results:
        final_result.extend(result)

    return final_result


@ray.remote
def _ray_apply_chunk(func, args_list_chunk, extra_args):
    for args in args_list_chunk:
        if extra_args is not None:
            args = list(args)
            args.extend(extra_args)
        func(**args)
    return True


def ray_apply(
    func: Callable,
    args_list: list[tuple],
    extra_args: tuple | None = None,
    chunksize: int | None = None,
    num_workers: int | None = None,
    over_decomp_factor: int = 10,
    shuffle: bool = False,
    **ray_options_kwargs,
):
    if chunksize is None:
        chunksize = compute_chunksize(args_list, num_workers, over_decomp_factor)

    if shuffle:
        random.shuffle(args_list)

    tasks = []
    for idx, args_list_chunk in enumerate(chunked(args_list, chunksize)):
        name = f"apply-{func.__name__}-chunk-{idx}"
        task = _ray_apply_chunk.options(name=name, **ray_options_kwargs)  # type: ignore
        task = task.remote(func, args_list_chunk, extra_args)
        tasks.append(task)

    results = ray.get(tasks)
    assert all(results), "Unexpected result."

    return True
