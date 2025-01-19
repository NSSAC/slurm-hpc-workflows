"""Ray utilities."""

import math
import random
from typing import Callable
from more_itertools import chunked

import ray

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
def _ray_map_chunk(func, args_idx_chunk, args_list, extra_args):
    results = []
    for idx in args_idx_chunk:
        args = args_list[idx]
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

    args_list_ref = ray.put(args_list)
    extra_args_ref = ray.put(extra_args)
    args_idx_list = list(range(len(args_list)))

    if shuffle:
        random.shuffle(args_idx_list)

    tasks = []
    for idx, args_idx_chunk in enumerate(chunked(args_idx_list, chunksize)):
        name = f"map-{func.__name__}-chunk-{idx}"
        task = _ray_map_chunk.options(name=name, **ray_options_kwargs)
        task = task.remote(func, args_idx_chunk, args_list_ref, extra_args_ref)
        tasks.append(task)
    results = ray.get(tasks)

    final_result = []
    for result in results:
        final_result.extend(result)

    return final_result


@ray.remote
def _ray_apply_chunk(func, args_idx_chunk, args_list, extra_args):
    for idx in args_idx_chunk:
        args = args_list[idx]
        if extra_args is not None:
            args = list(args)
            args.extend(extra_args)
        func(*args)


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

    args_list_ref = ray.put(args_list)
    extra_args_ref = ray.put(extra_args)
    args_idx_list = list(range(len(args_list)))

    if shuffle:
        random.shuffle(args_idx_list)

    tasks = []
    for idx, args_idx_chunk in enumerate(chunked(args_idx_list, chunksize)):
        name = f"apply-{func.__name__}-chunk-{idx}"
        task = _ray_apply_chunk.options(name=name, **ray_options_kwargs)
        task = task.remote(func, args_idx_chunk, args_list_ref, extra_args_ref)
        tasks.append(task)

    _ = ray.get(tasks)
