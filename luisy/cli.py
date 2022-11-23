# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0
"""
This module contains the entrypoint of luisy. Here we catch the CLI arguments of the user and
parse them inside a function. We also separate luigi params from luisy params here. After setting up
the luisy parameters in our :py:class:`luisy.config.Config` singleton, the logic of luisy is
called.
"""

import argparse
import logging
import sys
from luisy.luigi_interface import (
    PipelineMode,
    HashUpdateMode,
    get_root_task,
)
from luisy.config import get_default_params

logger = logging.getLogger(__name__)


def disable_azure_logging():
    log = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
    log.setLevel(logging.ERROR)


def parse_args(argv):
    """
    Method to get all luisy args out of the argv given inside the cli. Unknown arguments will be
    returned separately, to later forward them to luigi

    Args:
        argv: Should (conceptually) be ``sys.argv[1:]``

    Returns:
        tuple: dict of parsed luisy arguments and rest of
            `argv` in an unparsed format

    """
    parser = argparse.ArgumentParser(description='luisy Parser')
    for key, val in get_default_params().items():
        key = key.replace('_', '-')
        if type(val) == bool:
            parser.add_argument('--' + key, default=val, action='store_true')
        else:
            parser.add_argument('--' + key, default=val)

    args_luisy, args_luigi = parser.parse_known_args(argv)
    args_luisy = {key: val for key, val in vars(args_luisy).items() if val is not None}
    return args_luisy, args_luigi


def luisy_run(argv=sys.argv[1:]):
    """
    luisy CLI Call

    Args:
        argv: Should (conceptually) be ``sys.argv[1:]``

    """

    disable_azure_logging()
    args_luisy, args_luigi = parse_args(argv)
    root_task = get_root_task(args_luigi)
    force_hash_update = args_luisy.pop('hash_update_mode')
    if force_hash_update:
        ret_code = HashUpdateMode(
            args_luisy=args_luisy,
            root_task=root_task
        ).run()
    else:
        # Run the pipeline
        ret_code = PipelineMode(
            args_luisy=args_luisy,
            args_luigi=args_luigi,
            root_task=root_task,
        ).run()

    sys.exit(ret_code)


def build(task, workers=1, local_scheduler=True, **args_luisy):
    """
    Similar to :py:func:`luigi.build`, it executes a task and along with upstream requirements.
    There, it uses the hashing functionality to detect tasks that need a rerun.

    Args:
        task (luisy.base.Task): The luisy task to be executed (along with its upstream
            requirements).
        workers (int): Number of (luigi) workers
        local_scheduler (bool): Whether to use the local scheduler of luigi

    Returns:
        luisy.luigi_interface: Coordinator: The coordinator object of the luisy/luigi build
    """

    args_luigi = [f"--workers={workers}"]

    if local_scheduler:
        args_luigi.append('--local-scheduler')

    if 'no_ask' not in args_luisy:
        args_luisy['no_ask'] = True

    if 'hash_update_mode' not in args_luisy:
        hash_update_mode = False
    else:
        hash_update_mode = args_luisy['hash_update_mode']

    if hash_update_mode:
        coordinator = HashUpdateMode(
            args_luisy=args_luisy,
            root_task=task
        )
    else:
        # Run the pipeline
        coordinator = PipelineMode(
            args_luisy=args_luisy,
            args_luigi=args_luigi,
            root_task=task,
            run_luigi_cli=False
        )

    coordinator.run()
    return coordinator
