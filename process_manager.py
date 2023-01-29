"""Manages the worker process, starting it if it dies.
"""

import asyncio
from multiprocessing import Process, Queue
from typing import Callable

from loguru import logger

from worker import run_worker


class WorkerProcessManager:
    """Manage the worker process.

    Attributes
    ----------
    _input_queue : Queue | None
        The input queue.
    _output_queue : Queue | None
        The output queue.
    _process : Process | None
        The process.
    _dead_worker_callback: Callable | None
        The callback to call when the worker process dies.

    Properties
    ----------
    input_queue : Queue
        The input queue.
    output_queue : Queue
        The output queue.
    """

    @property
    def input_queue(self) -> Queue:
        """Get the input queue.

        Returns
        -------
        Queue
            The input queue.
        """
        while self._input_queue is None:
            # Error
            raise RuntimeError("Input queue is None")

        return self._input_queue

    @property
    def output_queue(self) -> Queue:
        """Get the output queue.

        Returns
        -------
        Queue
            The output queue.
        """
        while self._output_queue is None:
            # Error
            raise RuntimeError("Output queue is None")

        return self._output_queue

    def __init__(self, dead_callback: Callable) -> None:
        """Initialize.

        Parameters
        ----------
        dead_callback : Callable
            The callback to call when the worker process dies.
        """
        self._input_queue = None
        self._output_queue = None
        self._process = None
        self._dead_worker_callback = dead_callback
        self._watchdog_task = None

    def _start_process(self) -> None:
        """Start the worker process."""
        self._input_queue = Queue()
        self._output_queue = Queue()
        self._process = Process(
            target=run_worker, args=(self._input_queue, self._output_queue)
        )
        self._process.start()

    async def _watchdog(self) -> None:
        """Watchdog."""
        while True:
            if self._process is not None and not self._process.is_alive():
                logger.error("Process is dead, restarting")
                self._dead_worker_callback()
                self._start_process()
            await asyncio.sleep(1)

    async def start(self) -> None:
        """Start the manager."""
        if self._watchdog_task is None:
            self._watchdog_task = asyncio.create_task(self._watchdog())
        else:
            raise RuntimeError("Already started")

    async def stop(self) -> None:
        """Stop the manager."""
        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            self._watchdog_task = None
        else:
            raise RuntimeError("Not started")
