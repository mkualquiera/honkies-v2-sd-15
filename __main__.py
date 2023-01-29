"""Entry point for the worker client."""

import asyncio
import json
from enum import Enum
from typing import cast

from loguru import logger
from websockets.exceptions import ConnectionClosedError, InvalidHandshake, InvalidURI
from websockets.legacy.client import WebSocketClientProtocol, connect
from websockets.typing import Data

from process_manager import WorkerProcessManager


class JobState(Enum):
    """The job state.

    Attributes
    ----------
    PENDING : str
        The job has been received from the server but hasn't been sent to the
        worker process yet.
    SCHEDULED : str
        The job has been sent to the worker process but it hasn't started working
        on it yet.
    RUNNING : str
        The job is being worked on by the worker process.
    DONE : str
        The job has been completed by the worker process.
    FAILED : str
        The job has failed.
    """

    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


class ServerToClientMessage(Enum):
    """The message from the server to the client.

    Attributes
    ----------
    JOB : str
        The message contains a job.
    """

    JOB = "job"


class ClientToServerMessage(Enum):
    """The message from the client to the server.

    Attributes
    ----------
    JOB_STATE : str
        The message contains the state of a job.
    """

    JOB_STATE = "job_state"


class WorkerClientManager:
    """Manage the worker client.

    It connects to the server, reconnecting if necessary. It handles messages
    from the server and sends them to the worker process, and also handles
    messages from the worker process and sends them to the server. All jobs
    in the job queue get invalidated when the worker process dies.

    Attributes
    ----------
    _process_manager : WorkerProcessManager
        The process manager.
    _queue: list[dict]
        The job queue.
    _callback_uri : str
        The callback uri.
    """

    def __init__(self, callback_uri: str) -> None:
        """Initialize the worker client manager.

        Parameters
        ----------
        callback_uri : str
            The callback uri.
        """
        self._process_manager = WorkerProcessManager(self._dead_worker_callback)
        self._queue = []
        self._callback_uri = callback_uri

    def _dead_worker_callback(self) -> None:
        """The callback to call when the worker process dies."""
        logger.info("Worker process died")
        for job in self._queue:
            job["state"] = JobState.PENDING

    async def start(self) -> None:
        """Start the worker client manager."""

        await self._process_manager.start()

        while True:
            try:
                async with connect(self._callback_uri) as websocket:
                    await self._run_client(websocket)
            except (InvalidHandshake, ConnectionClosedError) as exception:
                logger.error(exception)
                await asyncio.sleep(1)
            except InvalidURI as exception:
                logger.error(exception)
                break

    async def _run_client(self, websocket: WebSocketClientProtocol) -> None:
        """Run the websocket client.

        Parameters
        ----------
        websocket : websockets.legacy.client.WebSocketClientProtocol
            The websocket client.
        """

        while True:
            try:
                message = await websocket.recv()
                await self._handle_receive(message, websocket)
            except ConnectionClosedError:
                break

    async def _handle_receive(
        self, message: Data, websocket: WebSocketClientProtocol
    ) -> None:
        """Handle receiving a message from the server.

        Parameters
        ----------
        message : websockets.typing.Data
            The message.
        websocket : websockets.legacy.client.WebSocketClientProtocol
            The websocket client.
        """

        # Parse as JSON
        try:
            data = json.loads(message)
        except json.JSONDecodeError as exception:
            logger.error(f"Failed to parse message as JSON ({message})")
            logger.error(exception)
            return

        data = cast(dict, data)
        message_type = data.get("type", None)

        try:
            message_type = ServerToClientMessage(message_type)
        except ValueError:
            logger.error(f"Invalid message type ({message_type})")
            logger.error(data)
            return

        match message_type:
            case ServerToClientMessage.JOB:
                await self._handle_job(data, websocket)
            case _:
                logger.error(f"Unhandled message type ({message_type})")

    def validate_job(self, job: dict) -> bool:
        """Validate a job.

        Parameters
        ----------
        job : dict
            The job.

        Returns
        -------
        bool
            Whether the job is valid.
        """

        # FIXME: Validate job
        return True

    async def _report_job_state(
        self, job: dict, websocket: WebSocketClientProtocol
    ) -> None:
        """Report the state of a job to the server.

        Parameters
        ----------
        job : dict
            The job.
        websocket : websockets.legacy.client.WebSocketClientProtocol
            The websocket client.
        """

        state = job["state"]
        job_id = job["id"]

        message = {
            "type": ClientToServerMessage.JOB_STATE.value,
            "id": job_id,
            "state": state.value,
        }

        await websocket.send(json.dumps(message))

    async def _handle_job(self, data: dict, websocket: WebSocketClientProtocol) -> None:
        """Handle a job.

        Parameters
        ----------
        data : dict
            The job.
        websocket : websockets.legacy.client.WebSocketClientProtocol
            The websocket client.
        """

        job = data.get("job", {})

        is_valid = self.validate_job(job)

        if not is_valid:
            logger.error(f"Invalid job ({job})")
            return

        # Tag job as pending
        job["state"] = JobState.PENDING

        # Add job to queue
        self._queue.append(data)

        # Report job state
        await self._report_job_state(job, websocket)
