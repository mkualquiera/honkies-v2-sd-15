def run_worker(in_queue: Queue, out_queue: Queue) -> None:
    """Run the worker.
    Parameters
    ----------
    in_queue : Queue
        The input queue.
    out_queue : Queue
        The output queue.
    """
    while True:
        try:
            message = in_queue.get()
            if message == "stop":
                break
            out_queue.put(message)
        except Exception as exception:
            logger.error(exception)
            break
