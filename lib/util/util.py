import functools
from asyncio import events, ensure_future, futures
from asyncio.tasks import _release_waiter, _cancel_and_wait


async def _waiter_check_cancel(cb):
    while True:
        if await cb():
            return True
        import time
        # time.sleep(1)
        import asyncio
        await asyncio.sleep(1)


async def wait_for_with_cancel(fut, timeout, *, loop=None, check_cancel=None):
    """Wait for the single Future or coroutine to complete, with timeout.

    Coroutine will be wrapped in Task.

    Returns result of the Future or coroutine.  When a timeout occurs,
    it cancels the task and raises TimeoutError.  To avoid the task
    cancellation, wrap it in shield().

    If the wait is cancelled, the task is also cancelled.

    This function is a coroutine.
    """
    if loop is None:
        loop = events.get_event_loop()

    if timeout is None:
        return await fut

    if timeout <= 0:
        fut = ensure_future(fut, loop=loop)

        if fut.done():
            return fut.result()

        fut.cancel()
        raise futures.TimeoutError()

    if check_cancel is None:
        waiter = loop.create_future()
    else:
        waiter = _waiter_check_cancel(check_cancel)

    timeout_handle = loop.call_later(timeout, _release_waiter, waiter)
    cb = functools.partial(_release_waiter, waiter)

    fut = ensure_future(fut, loop=loop)
    fut.add_done_callback(cb)

    try:
        # wait until the future completes or the timeout
        try:
            await waiter
        except futures.CancelledError:
            fut.remove_done_callback(cb)
            fut.cancel()
            raise

        if fut.done():
            return fut.result()
        else:
            fut.remove_done_callback(cb)
            # We must ensure that the task is not running
            # after wait_for() returns.
            # See https://bugs.python.org/issue32751
            await _cancel_and_wait(fut, loop=loop)
            raise futures.TimeoutError()
    finally:
        timeout_handle.cancel()
