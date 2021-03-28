import asyncio


if hasattr(asyncio, 'get_running_loop'):
    get_running_loop = asyncio.get_running_loop
else:
    get_running_loop = asyncio.get_event_loop


if hasattr(asyncio, 'all_tasks'):
    all_tasks = asyncio.all_tasks
else:
    all_tasks = asyncio.Task.all_tasks  # type: ignore


if hasattr(asyncio, 'current_task'):
    current_task = asyncio.current_task
else:
    current_task = asyncio.Task.current_task  # type: ignore
