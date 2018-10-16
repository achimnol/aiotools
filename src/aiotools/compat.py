import asyncio


if hasattr(asyncio, 'get_running_loop'):
    get_running_loop = asyncio.get_running_loop
else:
    get_running_loop = asyncio.get_event_loop


if hasattr(asyncio, 'all_tasks'):
    all_tasks = asyncio.all_tasks
else:
    all_tasks = asyncio.Task.all_tasks
