
from gevent.queue import Queue
from gevent.lock import BoundedSemaphore

# Queue processing log files
global tasks_workqueue
tasks_workqueue = Queue()

global tasks_worksem
tasks_worksem = BoundedSemaphore(1)

global tasks_workdict
tasks_workdict = {}

global co_routines_status
co_routines_status = 0
global co_statussem
co_statussem = BoundedSemaphore(1)

