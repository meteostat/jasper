"""
Run Meteor tasks

The code is licensed under the MIT license.
"""

from .meteor import Meteor

def run(task_ref: Meteor) -> None:
    """
    Run Meteor task
    """
    task = task_ref()
    task.setup()
    task.main()
