"""
Schedules: the "when" -- attaches a cron-style interval to a job.
"""

from dagster import ScheduleDefinition
from dagster_project.jobs import silver_job

# Every minute. Cron syntax: minute hour day month day-of-week.
silver_schedule = ScheduleDefinition(
    job=silver_job,
    cron_schedule="* * * * *",
)