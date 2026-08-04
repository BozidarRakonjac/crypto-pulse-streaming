"""
Schedules: the "when" -- attaches a cron-style interval to a job.
"""

from dagster import ScheduleDefinition
from dagster_project.jobs import pipeline_job

# Every minute. Cron syntax: minute hour day month day-of-week.
pipeline_schedule = ScheduleDefinition(
    job=pipeline_job,
    cron_schedule="* * * * *",
)