import sys
from awsglue.utils import getResolvedOptions
from core.constants import logger
from core.redshift_connection_helper import RedshiftConnectionHelper
from core.send_sns_notification import send_message_alert
from globalmeet.common.constants import PFS_JOB_ID, TODAY_STRING
from globalmeet.redshift_sql_commands.copy_command import sql_copy_table
from globalmeet.redshift_sql_commands.create_pfs_audience_table import CREATE_TABLE_AUDIENCE_DETAIL
from globalmeet.redshift_sql_commands.create_pfs_event_table import CREATE_TABLE_EVENT_SUMMARY
from globalmeet.model.pydantic.globalmeet_pipeline_events import GlobalMeetFailureEvent


def main(
    redshift_secret_arn: str,
    redshift_secret: str,
    copy_role: str,
    parquet_bucket: str,
    full_path_to_file: str,
    job_id: str,
    alert_topic: str,
    file_name: str,
    completion_topic: str,
    failure_topic: str,
    schema: str,
    job_name: str = "default_glue_job_name"
) -> None:

    rs_helper = RedshiftConnectionHelper(redshift_secret, redshift_secret_arn)

    try:
        logger.info(f"the job id is {job_id}")

        if job_id == PFS_JOB_ID:

            if "pfs_audience_detail" in file_name:
                copy_pfs_audience_detail = sql_copy_table(
                    schema,
                    f"staging_globalmeet_pfs_audience_detail_{TODAY_STRING}",
                    full_path_to_file,
                    copy_role
                )

                pfs_audience_create_query = CREATE_TABLE_AUDIENCE_DETAIL.format(
                    SCHEMA=schema,
                    today_string=TODAY_STRING
                )

                rs_helper.write_to_redshift_from_gluejob(pfs_audience_create_query)
                logger.info("pfs_audience_detail staging table was successfully created in redshift")

                rs_helper.write_to_redshift_from_gluejob(copy_pfs_audience_detail)
                logger.info("Loaded data into staging table pfs_audience_detail")

            elif "pfs_event_summary" in file_name:
                copy_pfs_virtual_event = sql_copy_table(
                    schema,
                    f"staging_globalmeet_pfs_event_summary_{TODAY_STRING}",
                    full_path_to_file,
                    copy_role
                )

                pfs_event_create_query = CREATE_TABLE_EVENT_SUMMARY.format(
                    SCHEMA=schema,
                    today_string=TODAY_STRING
                )

                rs_helper.write_to_redshift_from_gluejob(pfs_event_create_query)
                logger.info("pfs_event_summary staging table was successfully created in redshift")

                rs_helper.write_to_redshift_from_gluejob(copy_pfs_virtual_event)
                logger.info("Loaded data into staging table pfs_event_summary")

    except Exception as e:
        logger.exception(f"Exception occurred inserting records: {e}")
        send_message_alert(e, alert_topic, job_name)

        GlobalMeetFailureEvent(
            job_name=job_name,
            failure_msg=f"Exception occurred adding data: {e}",
            exception=e
        ).publish_event(failure_topic)

        raise Exception(e)


if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv,
        [
            "redshift_secret_arn",
            "redshift_secret",
            "copy_role",
            "parquet_bucket",
            "parquet_path",
            "job_id",
            "alert_topic",
            "file_name",
            "completion_topic",
            "failure_topic",
            "schema",
            "JOB_NAME"
        ]
    )

    main(
        redshift_secret_arn=args["redshift_secret_arn"],
        redshift_secret=args["redshift_secret"],
        copy_role=args["copy_role"],
        parquet_bucket=args["parquet_bucket"],
        full_path_to_file=args["parquet_path"],
        job_id=args["job_id"],
        alert_topic=args["alert_topic"],
        file_name=args["file_name"],
        completion_topic=args["completion_topic"],
        failure_topic=args["failure_topic"],
        schema=args["schema"],
        job_name=args["JOB_NAME"]
    )