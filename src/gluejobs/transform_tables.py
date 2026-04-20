import sys
from awsglue.utils import getResolvedOptions
from core.constants import logger
from core.redshift_connection_helper import RedshiftConnectionHelper
from core.send_sns_notification import send_message_alert
from globalmeet.common.constants import PFS_JOB_ID, TODAY_STRING
from globalmeet.redshift_sql_commands.create_pfs_event_table import CREATE_TABLE_BASE_PFS_VIRTUAL_EVENT
from globalmeet.redshift_sql_commands.insert_command_table_pfs_event import INSERT_INTO_PFS_VIRTUAL_EVENT
from globalmeet.redshift_sql_commands.create_pfs_audience_table import CREATE_TABLE_BASE_PFS_AUDIENCE_DETAIL
from globalmeet.redshift_sql_commands.insert_command_table_pfs_audience_detail import INSERT_INTO_PFS_AUDIENCE_DETAIL
from globalmeet.redshift_sql_commands.drop_command import sql_drop_table
from globalmeet.model.pydantic.globalmeet_pipeline_events import GlobalMeetFailureEvent


def main(
    redshift_secret_arn: str,
    redshift_secret: str,
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
        logger.info(f"The job id is {job_id}")

        if job_id == PFS_JOB_ID:

            if "pfs_event_summary" in file_name:

                base_pfs_event_create_query = CREATE_TABLE_BASE_PFS_VIRTUAL_EVENT.format(
                    SCHEMA=schema
                )
                rs_helper.write_to_redshift_from_gluejob(base_pfs_event_create_query)
                logger.info("Successfully created baseline table pfs_virtual_event")

                base_pfs_event_insert_query = INSERT_INTO_PFS_VIRTUAL_EVENT.format(
                    SCHEMA=schema,
                    today_string=TODAY_STRING
                )
                rs_helper.write_to_redshift_from_gluejob(base_pfs_event_insert_query)
                logger.info("Successfully inserted records into baseline table pfs_virtual_event")

                rs_helper.write_to_redshift_from_gluejob(
                    sql_drop_table(schema, f"staging_globalmeet_pfs_event_summary_{TODAY_STRING}")
                )
                logger.info(f"Dropped table staging_globalmeet_pfs_event_summary_{TODAY_STRING}")

            if "pfs_audience_detail" in file_name:

                base_pfs_audience_create_query = CREATE_TABLE_BASE_PFS_AUDIENCE_DETAIL.format(
                    SCHEMA=schema
                )
                rs_helper.write_to_redshift_from_gluejob(base_pfs_audience_create_query)
                logger.info("Successfully created baseline table pfs_audience_detail")

                base_pfs_audience_insert_query = INSERT_INTO_PFS_AUDIENCE_DETAIL.format(
                    SCHEMA=schema,
                    today_string=TODAY_STRING
                )
                rs_helper.write_to_redshift_from_gluejob(base_pfs_audience_insert_query)
                logger.info("Successfully inserted records into baseline table pfs_audience_detail")

                rs_helper.write_to_redshift_from_gluejob(
                    sql_drop_table(schema, f"staging_globalmeet_pfs_audience_detail_{TODAY_STRING}")
                )
                logger.info(f"Dropped table staging_globalmeet_pfs_audience_detail_{TODAY_STRING}")

    except Exception as e:
        logger.exception(f"Exception occurred transforming tables: {e}")
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
        job_id=args["job_id"],
        alert_topic=args["alert_topic"],
        file_name=args["file_name"],
        completion_topic=args["completion_topic"],
        failure_topic=args["failure_topic"],
        schema=args["schema"],
        job_name=args["JOB_NAME"]
    )