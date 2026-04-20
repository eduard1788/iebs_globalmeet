from string import Template
import boto3
import json
import re
import os


PFS_AUDIENCE_FILE_SUFFIX = "pfs_audience_detail"
PFS_EVENT_FILE_SUFFIX = "pfs_event_summary"
PFS_SURVEY_FILE_SUFFIX = "pfs_question_and_survey_result"

def lambda_handler(event, context):
    """
    Función que reaccionará ante la carga de archivos en s3.
    Luego validará el evento recibido para extraer parametros de interés para el flow.
    Estos parámetros se pasarán al step function cuando se inicie éste.
    :param event: el evento que ejecuta al lambda
    :param context: _
    :return: None
    """

    print("Función Lambda comienza...")

    sfn_arn = ["step_function_arn"]
    s3_parquet = os.environ["parquet_bucket"]
    json_event = json.loads(event["Records"][0]["body"])
    safe_mode: str = os.environ["safe_mode"]
    file_name = json_event["output_file_name"]
    edl_job_id = str(json_event["job_id"])

    # Validar que archivo se ha recibido (audience, event summary o survey result)
    if PFS_AUDIENCE_FILE_SUFFIX in file_name:
        file_sub_folder = PFS_AUDIENCE_FILE_SUFFIX
    elif PFS_EVENT_FILE_SUFFIX in file_name:
        file_sub_folder = PFS_EVENT_FILE_SUFFIX
    elif PFS_SURVEY_FILE_SUFFIX in file_name:
        file_sub_folder = PFS_SURVEY_FILE_SUFFIX
    else:
        print("Fichero invalido. Por favor cargar el fichero correcto.")
        return None

    client = boto3.client("stepfunctions")

    #
    pattern = r'\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}'
    ingest_date = re.findall(pattern, file_name)[0].split('_')[-1]

    path_template = Template("s3://$bucket/job_id=$edl_job_id/$file_sub_folder/business_date=$ingest_date/")
    full_path = path_template.substitute(
        {
            "bucket": s3_parquet,
            "eld_job_id": edl_job_id,
            "file_sub_folder": file_sub_folder,
            "ingest_date": ingest_date
        }
    )

    output_event = {
        "job_id": edl_job_id,
        "parquet_path": full_path,
        "file_name": file_name,
        "ingest_date": ingest_date,
        "safe_mode": safe_mode
    }

    try:
        execute_response = client.start_execution(
            stateMachine=sfn_arn,
            name=file_name,
            input=json.dumps(output_event)
        )

    except Exception as ex:
        print(f"Failed to invoke Step Function: {ex}.")
