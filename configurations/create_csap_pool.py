from core_data_modules.cleaners import somali
from core_data_modules.cleaners.codes import Codes

from src.pipeline_configuration_spec import *

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="Create-CSAP-Somalia-Pool",
    # TODO: Add remaining projects to this description once their configurations are included too.
    description="Creates the initial CSAP-Somalia pool using demographics flows from EU-PCVE s01, ",
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",  # TODO: Switch to production
        database_path="engagement_db_experiments/csap_test"
    ),
    uuid_table=UUIDTableClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",  # TODO: Switch to production
        table_name="_engagement_db_csap_test",  # TODO: Switch to production
        uuid_prefix="avf-participant-uuid-"
    ),
    operations_dashboard=OperationsDashboardConfiguration(
        credentials_file_url="gs://avf-credentials/avf-dashboards-firebase-adminsdk-gvecb-ef772e79b6.json",
    ),
    rapid_pro_sources=[
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/csap-text-it-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    # EU-PCVE s01
                    FlowResultConfiguration("csap_eu_pcve_demog", "location", "csap_location"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "gender", "csap_gender"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "age", "csap_age"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "recently_displaced", "csap_recently_displaced"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "hh_language", "csap_household_language"),
                ]
            )
        )
    ],
    archive_configuration=None
)
