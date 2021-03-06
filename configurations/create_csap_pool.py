from core_data_modules.cleaners import somali
from core_data_modules.cleaners.codes import Codes
from dateutil.parser import isoparse

from src.pipeline_configuration_spec import *

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="Create-CSAP-Somalia-Pool",
    description="Creates the initial CSAP-Somalia pool using demographics flows from EU-PCVE s01, FCDO-EiE, TIS-Plus, "
                "USAID-IBTCI, and JPLG-2020.",
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/avf-engagement-databases-firebase-credentials-file.json",
        database_path="engagement_databases/CSAP_Somalia"
    ),
    uuid_table=UUIDTableClientConfiguration(
        credentials_file_url="gs://avf-credentials/avf-id-infrastructure-firebase-adminsdk-6xps8-b9173f2bfd.json",
        table_name="avf-global-urn-to-participant-uuid",
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
                    FlowResultConfiguration("csap_eu_pcve_demog", "location", "location"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "gender", "gender"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "age", "age"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "recently_displaced", "recently_displaced"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "hh_language", "household_language"),

                    # TIS-Plus (ran from October to December 2020)
                    FlowResultConfiguration("csap_s09_demog", "location", "location"),
                    FlowResultConfiguration("csap_s09_demog", "idp_camp", "in_idp_camp"),
                    FlowResultConfiguration("csap_s09_demog", "gender", "gender"),
                    FlowResultConfiguration("csap_s09_demog", "age", "age"),
                    FlowResultConfiguration("csap_s09_demog", "recently_displaced", "recently_displaced"),
                ]
            )
        ),
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/csap-2-textit-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    # USAID-IBTCI (ran from November 2020 to March 2021)
                    FlowResultConfiguration("csap_s08_demog", "age", "age"),
                    FlowResultConfiguration("csap_s08_demog", "gender", "gender"),
                    FlowResultConfiguration("csap_s08_demog", "location", "location"),
                    FlowResultConfiguration("csap_s08_demog", "recently_displaced", "recently_displaced"),
                    FlowResultConfiguration("csap_s08_demog", "livelihood", "livelihood"),
                    FlowResultConfiguration("csap_s08_demog", "hh_language", "household_language"),

                    # JPLG-2020 (ran in June 2020)
                    FlowResultConfiguration("csap_s07_demog", "gender", "gender"),
                    FlowResultConfiguration("csap_s07_demog", "location", "location"),
                    FlowResultConfiguration("csap_s07_demog", "age", "age"),
                    FlowResultConfiguration("csap_s07_demog", "recently_displaced", "recently_displaced"),
                    FlowResultConfiguration("csap_s07_demog", "idp_camp", "in_idp_camp"),
                    FlowResultConfiguration("csap_s07_demog", "hh_language", "household_language"),
                ]
            )
        ),
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/csap-3-text-it-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    # FCDO-EiE (ran late 2020 to early 2021)
                    FlowResultConfiguration("csap_s10_demog", "location", "location"),
                    FlowResultConfiguration("csap_s10_demog", "gender", "gender"),
                    FlowResultConfiguration("csap_s10_demog", "age", "age"),
                    FlowResultConfiguration("csap_s10_demog", "recently_displaced", "recently_displaced"),
                    FlowResultConfiguration("csap_s10_demog", "livelihood", "livelihood")
                ]
            )
        )
    ],
    csv_sources=[
        # These CSV sources are from EU s01, where approximately half of messages from Hormuud via Shaqodoon didn't
        # make it to TextIt.
        CSVSource(
            "gs://avf-project-datasets/2022/CREATE-CSAP-POOL/recovery_csvs/recovered_golis_s01e01_de_identified.csv",
            engagement_db_datasets=[
                CSVDatasetConfiguration("eu_pcve_s01e01")
            ],
            timezone="Africa/Mogadishu"
        ),
        CSVSource(
            "gs://avf-project-datasets/2022/CREATE-CSAP-POOL/recovery_csvs/recovered_golis_s01e03_de_identified.csv",
            engagement_db_datasets=[
                CSVDatasetConfiguration("eu_pcve_s01e03")
            ],
            timezone="Africa/Mogadishu"
        ),
        CSVSource(
            "gs://avf-project-datasets/2022/CREATE-CSAP-POOL/recovery_csvs/recovered_hormuud_s01_aug_sep_de_identified.csv",
            engagement_db_datasets=[
                CSVDatasetConfiguration("eu_pcve_s01e01", start_date=isoparse("2021-08-29T00:00:00+03:00"), end_date=isoparse("2021-09-04T24:00:00+03:00")),
                CSVDatasetConfiguration("eu_pcve_s01e02", start_date=isoparse("2021-09-05T00:00:00+03:00"), end_date=isoparse("2021-09-11T24:00:00+03:00")),
                CSVDatasetConfiguration("eu_pcve_s01e03", start_date=isoparse("2021-09-12T00:00:00+03:00"), end_date=isoparse("2021-09-18T24:00:00+03:00")),
                CSVDatasetConfiguration("eu_pcve_s01e04", start_date=isoparse("2021-09-19T00:00:00+03:00"), end_date=isoparse("2021-09-25T24:00:00+03:00")),
                CSVDatasetConfiguration("eu_pcve_s01e05", start_date=isoparse("2021-09-26T00:00:00+03:00"), end_date=isoparse("2021-09-30T24:00:00+03:00"))
            ],
            timezone="Africa/Mogadishu"
        )
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-production.json"),
        sync_config=CodaSyncConfiguration(
            project_users_file_url="gs://avf-project-datasets/2021/EU-PCVE/s02/coda_users.json",
            dataset_configurations=[
                CodaDatasetConfiguration(
                    coda_dataset_id="EU_PCVE_rqa_s01e01",
                    engagement_db_dataset="eu_pcve_s01e01",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s01e01"), auto_coder=None)
                    ],
                    ws_code_string_value="eu_pcve_s01e01"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="EU_PCVE_rqa_s01e02",
                    engagement_db_dataset="eu_pcve_s01e02",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s01e02"), auto_coder=None)
                    ],
                    ws_code_string_value="eu_pcve_s01e02"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="EU_PCVE_rqa_s01e03",
                    engagement_db_dataset="eu_pcve_s01e03",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s01e03"), auto_coder=None)
                    ],
                    ws_code_string_value="eu_pcve_s01e03"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="EU_PCVE_rqa_s01e04",
                    engagement_db_dataset="eu_pcve_s01e04",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s01e04"), auto_coder=None)
                    ],
                    ws_code_string_value="eu_pcve_s01e04"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="EU_PCVE_rqa_s01e05",
                    engagement_db_dataset="eu_pcve_s01e05",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s01e05"), auto_coder=None)
                    ],
                    ws_code_string_value="eu_pcve_s01e05"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="CSAP_location",
                    engagement_db_dataset="location",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/mogadishu_sub_district"),
                                                auto_coder=somali.DemographicCleaner.clean_mogadishu_sub_district,
                                                coda_code_schemes_count=1),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/somalia_district"),
                                                auto_coder=lambda text:
                                                somali.DemographicCleaner.clean_somalia_district(text)
                                                if somali.DemographicCleaner.clean_mogadishu_sub_district == Codes.NOT_CODED
                                                else Codes.NOT_CODED,
                                                coda_code_schemes_count=1),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/somalia_region"),
                                                auto_coder=None,
                                                coda_code_schemes_count=1),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/somalia_state"),
                                                auto_coder=None,
                                                coda_code_schemes_count=1),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/somalia_zone"),
                                                auto_coder=None,
                                                coda_code_schemes_count=1)
                    ],
                    ws_code_string_value="location"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="CSAP_age",
                    engagement_db_dataset="age",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/age"),
                                                auto_coder=lambda text: str(somali.DemographicCleaner.clean_age_within_range(text)),
                                                coda_code_schemes_count=1),
                    ],
                    ws_code_string_value="age"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="CSAP_gender",
                    engagement_db_dataset="gender",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/gender"),
                                                auto_coder=somali.DemographicCleaner.clean_gender,
                                                coda_code_schemes_count=1)
                    ],
                    ws_code_string_value="gender"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="CSAP_household_language",
                    engagement_db_dataset="household_language",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/household_language"),
                                                auto_coder=None,
                                                coda_code_schemes_count=1)
                    ],
                    ws_code_string_value="household_language"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="CSAP_recently_displaced",
                    engagement_db_dataset="recently_displaced",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/recently_displaced"),
                                                auto_coder=None,
                                                coda_code_schemes_count=1)
                    ],
                    ws_code_string_value="recently_displaced"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="CSAP_in_idp_camp",
                    engagement_db_dataset="in_idp_camp",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/in_idp_camp"),
                                                auto_coder=somali.DemographicCleaner.clean_yes_no,
                                                coda_code_schemes_count=1)
                    ],
                    ws_code_string_value="in_idp_camp"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="CSAP_livelihood",
                    engagement_db_dataset="livelihood",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/livelihood"),
                                                auto_coder=None,
                                                coda_code_schemes_count=1)
                    ],
                    ws_code_string_value="livelihood"
                ),

            ],
            set_dataset_from_ws_string_value=True,
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset")
        )
    ),
    rapid_pro_target=RapidProTarget(
        rapid_pro=RapidProClientConfiguration(
            domain="textit.com",
            token_file_url="gs://avf-credentials/csap-text-it-token.txt"
        ),
        sync_config=EngagementDBToRapidProConfiguration(
            normal_datasets=[
                DatasetConfiguration(
                    engagement_db_datasets=["location"],
                    rapid_pro_contact_field=ContactField(key="csap_pool_location", label="csap pool location")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["age"], 
                    rapid_pro_contact_field=ContactField(key="csap_pool_age", label="csap pool age")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["gender"], 
                    rapid_pro_contact_field=ContactField(key="csap_pool_gender", label="csap pool gender")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["household_language"], 
                    rapid_pro_contact_field=ContactField(key="csap_pool_household_language", label="csap pool household language")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["recently_displaced"], 
                    rapid_pro_contact_field=ContactField(key="csap_pool_recently_displaced", label="csap pool recently displaced")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["in_idp_camp"],
                    rapid_pro_contact_field=ContactField(key="csap_pool_in_idp_camp", label="csap pool in idp camp")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["livelihood"],
                    rapid_pro_contact_field=ContactField(key="csap_pool_livelihood", label="csap pool livelihood")
                ),
            ],
            consent_withdrawn_dataset=None,
            write_mode=WriteModes.CONCATENATE_TEXTS,
            allow_clearing_fields=True
        )
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2022/Create-CSAP-Somalia-Pool"
    )
)
