from core_data_modules.cleaners import somali
from core_data_modules.cleaners.codes import Codes
from dateutil.parser import isoparse

from src.pipeline_configuration_spec import *

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="EU-PCVE-S03",
    description="Runs the EU-PCVE season 3 pipeline",
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
                    FlowResultConfiguration("csap_eu_pcve_s03e01_activation", "rqa_eu_pcve_s03e01", "eu_pcve_s03e01"),
                    FlowResultConfiguration("csap_eu_pcve_s03e02_activation", "rqa_eu_pcve_s03e02", "eu_pcve_s03e02"),
                    FlowResultConfiguration("csap_eu_pcve_s03e03_activation", "rqa_eu_pcve_s03e03", "eu_pcve_s03e03"),
                    FlowResultConfiguration("csap_eu_pcve_s03_closeout_activation", "eu_pcve_s03_closeout", "eu_pcve_s03_closeout"),

                    # The s03e02 sms ad sent people to the s01e02 activation flow by mistake.
                    # Fetch the recent data from that flow and include it in the eu_pcve_s03e02 dataset too.
                    FlowResultConfiguration("csap_eu_pcve_s01e02_activation", "rqa_eu_pcve_s01e02", "eu_pcve_s03e02",
                                            created_after_inclusive=isoparse("2022-09-01T00:00+03:00")),

                    # (Demographics use the same flow as seasons 1+2, with a new disability quesiton)
                    FlowResultConfiguration("csap_eu_pcve_demog", "location", "location"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "gender", "gender"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "age", "age"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "recently_displaced", "recently_displaced"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "hh_language", "household_language"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "disability", "disability"),
                ]
            )
        )
    ],
    csv_sources=[
        CSVSource(
            "gs://avf-project-datasets/2022/EU-PCVE-S03/recovered_hormuud_2022_09_de_identified.csv",
            engagement_db_datasets=[
                CSVDatasetConfiguration("eu_pcve_s03e01", start_date=isoparse("2022-09-04T00:00:00+03:00"), end_date=isoparse("2022-09-10T24:00:00+03:00")),
                CSVDatasetConfiguration("eu_pcve_s03e02", start_date=isoparse("2022-09-11T00:00:00+03:00"), end_date=isoparse("2022-09-17T24:00:00+03:00")),
                CSVDatasetConfiguration("eu_pcve_s03e03", start_date=isoparse("2022-09-18T00:00:00+03:00"), end_date=isoparse("2022-09-24T24:00:00+03:00")),
                CSVDatasetConfiguration("eu_pcve_s03_closeout", start_date=isoparse("2022-09-25T00:00:00+03:00"), end_date=isoparse("2022-09-30T24:00:00+03:00"))
            ],
            timezone="Africa/Mogadishu"
        )
    ],
    rapid_pro_target=RapidProTarget(
        rapid_pro=RapidProClientConfiguration(
            domain="textit.com",
            token_file_url="gs://avf-credentials/csap-text-it-token.txt"
        ),
        sync_config=EngagementDBToRapidProConfiguration(
            consent_withdrawn_dataset=DatasetConfiguration(
                engagement_db_datasets=[
                    "eu_pcve_s02e01", "eu_pcve_s02e02", "eu_pcve_s02e03", "eu_pcve_s02_closeout",
                    "eu_pcve_s03e01", "eu_pcve_s03e02", "eu_pcve_s03e03", "eu_pcve_s03_closeout",
                    "location", "age", "gender", "household_language", "recently_displaced", "disability"
                ],
                rapid_pro_contact_field=ContactField(
                    key="engagement_db_consent_withdrawn",
                    label="Engagement DB Consent Withdrawn"
                )
            ),
            write_mode=WriteModes.CONCATENATE_TEXTS,
            allow_clearing_fields=True,
            weekly_advert_contact_field=ContactField(
                key="eu_pcve_s03_weekly_advert_group",
                label="eu pcve weekly advert group"
            ),
            sync_advert_contacts=True,
        )
    ),
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-production.json"),
        sync_config=CodaSyncConfiguration(
            project_users_file_url="gs://avf-project-datasets/2021/EU-PCVE/s03/coda_users.json",
            dataset_configurations=[
                CodaDatasetConfiguration(
                    coda_dataset_id="EU_PCVE_rqa_s03e01",
                    engagement_db_dataset="eu_pcve_s03e01",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(
                            code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s03e01"),
                            auto_coder=None,
                            coda_code_schemes_count=3
                        )
                    ],
                    ws_code_match_value="eu_pcve_s03e01"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="EU_PCVE_rqa_s03e02",
                    engagement_db_dataset="eu_pcve_s03e02",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(
                            code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s03e02"),
                            auto_coder=None,
                            coda_code_schemes_count=3
                        )
                    ],
                    ws_code_match_value="eu_pcve_s03e02"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="EU_PCVE_rqa_s03e03",
                    engagement_db_dataset="eu_pcve_s03e03",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(
                            code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s03e03"),
                            auto_coder=None,
                            coda_code_schemes_count=3
                        )
                    ],
                    ws_code_match_value="eu_pcve_s03e03"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="EU_PCVE_rqa_s03_closeout",
                    engagement_db_dataset="eu_pcve_s03_closeout",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(
                            code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s03_closeout"),
                            auto_coder=None,
                            coda_code_schemes_count=3
                        )
                    ],
                    ws_code_match_value="eu_pcve_s03_closeout"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="CSAP_location",
                    engagement_db_dataset="location",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/mogadishu_sub_district"),
                                                auto_coder=somali.DemographicCleaner.clean_mogadishu_sub_district),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/somalia_district"),
                                                auto_coder=lambda text:
                                                somali.DemographicCleaner.clean_somalia_district(text)
                                                if somali.DemographicCleaner.clean_mogadishu_sub_district == Codes.NOT_CODED
                                                else Codes.NOT_CODED),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/somalia_region"),
                                                auto_coder=None),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/somalia_state"),
                                                auto_coder=None),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/somalia_zone"),
                                                auto_coder=None)
                    ],
                    ws_code_match_value="location"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="CSAP_age",
                    engagement_db_dataset="age",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/age"),
                                                auto_coder=lambda text: str(somali.DemographicCleaner.clean_age_within_range(text))),
                    ],
                    ws_code_match_value="age"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="CSAP_gender",
                    engagement_db_dataset="gender",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/gender"),
                                                auto_coder=somali.DemographicCleaner.clean_gender)
                    ],
                    ws_code_match_value="gender"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="CSAP_household_language",
                    engagement_db_dataset="household_language",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/household_language"),
                                                auto_coder=None)
                    ],
                    ws_code_match_value="household_language"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="CSAP_recently_displaced",
                    engagement_db_dataset="recently_displaced",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/recently_displaced"),
                                                auto_coder=None)
                    ],
                    ws_code_match_value="recently_displaced"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="CSAP_disability",
                    engagement_db_dataset="disability",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/disability"),
                                                auto_coder=None)
                    ],
                    ws_code_match_value="disability"
                ),
            ],
            set_dataset_from_ws_string_value=True,
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset")
        )
    ),
    test_participant_uuids=[
        "avf-participant-uuid-45d15c2d-623c-4f89-bd91-7518147bf1dc",
        "avf-participant-uuid-e8481c97-3797-40b8-a57f-8e396c5d3592",
        "avf-participant-uuid-57ba3ccc-a6a6-44c8-8a69-6f951e69a6a3",
        "avf-participant-uuid-dd8514ef-1b27-468b-8098-ec6a350cf9f4",
        "avf-participant-uuid-2d36746f-d200-4160-866f-8f1de7f64294",
        "avf-participant-uuid-48e91315-1f85-4a29-9e3e-b70bed3165a1",
        "avf-participant-uuid-b8c44fb8-7159-409c-a6fd-74ded79695ef",
        "avf-participant-uuid-045893fe-5cb0-4a6b-97b0-a3c68645cf71",
        "avf-participant-uuid-8251b21b-0db9-4bd5-b23a-afeb74ae16ae",
        "avf-participant-uuid-22c46bbe-5a87-49de-a0fc-ff30614b1321",
        "avf-participant-uuid-33c7a5d8-0481-4685-9d54-2e1f471a9d78",
        "avf-participant-uuid-522a793a-6080-4b49-bb99-147df70ef259",
        "avf-participant-uuid-6b88506f-a8e2-4bb7-8d27-9154acd2ae60"
    ],
    analysis=AnalysisConfiguration(
        google_drive_upload=GoogleDriveUploadConfiguration(
            credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json",
            drive_dir="eu_pcve_analysis_outputs/s03"
        ),
        dataset_configurations=[
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["eu_pcve_s03e01"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="eu_pcve_s03e01_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s03e01"),
                        analysis_dataset="s03e01"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["eu_pcve_s03e02"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="eu_pcve_s03e02_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s03e02"),
                        analysis_dataset="s03e02"
                    )
                ],
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["eu_pcve_s03e03"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="eu_pcve_s03e03_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s03e03"),
                        analysis_dataset="s03e03"
                    )
                ],
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["eu_pcve_s03_closeout"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="eu_pcve_s03_closeout_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s03_closeout"),
                        analysis_dataset="s03_closeout"
                    )
                ],
            ),
            OperatorDatasetConfiguration(
                raw_dataset="operator_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("csap_operator"),
                        analysis_dataset="operator",
                        analysis_location=AnalysisLocations.SOMALIA_OPERATOR
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["age"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="age_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/age"),
                        analysis_dataset="age"
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/age_category"),
                        analysis_dataset="age_category",
                        age_category_config=AgeCategoryConfiguration(
                            age_analysis_dataset="age",
                            categories={
                                (10, 14): "10 to 14",
                                (15, 17): "15 to 17",
                                (18, 35): "18 to 35",
                                (36, 54): "36 to 54",
                                (55, 99): "55 to 99"
                            }
                        )
                    ),
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["gender"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="gender_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/gender"),
                        analysis_dataset="gender"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["location"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="location_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/mogadishu_sub_district"),
                        analysis_dataset="mogadishu_sub_district",
                        analysis_location=AnalysisLocations.MOGADISHU_SUB_DISTRICT
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/somalia_district"),
                        analysis_dataset="district",
                        analysis_location=AnalysisLocations.SOMALIA_DISTRICT
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/somalia_region"),
                        analysis_dataset="region",
                        analysis_location=AnalysisLocations.SOMALIA_REGION
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/somalia_state"),
                        analysis_dataset="state",
                        analysis_location=AnalysisLocations.SOMALIA_STATE
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/somalia_zone"),
                        analysis_dataset="zone",
                        analysis_location=AnalysisLocations.SOMALIA_ZONE
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["recently_displaced"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="recently_displaced",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/recently_displaced"),
                        analysis_dataset="recently_displaced"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["household_language"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="household_language",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/household_language"),
                        analysis_dataset="household_language"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["disability"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="disability",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/disability"),
                        analysis_dataset="disability"
                    )
                ]
            ),
        ],
        ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2022/EU-PCVE-S03"
    )
)
