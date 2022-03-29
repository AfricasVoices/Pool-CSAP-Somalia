from core_data_modules.cleaners import somali
from core_data_modules.cleaners.codes import Codes
from dateutil.parser import isoparse
from datetime import timedelta

from src.pipeline_configuration_spec import *


def create_traffic_labels_configuration():
    """
    Creates the traffic label configurations for the promos.

    Promos run 3 times per day, 3 days a week (Sun-Tue), per station.

    :return:
    :rtype: list of TrafficLabel
    """

    # Lists radio station, time of day (morning/midday/evening), and scheduled promo start time.
    promo_schedule = [
        ("Radio Codka Bartamaha, Subax",  "06:59"),
        ("Radio Codka Bartamaha, Duhur",  "12:45"),
        ("Radio Codka Bartamaha, Habeen", "18:55"),

        ("Radio Hayaan, Subax",  "07:45"),
        ("Radio Hayaan, Duhur",  "12:45"),
        ("Radio Hayaan, Habeen", "18:44"),

        ("Radio Galgaduud, Subax",  "08:25"),
        ("Radio Galgaduud, Duhur",  "12:50"),
        ("Radio Galgaduud, Habeen", "19:20"),

        ("Radio Sooyaal Caabudwaaq, Subax",  "07:20"),
        ("Radio Sooyaal Caabudwaaq, Duhur",  "13:25"),
        ("Radio Sooyaal Caabudwaaq, Habeen", "19:25"),

        ("Radio Cadaado, Subax",  "07:59"),
        ("Radio Cadaado, Duhur",  "13:28"),
        ("Radio Cadaado, Habeen", "20:58"),

        ("Radio Daljir (Garowe), Subax",  "07:00"),
        ("Radio Daljir (Garowe), Duhur",  "12:20"),
        ("Radio Daljir (Garowe), Habeen", "16:57"),

        ("Radio Galkacyo, Subax",  "07:56"),
        ("Radio Galkacyo, Duhur",  "13:59"),
        ("Radio Galkacyo, Habeen", "18:45"),

        ("Radio Daljir (Bosaso), Subax",  "07:45"),
        ("Radio Daljir (Bosaso), Duhur",  "12:25"),
        ("Radio Daljir (Bosaso), Habeen", "20:50"),

        ("Radio Codka Nabada, Subax",  "06:58"),
        ("Radio Codka Nabada, Duhur",  "14:31"),
        ("Radio Codka Nabada, Habeen", "19:58"),

        ("Radio Galdogob, Subax",  "06:50"),
        ("Radio Galdogob, Duhur",  "12:50"),
        ("Radio Galdogob, Habeen", "19:55"),

        ("Radio Kismaayo, Subax",  "07:00"),
        ("Radio Kismaayo, Duhur",  "12:55"),
        ("Radio Kismaayo, Habeen", "19:45"),

        ("Star FM, Subax",  "06:55"),
        ("Star FM, Duhur",  "13:25"),
        ("Star FM, Habeen", "20:55"),

        ("Radio Gedo, Subax",  "07:25"),
        ("Radio Gedo, Duhur",  "13:15"),
        ("Radio Gedo, Habeen", "19:25"),

        ("Radio Markableey, Subax",  "07:30"),
        ("Radio Markableey, Duhur",  "13:38"),
        ("Radio Markableey, Habeen", "20:00"),

        ("Radio Xog-maal, Subax", "07:45"),
        ("Radio Xog-maal, Duhur", "11:45"),
        ("Radio Xog-maal, Habeen", "20:45"),
    ]

    traffic_labels = []
    for episode in range(1, 4):
        for promo in promo_schedule:
            for day in range(0, 3):
                # Name the label after the episode, day of week, and promo id.
                episode_name = f"s02e0{episode}"
                day_name = ["Sunday", "Monday", "Tuesday"][day]
                label = f"{episode_name}, {promo[0]}, {day_name}"

                # Compute the start time for this promo.
                # Promos run Sunday - Tuesday, for 3 weeks, at the same time every week, starting on March 20th.
                project_start_date = isoparse("2022-03-20T00:00+03:00")
                week_offset = timedelta(days=(episode - 1) * 7)
                day_offset = timedelta(days=day)
                parsed_promo_time = datetime.strptime(promo[1], "%H:%M")
                time_offset = timedelta(hours=parsed_promo_time.hour, minutes=parsed_promo_time.minute)
                start_date = project_start_date + week_offset + day_offset + time_offset

                # Based on analysis of the traffic dashboard, spikes around a promo are short.
                # Only count messages sent within 20 minutes of the promo in this estimate.
                end_date = start_date + timedelta(minutes=20)

                traffic_labels.append(TrafficLabel(start_date, end_date, label))

    return traffic_labels


PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="EU-PCVE-S02",
    description="Runs the EU-PCVE season 2 pipeline",
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
                    FlowResultConfiguration("csap_eu_pcve_s02e01_activation", "rqa_eu_pcve_s02e01", "eu_pcve_s02e01"),
                    FlowResultConfiguration("csap_eu_pcve_s02e02_activation", "rqa_eu_pcve_s02e02", "eu_pcve_s02e02"),

                    # (Demographics use the same flow as season 1)
                    FlowResultConfiguration("csap_eu_pcve_demog", "location", "location"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "gender", "gender"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "age", "age"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "recently_displaced", "recently_displaced"),
                    FlowResultConfiguration("csap_eu_pcve_demog", "hh_language", "household_language"),
                ]
            )
        )
    ],
    csv_sources=[
        CSVSource(
            "gs://avf-project-datasets/2022/EU-PCVE-S02/recovered_hormuud_2022_03_20_to_2022_03_22_de_identified.csv",
            engagement_db_datasets=[
                CSVDatasetConfiguration("eu_pcve_s02e01", start_date=isoparse("2022-03-20T00:00:00+03:00"), end_date=isoparse("2022-03-22T13:30:00+03:00"))
            ],
            timezone="Africa/Mogadishu"
        ),
        CSVSource(
            "gs://avf-project-datasets/2022/EU-PCVE-S02/recovered_hormuud_2022_03_22_to_2022_03_25_de_identified.csv",
            engagement_db_datasets=[
                CSVDatasetConfiguration("eu_pcve_s02e01", start_date=isoparse("2022-03-22T13:30:00+03:00"), end_date=isoparse("2022-03-25T24:00:00+03:00"))
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
                    coda_dataset_id="EU_PCVE_rqa_s02e01",
                    engagement_db_dataset="eu_pcve_s02e01",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s02e01"), auto_coder=None)
                    ],
                    ws_code_string_value="eu_pcve_s02e01"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="EU_PCVE_rqa_s02e02",
                    engagement_db_dataset="eu_pcve_s02e02",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s02e02"), auto_coder=None)
                    ],
                    ws_code_string_value="eu_pcve_s02e02"
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
            ],
            set_dataset_from_ws_string_value=True,
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset")
        )
    ),
    analysis=AnalysisConfiguration(
        google_drive_upload=GoogleDriveUploadConfiguration(
            credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json",
            drive_dir="eu_pcve_analysis_outputs/s02"
        ),
        dataset_configurations=[
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["eu_pcve_s02e01"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="eu_pcve_s02e01_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s02e01"),
                        analysis_dataset="s02e01"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["eu_pcve_s02e02"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="eu_pcve_s02e02_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/eu_pcve/eu_pcve_s02e02"),
                        analysis_dataset="s02e02"
                    )
                ]
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
        ],
        ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
        traffic_labels=create_traffic_labels_configuration()
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2022/EU-PCVE-S02"
    )
)
