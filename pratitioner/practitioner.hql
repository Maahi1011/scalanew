DROP TABLE if exists pubz_datalake_cnfd_prv.practitioner;

CREATE EXTERNAL TABLE pubz_datalake_cnfd_prv.practitioner (
   practitioner_id string,
   practitioner_resource_id string,
   npi_nbr string,
   first_nme string,
   preferred_first_nme string,
   middle_nme string,
   last_nme string,
   name_prefix_txt string,
   name_professional_suffixes_txt array<string>,
   name_generational_suffix_txt string,
   birth_dte date,
   death_dte date,
   gender_dsc string,
   status_dsc string,
   status_effective_tms timestamp,
   status_effective_tms_txt string,
   status_effective_timezone_txt string,
   status_expiration_tms timestamp,
   status_expiration_tms_txt string,
   status_expiration_timezone_txt string,
   practitioner_type_cde string,
   practitioner_type_dsc string,
   practitioner_category_cde string,
   practitioner_category_dsc string,
   concierge_ind boolean,
   specialties_txt array<string>,
   tenant_id decimal(18,0),
   data_lake_last_update_tms timestamp,
   npi_agency_dsc string,
   npi_status_dsc string,
   npi_status_effective_tms timestamp,
   npi_status_effective_tms_txt string,
   npi_status_effective_timezone_txt string,
   npi_status_expiration_tms timestamp,
   npi_status_expiration_tms_txt string,
   npi_status_expiration_timezone_txt string,
   medical_education_nbr string,
   medical_education_nbr_status string,
   medical_education_nbr_status_effective_tms timestamp,
   medical_education_nbr_status_effective_tms_txt string,
   medical_education_nbr_status_effective_timezone_txt string,
   medical_education_nbr_status_expiration_tms timestamp,
   medical_education_nbr_status_expiration_tms_txt string,
   medical_education_nbr_status_expiration_timezone_txt string,
   spi_id string,
   spi_id_agency_dsc string,
   spi_id_status_dsc string,
   spi_id_status_effective_tms timestamp,
   spi_id_status_effective_tms_txt string,
   spi_id_status_effective_timezone_txt string,
   spi_id_status_expiration_tms timestamp,
   spi_id_status_expiration_tms_txt string,
   spi_id_status_expiration_timezone_txt string,
   military_id string,
   military_id_agency_dsc string,
   military_id_status_dsc string,
   military_id_status_effective_tms timestamp,
   military_id_status_effective_tms_txt string,
   military_id_status_effective_timezone_txt string,
   military_id_status_expiration_tms timestamp,
   military_id_status_expiration_tms_txt string,
   military_id_status_expiration_timezone_txt string,
   languages array<struct< language_cde:string, language_nme:string >>,
   store_keys array<struct< store_nme:string, key_nme:string, primary_store_ind:boolean, component_nme:string, component_value_txt:string, status_dsc:string, status_effective_tms:timestamp, status_effective_tms_txt:string, status_effective_timezone_txt:string, status_expiration_tms:timestamp, status_expiration_tms_txt:string, status_expiration_timezone_txt:string >>,
   dea_identifiers array<struct< dea_registration_nbr:string, issuing_state_cde:string, issuing_location_dsc:string, agency_dsc:string, postal_address_id:string, status_dsc:string, status_effective_tms:timestamp, status_effective_tms_txt:string, status_effective_timezone_txt:string, status_expiration_tms:timestamp, status_expiration_tms_txt:string, status_expiration_timezone_txt:string, dea_authorizations:array<struct< schedule_1_ind:boolean, schedule_2_ind:boolean, schedule_2n_ind:boolean, schedule_3_ind:boolean, schedule_3n_ind:boolean, schedule_4_ind:boolean, schedule_5_ind:boolean, list_1_ind:boolean, authorization_status_dsc:string, authorization_status_effective_tms:timestamp, authorization_status_effective_tms_txt:string, authorization_status_effective_timezone_txt:string, authorization_status_expiration_tms:timestamp, authorization_status_expiration_tms_txt:string, authorization_status_expiration_timezone_txt:string >> >>,
   state_license_identifiers array<struct< state_license_id:string, issuing_state_cde:string, agency_dsc:string, issuing_state_nme:string, issuing_state_dsc:string, status_dsc:string, status_effective_tms:timestamp, status_effective_tms_txt:string, status_effective_timezone_txt:string, status_expiration_tms:timestamp, status_expiration_tms_txt:string, status_expiration_timezone_txt:string >>,
   medicaid_identifiers array<struct< medicaid_provider_nbr:string, issuing_state_cde:string, issuing_state_dsc:string, agency_dsc:string, status_dsc:string, status_effective_tms:timestamp, status_effective_tms_txt:string, status_effective_timezone_txt:string, status_expiration_tms:timestamp, status_expiration_tms_txt:string, status_expiration_timezone_txt:string >>,
   postal_addresses array<struct< postal_address_relative_id:string, street_address_lines_txt:array<string>, office_suite_txt:string, apartment_number_txt:string, post_office_box_txt:string, postal_cde:string, city_nme:string, state_cde:string, state_nme:string, state_dsc:string, country_iso_alpha2_cde:string, country_iso_alpha3_cde:string, country_nme:string, country_dsc:string, status_dsc:string, status_effective_tms:timestamp, status_effective_tms_txt:string, status_effective_timezone_txt:string, status_expiration_tms:timestamp, status_expiration_tms_txt:string, status_expiration_timezone_txt:string, temporary_effective_tms:timestamp, temporary_effective_tms_txt:string, temporary_effective_timezone_txt:string, temporary_expiration_tms:timestamp, temporary_expiration_tms_txt:string, temporary_expiration_timezone_txt:string, temporary_note_txt:string, unreachable_ind:boolean, unreachable_effective_tms:timestamp, unreachable_effective_tms_txt:string, unreachable_effective_timezone_txt:string, unreachable_note_txt:string, unverified_ind:boolean, unverified_effective_tms:timestamp, unverified_effective_tms_txt:string, unverified_effective_timezone_txt:string, unverified_note_txt:string, sourced_from_source_dsc:string, sourced_from_source_type_dsc:string, sourced_from_method_dsc:string >>,
   email_addresses array<struct< email_address_txt:string, email_address_relative_id:string, status_dsc:string, status_effective_tms:timestamp, status_effective_tms_txt:string, status_effective_timezone_txt:string, status_expiration_tms:timestamp, status_expiration_tms_txt:string, status_expiration_timezone_txt:string, unreachable_ind:boolean, unreachable_effective_tms:timestamp, unreachable_effective_tms_txt:string, unreachable_effective_timezone_txt:string, unreachable_note_txt:string, unverified_ind:boolean, unverified_effective_tms:timestamp, unverified_effective_tms_txt:string, unverified_effective_timezone_txt:string, unverified_note_txt:string >>,
   phone_numbers array<struct< country_calling_cde:string, phone_number_txt:string, phone_number_extension_txt:string, phone_relative_id:string, fax_capability_ind:boolean, voice_capability_ind:boolean, text_capability_ind:boolean, status_dsc:string, status_effective_tms:timestamp, status_effective_tms_txt:string, status_effective_timezone_txt:string, status_expiration_tms:timestamp, status_expiration_tms_txt:string, status_expiration_timezone_txt:string, unreachable_ind:boolean, unreachable_effective_tms:timestamp, unreachable_effective_tms_txt:string, unreachable_effective_timezone_txt:string, unreachable_note_txt:string, unverified_ind:boolean, unverified_effective_tms:timestamp, unverified_effective_tms_txt:timestamp, unverified_effective_timezone_txt:string, unverified_note_txt:string, practitioner_phone_postal_addresses:array<string> >>,
   taxonomy array<struct< taxonomy_cde:string, taxonomy_category_dsc:string, primary_ind:boolean, taxonomy_relative_id:string, taxonomy_type_dsc:string, specialization_dsc:string, status_dsc:string, status_effective_tms:timestamp, status_effective_tms_txt:string, status_effective_timezone_txt:string, status_expiration_tms:timestamp, status_expiration_tms_txt:string, status_expiration_timezone_txt:string, sourced_from_source_dsc:string, sourced_from_source_type_dsc:string, sourced_from_method_dsc:string >>
 )
 STORED AS ORC
 LOCATION '/datahubez/pubz/datalake/published/conformed_prv/pubz_datalake_cnfd_prv.db/practitioner';
