# pylint: disable=logging-fstring-interpolation
"""Module for mapping NPDL data."""
from uuid import uuid4
import logging
import pyspark.sql.functions as f

from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from loader.transforms import \
    hodh_class_of_disposal, hodh_class_of_event, hodh_class_of_location, hodh_disposal, \
    hodh_event_inv_aggravating_factor, hodh_event_inv_location, hodh_location, \
    hodh_material_item, hodh_offence_detail, hodh_organisation, hodh_person_relation, \
    hodh_person, hodh_role_played, \
    homi_homicidecase, homi_homicidesuspect, \
    nas_apnr, \
    pnc_nphord_bailconditions, pnc_nphord_caseheader, pnc_nphord_breachofbail, \
    pnc_nphord_condition, pnc_nphord_cooffender, pnc_nphord_custody, pnc_nphord_disposal, \
    pnc_nphord_informationmarker, pnc_nphord_method, pnc_nphord_mokeywordlocation, \
    pnc_nphord_mokeywordvictim, pnc_nphord_nominal, pnc_nphord_occupation, \
    pnc_nphord_offence, pnc_nphord_oiconditions, pnc_nphord_operationalinformation, \
    pnc_nphord_periodininstitution, pnc_nphord_releasedetails, pnc_nphord_subsequentappearance, \
    visor_address_other, visor_address, visor_arrest, visor_charge, visor_conviction_comment, \
    visor_conviction, visor_custody, visor_employer, visor_employment_rel_map, \
    visor_financial_account, visor_financial_card, visor_firearm, visor_license, \
    visor_management_plan_approval_comment, visor_management_plan_detail, \
    visor_management_plan_risk_level_rationale, visor_management_plan, visor_missing_nominal_action, \
    visor_missing_nominal_closure, visor_missing_nominal_comment, visor_missing_nominal, \
    visor_modus_operandi_freetext, visor_modus_operandi_keyword, visor_modus_operandi, \
    visor_name, visor_nominal_address_map, visor_nominal_flag, visor_nominal_relationship, \
    visor_nominal, visor_offence, visor_order_breach, visor_order_condition_comment, \
    visor_order_condition, visor_order_detail_nco_decision, visor_order_detail, \
    visor_other_transport, visor_personal_computer, visor_personal_date, visor_personal_description, \
    visor_personal_detail, visor_personal_email, visor_personal_telephone, visor_relationship, \
    visor_static_condition, visor_static_data, visor_static_force_location, visor_static_mo_text, \
    visor_static_pnc_conviction_data, visor_static_sentence, visor_travel_journey_accompany, \
    visor_travel_journey_leg, visor_travel_journey, visor_vehicle_relationship_map, \
    visor_vehicle, visor_wanted_nominal_action, visor_wanted_nominal_comment, \
    visor_wanted_nominal_police_power, visor_wanted_nominal_resolution, \
    visor_wanted_nominal_warrant_detail, visor_wanted_nominal


class Datasource:
    """
    Base Datasource class.

    Provides interface and default implementation for
    data processing from NPDL to Redshift NDPL

    Methods
    -------
    transform(delta_frames, spark)
        Transform `delta_frame` data for Redshift based on `spark` instance

    relationalize(root, delta_frames, glue_context, spark)
        Split `delta_frame` into separate df's starting at
         `root` for the `glue context` and `spark` instance

    """

    def __init__(self, spark, types, args):
        """
        Construct Datasource.

        Parameters
        ----------
        types : list
            List of NPDL types this datasource handles

        """
        self.logger = logging.getLogger('loader.Datasource')
        self.logger.info(f'Datasource [{types}]')
        self.types = types
        self._spark = spark
        self._args = args

    def transform(self, delta_frames):
        """
        Transform data frames for loading to Redshift.

        Datasources may override this method where required.

        Parameters
        ----------
        delta_frames : dict
            Transformer delta_frames
        """
        self.logger.info('transform')

        # can be used for checking if a transform returns sql
        #delta_frames['read'] = 'hodh_disposal'

        root = self._args['RELATIONALIZE_ROOT_ARG']
        glue_job_run_id = self._args['JOB_RUN_ID']
        for entity in self.types:
            if entity in delta_frames['read']:
                self.logger.info(f'transform: {entity} for glue_job_run_id: {glue_job_run_id}')
                # used to tell Spark SQL whether to apply the CDLZ header
                header_req = True

                flattened = False

                transformers = {
                    'nas_anpr': nas_apnr.sql(glue_job_run_id),
                    'visor_address': visor_address.sql(glue_job_run_id),
                    'visor_address_other': visor_address_other.sql(glue_job_run_id),
                    'visor_arrest': visor_arrest.sql(glue_job_run_id),
                    'visor_charge': visor_charge.sql(glue_job_run_id),
                    'visor_conviction': visor_conviction.sql(glue_job_run_id),
                    'visor_conviction_comment': visor_conviction_comment.sql(glue_job_run_id),
                    'visor_custody': visor_custody.sql(glue_job_run_id),
                    'visor_employer': visor_employer.sql(glue_job_run_id),
                    'visor_employment_rel_map': visor_employment_rel_map.sql(glue_job_run_id),
                    'visor_financial_account': visor_financial_account.sql(glue_job_run_id),
                    'visor_financial_card': visor_financial_card.sql(glue_job_run_id),
                    'visor_firearm': visor_firearm.sql(glue_job_run_id),
                    'visor_license': visor_license.sql(glue_job_run_id),
                    'visor_management_plan': visor_management_plan.sql(glue_job_run_id),
                    'visor_management_plan_approval_comment': visor_management_plan_approval_comment.sql(glue_job_run_id),
                    'visor_management_plan_detail': visor_management_plan_detail.sql(glue_job_run_id),
                    'visor_management_plan_risk_level_rationale': visor_management_plan_risk_level_rationale.sql(glue_job_run_id),
                    'visor_missing_nominal': visor_missing_nominal.sql(glue_job_run_id),
                    'visor_missing_nominal_action': visor_missing_nominal_action.sql(glue_job_run_id),
                    'visor_missing_nominal_closure': visor_missing_nominal_closure.sql(glue_job_run_id),
                    'visor_missing_nominal_comment': visor_missing_nominal_comment.sql(glue_job_run_id),
                    'visor_modus_operandi': visor_modus_operandi.sql(glue_job_run_id),
                    'visor_modus_operandi_freetext': visor_modus_operandi_freetext.sql(glue_job_run_id),
                    'visor_modus_operandi_keyword': visor_modus_operandi_keyword.sql(glue_job_run_id),
                    'visor_name': visor_name.sql(glue_job_run_id),
                    'visor_nominal': visor_nominal.sql(glue_job_run_id),
                    'visor_nominal_address_map': visor_nominal_address_map.sql(glue_job_run_id),
                    'visor_nominal_flag': visor_nominal_flag.sql(glue_job_run_id),
                    'visor_nominal_relationship': visor_nominal_relationship.sql(glue_job_run_id),
                    'visor_offence': visor_offence.sql(glue_job_run_id),
                    'visor_order_breach': visor_order_breach.sql(glue_job_run_id),
                    'visor_order_condition': visor_order_condition.sql(glue_job_run_id),
                    'visor_order_condition_comment': visor_order_condition_comment.sql(glue_job_run_id),
                    'visor_order_detail': visor_order_detail.sql(glue_job_run_id),
                    'visor_order_detail_nco_decision': visor_order_detail_nco_decision.sql(glue_job_run_id),
                    'visor_other_transport': visor_other_transport.sql(glue_job_run_id),
                    'visor_personal_computer': visor_personal_computer.sql(glue_job_run_id),
                    'visor_personal_date': visor_personal_date.sql(glue_job_run_id),
                    'visor_personal_description': visor_personal_description.sql(glue_job_run_id),
                    'visor_personal_detail': visor_personal_detail.sql(glue_job_run_id),
                    'visor_personal_email': visor_personal_email.sql(glue_job_run_id),
                    'visor_personal_telephone': visor_personal_telephone.sql(glue_job_run_id),
                    'visor_relationship': visor_relationship.sql(glue_job_run_id),
                    'visor_static_condition': visor_static_condition.sql(glue_job_run_id),
                    'visor_static_data': visor_static_data.sql(glue_job_run_id),
                    'visor_static_force_location': visor_static_force_location.sql(glue_job_run_id),
                    'visor_static_mo_text': visor_static_mo_text.sql(glue_job_run_id),
                    'visor_static_pnc_conviction_data': visor_static_pnc_conviction_data.sql(glue_job_run_id),
                    'visor_static_sentence': visor_static_sentence.sql(glue_job_run_id),
                    'visor_travel_journey': visor_travel_journey.sql(glue_job_run_id),
                    'visor_travel_journey_accompany': visor_travel_journey_accompany.sql(glue_job_run_id),
                    'visor_travel_journey_leg': visor_travel_journey_leg.sql(glue_job_run_id),
                    'visor_vehicle': visor_vehicle.sql(glue_job_run_id),
                    'visor_vehicle_relationship_map': visor_vehicle_relationship_map.sql(glue_job_run_id),
                    'visor_wanted_nominal': visor_wanted_nominal.sql(glue_job_run_id),
                    'visor_wanted_nominal_action': visor_wanted_nominal_action.sql(glue_job_run_id),
                    'visor_wanted_nominal_comment': visor_wanted_nominal_comment.sql(glue_job_run_id),
                    'visor_wanted_nominal_police_power': visor_wanted_nominal_police_power.sql(glue_job_run_id),
                    'visor_wanted_nominal_resolution': visor_wanted_nominal_resolution.sql(glue_job_run_id),
                    'visor_wanted_nominal_warrant_detail': visor_wanted_nominal_warrant_detail.sql(glue_job_run_id),
                    'pnc_nphord_nominal': pnc_nphord_nominal.sql(glue_job_run_id),
                    'pnc_nphord_occupation': pnc_nphord_occupation.sql(glue_job_run_id),
                    'pnc_nphord_informationmarker': pnc_nphord_informationmarker.sql(glue_job_run_id),
                    'pnc_nphord_caseheader': pnc_nphord_caseheader.sql(glue_job_run_id),
                    'pnc_nphord_offence': pnc_nphord_offence.sql(glue_job_run_id),
                    'pnc_nphord_disposal': pnc_nphord_disposal.sql(glue_job_run_id),
                    'pnc_nphord_subsequentappearance': pnc_nphord_subsequentappearance.sql(glue_job_run_id),
                    'pnc_nphord_cooffender': pnc_nphord_cooffender.sql(glue_job_run_id),
                    'pnc_nphord_condition': pnc_nphord_condition.sql(glue_job_run_id),
                    'pnc_nphord_method': pnc_nphord_method.sql(glue_job_run_id),
                    'pnc_nphord_mokeywordlocation': pnc_nphord_mokeywordlocation.sql(glue_job_run_id),
                    'pnc_nphord_mokeywordvictim': pnc_nphord_mokeywordvictim.sql(glue_job_run_id),
                    'pnc_nphord_custody': pnc_nphord_custody.sql(glue_job_run_id),
                    'pnc_nphord_periodininstitution': pnc_nphord_periodininstitution.sql(glue_job_run_id),
                    'pnc_nphord_releasedetails': pnc_nphord_releasedetails.sql(glue_job_run_id),
                    'pnc_nphord_bailconditions': pnc_nphord_bailconditions.sql(glue_job_run_id),
                    'pnc_nphord_breachofbail': pnc_nphord_breachofbail.sql(glue_job_run_id),
                    'pnc_nphord_operationalinformation': pnc_nphord_operationalinformation.sql(glue_job_run_id),
                    'pnc_nphord_oiconditions': pnc_nphord_oiconditions.sql(glue_job_run_id),
                    
                    'pncnew_nphord_nominal': pnc_nphord_nominal.sql(glue_job_run_id),
                    'pncnew_nphord_occupation': pnc_nphord_occupation.sql(glue_job_run_id),
                    'pncnew_nphord_informationmarker': pnc_nphord_informationmarker.sql(glue_job_run_id),
                    'pncnew_nphord_caseheader': pnc_nphord_caseheader.sql(glue_job_run_id),
                    'pncnew_nphord_offence': pnc_nphord_offence.sql(glue_job_run_id),
                    'pncnew_nphord_disposal': pnc_nphord_disposal.sql(glue_job_run_id),
                    'pncnew_nphord_subsequentappearance': pnc_nphord_subsequentappearance.sql(glue_job_run_id),
                    'pncnew_nphord_cooffender': pnc_nphord_cooffender.sql(glue_job_run_id),
                    'pncnew_nphord_condition': pnc_nphord_condition.sql(glue_job_run_id),
                    'pncnew_nphord_method': pnc_nphord_method.sql(glue_job_run_id),
                    'pncnew_nphord_mokeywordlocation': pnc_nphord_mokeywordlocation.sql(glue_job_run_id),
                    'pncnew_nphord_mokeywordvictim': pnc_nphord_mokeywordvictim.sql(glue_job_run_id),
                    'pncnew_nphord_custody': pnc_nphord_custody.sql(glue_job_run_id),
                    'pncnew_nphord_periodininstitution': pnc_nphord_periodininstitution.sql(glue_job_run_id),
                    'pncnew_nphord_releasedetails': pnc_nphord_releasedetails.sql(glue_job_run_id),
                    'pncnew_nphord_bailconditions': pnc_nphord_bailconditions.sql(glue_job_run_id),
                    'pncnew_nphord_breachofbail': pnc_nphord_breachofbail.sql(glue_job_run_id),
                    'pncnew_nphord_operationalinformation': pnc_nphord_operationalinformation.sql(glue_job_run_id),
                    'pncnew_nphord_oiconditions': pnc_nphord_oiconditions.sql(glue_job_run_id),

                    'homi_homicidecase': homi_homicidecase.sql(glue_job_run_id),
                    'homi_homicidesuspect': homi_homicidesuspect.sql(glue_job_run_id),
                    'hodh_class_of_disposal': hodh_class_of_disposal.sql(glue_job_run_id),
                    'hodh_class_of_event': hodh_class_of_event.sql(glue_job_run_id),
                    'hodh_class_of_location': hodh_class_of_location.sql(glue_job_run_id),
                    'hodh_disposal': hodh_disposal.sql(glue_job_run_id),
                    'hodh_event_inv_aggravating_factor': hodh_event_inv_aggravating_factor.sql(glue_job_run_id),
                    'hodh_event_inv_location': hodh_event_inv_location.sql(glue_job_run_id),
                    'hodh_location': hodh_location.sql(glue_job_run_id),
                    'hodh_material_item': hodh_material_item.sql(glue_job_run_id),
                    'hodh_offence_detail': hodh_offence_detail.sql(glue_job_run_id),
                    'hodh_organisation': hodh_organisation.sql(glue_job_run_id),
                    'hodh_person_relation': hodh_person_relation.sql(glue_job_run_id),
                    'hodh_person': hodh_person.sql(glue_job_run_id),
                    'hodh_role_played': hodh_role_played.sql(glue_job_run_id),
                }

                # data was flattened, so transform each key
                if delta_frames.get('relationalize'):
                    for keyname in delta_frames['relationalize'][entity]['df']:

                        self.logger.info(f'relationalize keys [{keyname}]')
                        # used to tell Spark SQL whether to apply the CDLZ header
                        header_req = bool(root.lower() == keyname)

                        flattened = True

                        # delta_frames['relationalize'][entity]['df'][keyname].printSchema()
                        delta_frames['relationalize'][entity]['df'][keyname].createOrReplaceTempView(keyname)
                        #delta_frames['relationalize'][entity][keyname] = transformers[f'{entity}_{keyname}'](keyname, header_req, glue_job_run_id, self._spark)
                        delta_frames['relationalize'][entity][keyname] = self._spark.sql(
                            self._add_cdlz_header_to_sql(
                                header_req,
                                flattened,
                                keyname,
                                transformers[f'{entity}_{keyname}']
                            )
                        )
                        # self.logger.debug('Relationalized df count after transform:')
                        # self.logger.debug(delta_frames['relationalize'][entity][keyname].count())

                # run transform
                else:
                    # delta_frames['read'][entity] = transformers[entity](entity, header_req, glue_job_run_id, self._spark)
                    delta_frames['read'][entity] = self._spark.sql(
                        self._add_cdlz_header_to_sql(
                            header_req,
                            flattened,
                            entity,
                            transformers[entity]
                        )
                    )

                self.logger.debug('Transformed count:')
                self.logger.debug(delta_frames['read'][entity].count())

    def relationalize(self, delta_frames, glue_context):
        """
        Flatten nested formats.
        Root is the top-level entity of the nested format.
        Note that the CDLZ header is only applied to the root.

        Datasources may override this method where required.

        Parameters
        ----------
        root : string
            The top-level construct of the nested schema
        delta_frames : dict
            Relationalize delta_frames

        glue_context : AWS Glue context
        """
        self.logger.info('relationalize')

        root = self._args['RELATIONALIZE_ROOT_ARG']
        self.logger.info(f'printing root [{root}]')

        # for testing
        if delta_frames.get('read'):
            root = self._args['RELATIONALIZE_ROOT_ARG']

            for entity in self.types:
                
                self.logger.info(f'inside entity types [{entity}]')
    
                if entity in delta_frames['read']:
    
                    self.logger.info('Attempting to read delta_frames')
                    delta_frames['read'][entity].printSchema()
                    delta_frames['read'][entity].show()
    
                    delta_frames['relationalize'][entity] = {}
                    delta_frames['relationalize'][entity]['dyfc'] = {}
                    dyn = DynamicFrame.fromDF(delta_frames['read'][entity], glue_context, "dyn")
    
                    # Flatten and store as DynamicFrameCollection
                    delta_frames['relationalize'][entity]['dyfc'] = Relationalize.apply(frame=dyn, name=root)
                    # Stores the individual dfs from the DynamicFrameCollection
                    delta_frames['relationalize'][entity]['df'] = {}
    
                    self.logger.info(delta_frames['relationalize'][entity]['dyfc'].keys())
                    # pre-process each identified key prior to transform and load
                    for key in delta_frames['relationalize'][entity]['dyfc'].keys():
                        
                        self.logger.info('iterate delta_frames keys')
                        # normalise the names returned by the Relationalize function to match Redshift tables
                        keyname = key.split('.')[-1].lower()
                        self.logger.info(f'Relationalize Key: {keyname}')
    
                        # convert back to dataframe as dynamicframes do not allow sql
                        # print(delta_frames['relationalize'][entity]['dyfc'].select(key).schema())
                        df = delta_frames['relationalize'][entity]['dyfc'].select(key).toDF()
                        
                        #to_df.show()
                        #df = to_df.dropDuplicates()
                        #self.logger.info('Dropping duplicates before')
                        #df.show()
                        self.logger.debug('Relationalized df count:')
                        self.logger.debug(df.count())
    
                        # remove prefixes from column names
                        cols = df.columns
                        self.logger.info(cols)
                        df = df.toDF(*(col.split('.')[-1] for col in cols))
    
                        # Relationalize removes nulls fields, so add them back in
                        col_list = self._relationalize_null_cols(f'{entity}_{keyname}')
                        self.logger.info(col_list)
    
                        select_cols = df.columns + [f.lit(None).alias(c) for c in col_list if c not in df.columns]
                        self.logger.info(select_cols)
                        df = df.select(*select_cols)
    
                        # add uuid to the root
                        if root.lower() == keyname:
                            root_uuid = str(uuid4())
                            self.logger.info(root_uuid)
                        
                        df.show(truncate=False)                      
                        df = df.withColumn('root_uuid', f.lit(root_uuid))
                        df.show(truncate=False)
                        
                        delta_frames['relationalize'][entity]['df'][keyname] = df
    
                    return delta_frames
                    
                else:
                    self.logger.warning('relationalize called without read frames')


    def _relationalize_null_cols(self, key):
        """
        AWS Relationalize will remove cols with no data.
        For the transform SQL to work all cols must exist.
        This specifies the complete list of cols in each keyname
        including the id cols that Relationalize adds.

        Parameters
        ----------
        keyname : string
            The Key as identified in the relationalize process.

        Returns
        -------
        List of cols for the feed.
        """
        self.logger.info('_relationalize_null_cols')

        cols = {
            'pnc_nphord_nominal': [
                "PNCID", "CRONo", "PNCFilename",
                "Sex", "DateOfBirth", "EthnicAppearance",
                "Nationality1", "Nationality2",
                "Nationality3", "BRCStatus",
                "OffencesOnBailCount", "Occupation",
                "InformationMarker", "CaseHeader",
                "OperationalInformation"],
            'pnc_nphord_caseheader': [
                "CaseType", "CautionType", "CourtCode",
                "ResultDate", "OffenderAddField1",
                "OffenderAddField2", "OffenderAddField3",
                "OffenderAddField4", "OffenderAddField5",
                "OffenderAddPcode", "OffenderAddFscode",
                "CourtName", "OtherTIC", "CourtCaseRef",
                "Offence", "Condition", "Method",
                "Custody", "BailConditions", "BreachOfBail"],
            'pnc_nphord_occupation': [
                "OccupationCode", "OccupationDesc",
                "OccupationDate"],
            'pnc_nphord_informationmarker': [
                "IMCode", "IMLiteral", "IMDate",
                "IMFreeText", "IMFSRef"],
            'pnc_nphord_offence': [
                "OffenceACPO", "OffenceCCCJS", "OffenceQual1",
                "OffenceQual2", "OffenceStartDate",
                "OffenceEndDate", "OffenceAddF1",
                "OffenceAddF2", "OffenceAddF3",
                "OffenceAddF4", "OffenceAddF5",
                "OffenceAddPcode", "OffenceAddFree",
                "OffenceAddFscode", "ProcessStage",
                "StageProcessDate", "ForceBringingCharges",
                "CommittedOnBail", "Plea",
                "Adjudication", "OffenceText",
                "OffenceTIC", "ArrestSummonsRef",
                "CrimeOffenceRef", "OffenceStatusMarker",
                "CautionType", "Disposal", "SubsequentAppearance",
                "Cooffender"],
            'pnc_nphord_disposal': [
                "DisposalType", "DisposalDuration",
                "DisposalAmount", "DisposalDate",
                "FineUnits", "DisposalQualifiers",
                "QualifierDuration", "DisposalText"],
            'pnc_nphord_subsequentappearance': [
                "SAPReason", "SAPDate",
                "SAPCourtCode", "SAPCourtName"],
            'pnc_nphord_cooffender': [
                "PNCID", "Checkname"],
            'pnc_nphord_condition': [
                "ConditionText", "EndDate",
                "CompletionDate"],
            'pnc_nphord_method': [
                "CrimeOffenceRef", "MethodUsedInOffence",
                "MOKeywordLocation", "MOKeywordVictim"],
            'pnc_nphord_mokeywordlocation': [
                "MOKeyLocationCode", "MOKeyLocationDesc"],
            'pnc_nphord_mokeywordvictim': [
                "MOKeyVictimCode", "MOKeyVictimDesc"],
            'pnc_nphord_custody': [
                "CourtCaseRef", "ArrestSummonsRef",
                "CustodyDate", "PeriodInInstitution"],
            'pnc_nphord_periodininstitution': [
                "InstitutionPrisonCode", "InstitutionPrisonName",
                "PrisonCustodyFscode", "DateOfDetention",
                "EndOfDetentionDate", "PrisonerNumber",
                "ReleaseDetails"],
            'pnc_nphord_releasedetails': [
                "ReleaseDate", "ReleaseReasonCode",
                "ReleaseReasonFullDesc", "SentenceExpiryDate",
                "ParoleRefNo", "LicenseParoleTypeCode",
                "LicenseParoleTypeDesc", "LicenseConditions",
                "SupervisionStartDate", "SupervisionEndDate"],
            'pnc_nphord_bailconditions': [
                "ArrestSummonsRef", "RemandDate"],
            'pnc_nphord_breachofbail': [
                "ArrestSummonsRef", "RemandDate",
                "BailBreachDate", "BailBreachCondsDate",
                "BailBreachNature"],
            'pnc_nphord_operationalinformation': [
                "OIReptClass", "OIReptType", "OIFSRef",
                "OICourtLocation", "ArrestSummonsRef",
                "OIReason", "OIStartDate", "OIEndDate",
                "OIText", "OINotifAddF1", "OINotifAddF2",
                "OINotifAddF3", "OINotifAddF4",
                "OINotifAddF5", "OINotifAddPcode",
                "OIConditions"],
            'pnc_nphord_oiconditions': [
                "OICondition", "OICondAddF1",
                "OICondAddF2", "OICondAddF3",
                "OICondAddF4", "OICondAddF5",
                "OICondAddPcode"]
        }

        return cols[key]

    def _cdlz_header(self, flattened):
        """
        Adds the CDLZ header fields onto the SQL.

        Default behaviour

        Parameters
        ----------
        flattened: True or False
            Whether the record came from the source Avro or
            has beeen flattened by Relationalize

        delta_frames : dict
            Transformer delta_frames

        Returns
        -------
        sql
        """

        self.logger.info(f'_cdlz_header. Flattened: {flattened}')

        if flattened:
            sql_text = """
                BASE64(recordHash) AS cdlz_header_record_hash_b64
                , dataFeedId AS cdlz_header_data_feed_id
                , collectDataFeedTaskId AS cdlz_header_collect_data_feed_task_id
                , ingestDataFeedTaskId AS cdlz_header_ingest_data_feed_task_id
                , collectionDate AS cdlz_header_collection_date
                , ingestDateTime AS cdlz_header_ingest_date_time
                , dataFormat AS cdlz_header_data_format
                , sourceFilename AS cdlz_header_source_filename
                , CASE WHEN sourceZipArchive= '' THEN CAST(Null as string) ELSE sourceZipArchive END AS cdlz_header_source_zip_archive
                , current_timestamp() AS glue_timestamp
                , input_file_name() AS input_filename
            """
        else:
            sql_text = """
                BASE64(header.recordHash) AS cdlz_header_record_hash_b64
                , header.dataFeedId AS cdlz_header_data_feed_id
                , header.collectDataFeedTaskId AS cdlz_header_collect_data_feed_task_id
                , header.ingestDataFeedTaskId AS cdlz_header_ingest_data_feed_task_id
                , header.collectionDate AS cdlz_header_collection_date
                , header.ingestDateTime AS cdlz_header_ingest_date_time
                , header.dataFormat AS cdlz_header_data_format
                , header.sourceFilename AS cdlz_header_source_filename
                , header.sourceZipArchive AS cdlz_header_source_zip_archive
                , current_timestamp() AS glue_timestamp
                , input_file_name() AS input_filename
            """
        return sql_text

    def _add_cdlz_header_to_sql(self, header_req, flattened, tablename, sql_text):
        """
        Add the CDLZ header to the target SQL, if required

        Default behaviour

        Parameters
        ----------
        header_req : True or False
            Whether the header needs to be added

        flattened : True or False
            When the data has been flattened (relationalized)

        tablename : string
            Either the entity being processed or the key
            from a relationalized dynamic frame

        sql_text : string
            The transform for the Spark SQL

        Returns
        -------
        sql text
            The new SQL
        """
        self.logger.info(f'_add_cdlz_header_to_sql: {header_req}')

        if header_req:
            sql_text = f"SELECT {self._cdlz_header(flattened=flattened)} {sql_text} FROM {tablename}"
        else:
            sql_text = f"SELECT {sql_text} FROM {tablename}"

        return sql_text
