package com.hs.bdes.prescription

import com.esi.dpe.config.ConfigParser
import com.esi.dpe.custom.Transform
import com.hs.bdes.util.JsonUtils.{getJSONValue, getVal, loop}
import com.hs.bdes.util.TimeZoneUtils._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{JObject, JValue}

import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._
import scala.util.Try

class PrescriptionEntityTransform extends Transform with LazyLogging with Serializable {

 def parseJson(str: String): JValue = Try {
  parse(str)
 }.getOrElse(null)

 case class getItemStoreKeySchema(store_nme: String, key_nme: String, primary_store_ind: Boolean, component_nme: String, component_value_txt: String)

 case class getPrescribedItemStoreKeySchema(store_nme: String, key_nme: String, primary_store_ind: Boolean, component_nme: String, component_value_txt: String)

 case class getPrescribedItemsSchema(item_category_desc: String, created_tms: Timestamp, created_tms_txt: String, created_timezone_txt: String, dosage_units_txt: String, directions_txt: String, dose_frequency_txt: String, item_type_desc: String, item_compound_ind: Boolean, drug_nbr: String, item_resource_id: String, notes_txt: String, patient_dispense_as_written_desc: String, patient_dispense_as_written_nme: String, prescriber_dispense_as_written_desc: String, prescriber_dispense_as_written_nme: String, prescribed_item_relative_id: String, drug_route_txt: String, last_updated_tms: Timestamp, last_updated_tms_txt: String, last_updated_timezone_txt: String, supply_id: String, cycle_days_qty: Integer, days_supply_qty: Integer, dosage_qty: Double, label_print_qty: Integer, daily_dosage_qty: Double, dispensed_quantity_unit_txt: String, dispensed_qty: Double, prescribed_item_store_key: List[getPrescribedItemStoreKeySchema],  item_store_key: List[getItemStoreKeySchema])

 case class getPrescriberSchema(renewal_ind: Boolean, dea_nbr: String, fax_nbr: String, first_nme: String, npi_nbr: String, last_nme: String, phone_nbr: String, patient_provided_ind: Boolean, pracitioner_resource_id: String, state_license_nbr: String, city_nme: String, country_iso2_cde: String, country_iso3_cde: String, postal_cde: String, state_cde: String, state_nme: String, medical_education_nbr: String, pharmacy_practice_nbr: String, street_address_txt: List[String])

 case class getEventSchema(event_tms: Timestamp, event_tms_txt: String, event_timezone_txt: String, source_application_nme: String, event_type_desc: String, source_application_user_nme: String, current_ind: Boolean)

 case class getReferral_documentsSchema(referral_document_file_id: String, referral_document_reference_creation_tms: Timestamp, referral_document_reference_creation_tms_txt: String, referral_document_reference_creation_timezone_txt: String)

 case class getForPatientSchema(customer_nbr: String, mail_group_desc: String, patient_automatically_generated_nbr: String, person_nbr: String, person_resource_id: String, specialty_profile_id: String, sub_group_desc: String, previous_patient_type_desc: String, membership_agn_id: String)

 case class getPreviousForPatientSchema(person_resource_id: String, specialty_profile_id: String, patient_type_desc: String, expiration_tms: Timestamp, expiration_tms_txt: String, expiration_timezone_txt: String)

 case class getStatusSchema(effective_tms: Timestamp, status_desc: String, effective_tms_txt: String, effective_timezone_txt: String, current_ind: Boolean)

 case class getReceivingPharmacyStoreKeySchema(store_nme: String, key_nme: String, primary_store_ind: Boolean, component_nme: String, component_value_txt: String)

 case class getFillProtocolSchema(protocol_tms: Timestamp, protocol_tms_txt: String, protocol_timezone_txt: String, source_system_created_tms: Timestamp, source_system_created_tms_txt: String, source_system_created_timezone_txt: String, fulfillment_center_id: String, protocol_reason_txt: String, protocol_resolution_tms: Timestamp, protocol_resolution_tms_txt: String, protocol_resolution_Timezone_txt: String, source_system_last_updated_tms: Timestamp, source_system_last_updated_tms_txt: String, source_system_last_updated_timezone_txt: String, sequence_nbr: Integer, invoice_nbr: String)

 case class getFillReplacementSchema(replacement_prescription_fill_relative_id: String, source_system_created_tms: Timestamp, source_system_created_tms_txt: String, source_system_created_timezone_txt: String, fill_protocol: List[getFillProtocolSchema])

 case class getFillDispensingPharmacyStoreKeySchema(store_nme: String, key_nme: String, primary_store_ind: Boolean, component_nme: String, component_value_txt: String, fill_replacement: List[getFillReplacementSchema])

 case class getFillEventSchema(event_tms: Timestamp, event_tms_txt: String, event_timezone_txt: String, source_application_nme: String, event_type_desc: String, source_application_user_nme: String, current_ind: Boolean, fill_dispensing_pharmacy_store_key: List[getFillDispensingPharmacyStoreKeySchema])

 case class getFillItemStoreKeySchema(store_nme: String, key_nme: String, primary_store_ind: Boolean, component_nme: String, component_value_txt: String, fill_event: List[getFillEventSchema])

 case class getFillDispensedItemStoreKeySchema(store_nme: String, key_nme: String, primary_store_ind: Boolean, component_nme: String, component_value_txt: String, fill_item_store_key: List[getFillItemStoreKeySchema])

 case class getFillDispensingItemSchema(item_category_desc: String, directions_txt: String, dosage_units_txt: String, dose_frequency_txt: String, item_type_desc: String, item_compound_ind: Boolean, ndc_nbr: String, drug_nbr: String, item_resource_id: String, notes_txt: String, dispense_as_written_desc: String, dispense_as_written_nme: String, prescribed_item_relative_id: String, dispensed_item_relative_id: String, created_tms: Timestamp, created_tms_txt: String, created_timezone_txt: String, drug_route_txt: String, last_updated_tms: Timestamp, last_updated_tms_txt: String, last_updated_timezone_txt: String, dispensed_quantity_unit_txt: String, supply_id: String, cycle_days_qty: Integer, days_supply_qty: Integer, dosage_qty: Double, label_print_qty: Integer, dispensed_qty: Double, fill_dispensed_item_store_key: List[getFillDispensedItemStoreKeySchema])

 case class getFillStatusExplainationSchema(status_explaination_desc: String, status_explaination_message_txt: String, soba_desc: String, fill_dispensing_item: List[getFillDispensingItemSchema])

 case class getFillStatusDetailSchema(cancel_desc: String, stop_ind: String, status_desc: String, fill_status_explaination: List[getFillStatusExplainationSchema])

 case class getFillStatusSchema(effective_tms: Timestamp, fill_status_txt: String, current_ind: Boolean, effective_tms_txt: String, effective_Timezone_txt: String, fill_status_detail: List[getFillStatusDetailSchema])

 case class getFillsSchema(prescription_fill_id: String, auto_refilled_ind: Boolean, source_system_created_tms: Timestamp, external_id: String, fill_tms: Timestamp, fill_nbr: String, fill_type_desc: String, refill_reminder_dte:date, prescription_Fill_relative_id: String, reorder_dte:date, fill_last_update_tms: Timestamp, source_system_last_update_tms: Timestamp, refill_source_system_tms: Timestamp, completed_ind: Boolean, original_source_system_nme: String, refill_last_update_tms: Timestamp, source_system_created_ Timestamp_txt: String, source_system_created_timezone: String, refill_last_update_tms_txt: String, refill_last_update_timezone: String, refill_source_system_tms_txt: String, refill_source_system_timezone_txt: String, fill_ Timestamp_txt: String, fill_timezone_txt: String, fill_last_update_tms_txt: String, fill_last_update_timezone_txt: String, provisional_fill_ind: Boolean, dispensing_pharmacy_line_of_business_txt: String, fill_dte:date, fulfillment_center_id: String, invoice_nbr: String, primary_membership_client_id: String, actual_dispensed_qty: Double, actual_fill_qty: Double, fill_status: List[getFillStatusSchema])

 case class getScheduledFillSchema(created_tms: Timestamp, created_tms_txt: String, created_timezone_txt: String, next_fill_dte: Date, pending_final_tms: Timestamp, pending_final_tms_txt: String, pending_final_timezone_txt: String, status_desc: String, scheduled_fill_type_desc: String, source_system_last_update_user_id: String, source_system_last_update_tms: Timestamp, source_system_last_update_tms_txt: String, source_system_last_update_timezone_txt: String)

 case class getStoreKeySchema(store_nme: String, key_nme: String, secondary_store_ind: Boolean, component_nme: String, component_value_txt: String)


//prescribed_items


 private def val getPrescribedItems: UserDefinedFunction = udf((jsonStr: String) => {
  val jObj = parse(jsonStr)
  val prescItemList = getJSONValue[List[JValue]](jObj, "payload.prescribedItems")
  val resourceId = getJSONValue[String](jObj, "payload.resourceId")
  if (prescItemList == null) {
   List()
  } else prescItemList.map(jVal => {
    val item_category_desc = getJSONValue[String](jVal, "category"),
    val created_tms = getTimeStampPract(getJSONValue[String](jVal, "createdDateTime"))
    val created_tms_txt = getJSONValue[String](jVal, "createdDateTime")
    val created_timezone_txt = if (getJSONValue[String](jVal, "createdDateTime").eq(null)) "" else entityTimeZone
    val dosage_units_txt = getJSONValue[String](jVal, "dosageUnits"),
    val directions_txt = getJSONValue[String](jVal, "directions"),
    val dose_frequency_txt = getJSONValue[String](jVal, "doseFrequency"),
    val item_type_desc = getJSONValue[String](jVal, "item.type"),
    val item_compound_ind = getJSONValue[Boolean](jVal, "item.compoundIndicator"),
    val drug_nbr = getJSONValue[String](jVal, "item.drugNumber"),
    val item_resource_id = getJSONValue[String](jVal, "item.resourceId"),
    val notes_txt = getJSONValue[String](jVal, "note"),
    val patient_dispense_as_written_desc = getJSONValue[String](jVal, "patientDaw.description"),
    val patient_dispense_as_written_nme = getJSONValue[String](jVal, "patientDaw.name"),
    val prescriber_dispense_as_written_desc = getJSONValue[String](jVal, "prescriberDaw.description"),
    val prescriber_dispense_as_written_nme = getJSONValue[String](jVal, "prescriberDaw.name"),
    val prescribed_item_relative_id = getJSONValue[String](jVal, "relativeId"),
    val drug_route_txt = getJSONValue[String](jVal, "drugRoute"),
    val last_updated_tms = getTimeStamp(getJSONValue[String](jVal, "updatedDateTime")),
    val last_updated_tms_txt = getJSONValue[String](jVal, "updatedDateTime"),
    val last_updated_timezone_txt = if (getJSONValue[String](jVal, "updatedDateTime").eq(null)) ""  else entityTimeZone,
    val supply_id = " ",
    val cycle_qty = if(getJSONValue[Any](jVal, "cycleDaysQuantity").eq(null)) 0 else cycle_qty.toString.toDouble.floor.toInt,
    val days_qty =  if(getJSONValue[Any](jVal, "daysSupplyQuantity").eq(null)) 0 else days_qty.toString.toDouble.floor.toInt,
    val dosage_qty = getJSONValue[Double](jVal, "dosageQuantity"),
    val label_qty = if(getJSONValue[Any](jVal, "numLabelsToPrint").eq(null)) 0 else label_qty.toString.toDouble.floor.toInt,
    val daily_qty = if(getJSONValue[String](jVal, "dailyDosage").eq(null)) 0.0 else daily_qty.toDouble,
    val ?? = if (getJSONValue[String](jVal, "item.type").eq(null)) null else {if (getJSONValue[String](jVal, "item.type") == "Drug")  getJSONValue[String](jVal, "item.resourceId") else null},
    val dispensed_quantity_unit_txt = getJSONValue[String](jVal, "quantityUnits"),
    val dispensed_qty = getJSONValue[Double](jVal, "quantity"),
    val precItemStoreKey = getPrecItemStoreKeyList(jVal),
    val ItemStoreKey = getPrecItemStoreKeyList(jVal,"item.")

    getPrescribedItemsSchema.apply(item_category_desc,
     created_tms,
     created_tms_txt,
     created_timezone_txt,
     dosage_units_txt,
     directions_txt,
     dose_frequency_txt,
     item_type_desc,
     item_compound_ind,
     drug_nbr,
     item_resource_id,
     notes_txt,
     patient_dispense_as_written_desc,
     patient_dispense_as_written_nme,
     prescriber_dispense_as_written_desc,
     prescriber_dispense_as_written_nme,
     prescribed_item_relative_id,
     drug_route_txt,
     last_updated_tms,
     last_updated_tms_txt,
     last_updated_timezone_txt,
     supply_id,
     cycle_qty,
     days_qty,
     dosage_qty,
     label_qty,
     daily_qty,
     ??,
     dispensed_quantity_unit_txt,
     dispensed_qty,
     precItemStoreKey,
     ItemStoreKey)
  }
 })

  def getPrecItemStoreKeyList(jVal: JValue, sourceStr: String = ""): List[getPrescribedItemStoreKeySchema] = {
   val (pkeyName) = (s"$sourceStr" + "storeKeySet.primaryKey")
   val (pkList) = (getJSONValue[List[JObject]](jVal, pkeyName))
   val primaryStoreInd = pkList != null
   val pkCheck = if (primaryStoreInd) {
    val pk = List(loop(jVal, pkeyName.split("\\.")).asInstanceOf[JObject])
    getPrecStoreKeyVal(pk, true)
   } else List()
   pkCheck
  }

  def getPrecStoreKeyVal(list: List[JObject], storeInd: Boolean): List[Row] = {
   val kc = List(("", ""))
   if (list == null) List(): List[Row]
   else list.map(x => {
    (
     getJSONValue[String](x, "storeName"),
     getJSONValue[String](x, "keyName"),
     storeInd,
     if (getJSONValue[List[JObject]](x, "keyComponents").eq(null)) kc
     else (getJSONValue[List[JObject]](x, "keyComponents").
      map(x => (getJSONValue[String](x, "name"), getJSONValue[String](x, "value"))))
    )
   }).flatMap {
    case (b, c, d, e) => e.map(v => Row.fromSeq(Seq(b, c, d, v._1, v._2)))
   }
  }

 //prescriber

 private def getEvents: UserDefinedFunction = udf((jsonStr: String) => {
  val jObj = parseJson(jsonStr)
  val resourceId = getJSONValue[String](jObj, "payload.resourceId")
  val prescriber = getPrescriberList(jObj)
  prescriber

 })

 def getPrescriberList(jVal: JValue): getPrescriberSchema = {
      val renewalPrescriberList = getJSONValue[List[JObject]](jVal, "payload.renewalPrescriber")
      val prescriberList = getJSONValue[List[JObject]](jVal, "payload.prescriber")
      val renewalPrescriber = {
       if (renewalPrescriberList == null) List(): List[getPrescriberSchema]
       else List(getPrescriber(jVal, "payload.renewalPrescriber.", true))
      }
      val prescriber = {
       if (prescriberList == null) List(): List[getPrescriberSchema]
       else List(getPrescriber(jVal, "payload.prescriber.", false))
      }
      renewalPrescriber ::: prescriber
   }


 def getPrescriber(jVal: JValue, parentKey: String, ind: Boolean): getPrescriberSchema = {
   getPrescriberSchema.apply(
    ind,
    getJSONValue[String](jVal, s"${parentKey}dea"),
    getJSONValue[String](jVal, s"${parentKey}fax"),
    getJSONValue[String](jVal, s"${parentKey}name.first"),
    getJSONValue[String](jVal, s"${parentKey}npi"),
    getJSONValue[String](jVal, s"${parentKey}name.last"),
    getJSONValue[String](jVal, s"${parentKey}phoneNumber"),
    getJSONValue[Boolean](jVal,s"${parentKey}patientProvidedIndicator"),
    getJSONValue[String](jVal, s"${parentKey}resourceId"),
    getJSONValue[String](jVal, s"${parentKey}stateLicense"),
    getJSONValue[String](jVal, s"${parentKey}postalAddress.city"),
    getJSONValue[String](jVal, s"${parentKey}postalAddress.country.iso2Code"),
    getJSONValue[String](jVal, s"${parentKey}postalAddress.country.iso3Code"),
    getJSONValue[String](jVal, s"${parentKey}postalAddress.postalCode"),
    getJSONValue[String](jVal, s"${parentKey}postalAddress.state.code"),
    getJSONValue[String](jVal, s"${parentKey}postalAddress.state.name"),
    getJSONValue[String](jVal, s"${parentKey}medicalEducationNumber"),
    getJSONValue[String](jVal, s"${parentKey}pharmacyPracticeNumber"),
      {
      val StreetAddrList = getJSONValue[List[JValue]](jVal, s"${parentKey}streetAddress")
      if (StreetAddrList.eq(null)) null else StreetAddrList.map(x => getVal[String](x)).filterNot(_ == null)
    }
  )
  }


 // event

 private def getEvents: UserDefinedFunction = udf((jsonStr: String) => {
   val jObj = parseJson(jsonStr)
   val resourceId = getJSONValue[String](jObj, "payload.resourceId")
   val events = getEventsList(jObj)
   events

 })
 def getPrescEvent(jVal: JValue, parentKey: String, ind: Boolean): getEventSchema = {
  getEventSchema.apply(
   getTimeStamp(getJSONValue[String](jVal, s"${parentKey}eventDateTime")),
   getJSONValue[String](jVal, s"${parentKey}eventDateTime"),
   if (getJSONValue[String](jVal, s"${parentKey}eventDateTime").eq(null)) ""
   else entityTimeZone,
   getJSONValue[String](jVal, s"${parentKey}sourceApplication"),
   getJSONValue[String](jVal, s"${parentKey}type"),
   getJSONValue[String](jVal, s"${parentKey}user"),
   ind
  )
 }

 def getEventsList(jVal: JValue): getEventSchema = {
   val eventsList = getJSONValue[List[JObject]](jVal, "payload.events")
   val eventList = getJSONValue[List[JObject]](jVal, "payload.event")
   val events = {
     if (eventsList == null) List() else {
     eventsList.map(jVal => if (jVal == null) null else getPrescEvent(jVal, "", true))
     }
   }
   val event = {
     if (eventList == null) List():
     else List(getPrescEvent(jVal, "payload.event.", false))
   }
   events ::: event
 }


 // referral_documents
 private def getReferralDocs: UserDefinedFunction = udf((jsonStr: String) => {
  val jObj = parseJson(jsonStr)
  val referralList = getJSONValue[List[JObject]](jObj, "payload.referralDocuments")

  if (null == referralList) {
   //List(getTaxonomySchema.apply(null,null,false,null,null,null,null,null,null,null,null,null,null,null,null,null))
   List()
  } else referralList.map(jVal => {
   if (jVal == null) null else{
    getReferralDocsList(jVal, "")

   }
 }
  def getReferralDocsList(jVal: JValue, parentKey: String): getReferral_documentsSchema = {
   Row.fromSeq(Seq(
    getJSONValue[String](jVal, "fileNetDocId"),
    getTimeStamp(getJSONValue[String](jVal, "createdDateTime")),
    getJSONValue[String](jVal, "createdDateTime"),
    if (getJSONValue[String](jVal, "createdDateTime").eq(null)) ""
    else entityTimeZone
   ))
  }
  // for_patient
  private def getForPatient: UserDefinedFunction = udf((jsonStr: String) => {
   val patientList = getJSONValue[List[JObject]](jVal, "payload.forPatient")
   if (patientList == null) List() else List(getForPatientList(jVal, "payload.forPatient."))
  }
  def getForPatientList(jVal: JValue, parentKey: String): getForPatientSchema = {
   getForPatientSchema.apply(
    getJSONValue[String](jVal, s"${parentKey}customerNumber"),
    getJSONValue[String](jVal, s"${parentKey}mailGroup"),
    getJSONValue[String](jVal, s"${parentKey}patientAGN"),
    getJSONValue[String](jVal, s"${parentKey}personNumber"),
    getJSONValue[String](jVal, s"${parentKey}resourceId"),
    getJSONValue[String](jVal, s"${parentKey}specialtyProfileId"),
    getJSONValue[String](jVal, s"${parentKey}subGroup"),
    getJSONValue[String](jVal, s"${parentKey}type"),
    getJSONValue[String](jVal, s"${parentKey}membershipId")
   )
  }

  // previous_for_patient

  private def getPrevForPatient: UserDefinedFunction = udf((jsonStr: String) => {

    val prevPatientList = getJSONValue[List[JObject]](jVal, "payload.previousForPatients")
    if (prevPatientList == null) List() else {
     prevPatientList.map(jVal => if (jVal == null) null else getPrevForPatientList(jVal, ""))
    }
   }
  def getPrevForPatientList(jVal: JValue, parentKey: String): getPreviousForPatientSchema = {
   getPreviousForPatientSchema.apply(
    getJSONValue[String](jVal, "resourceId"),
    getJSONValue[String](jVal, "specialtyProfileId"),
    getJSONValue[String](jVal, "type"),
    getTimeStamp(getJSONValue[String](jVal, "expirationDateTime")),
    getJSONValue[String](jVal, "expirationDateTime"),
    if (getJSONValue[String](jVal, "expirationDateTime").eq(null)) ""
    else entityTimeZone
   )
  }

  //status
  private def getPrescStatus: UserDefinedFunction = udf((jsonStr: String) => {
   val jObj = parseJson(jsonStr)
   val resourceId = getJSONValue[String](jObj, "payload.resourceId")
   val prescStatus = getPrescStatusList(jObj)
   prescStatus

  })

   def getPrescStatusList(jVal: JValue): getStatusSchema= {
     val statusesList = getJSONValue[List[JObject]](jVal, "payload.statuses")
     val statusList = getJSONValue[List[JObject]](jVal, "payload.status")
     val statuses = {
      if (statusesList == null) List(): List[getStatusSchema] else {
       statusesList.map(p => if (p == null) null else getPrescStatus(p, "", true))
      }
     }
     val status = {
      if (statusList == null) List(): List[getStatusSchema]
      else List(getPrescStatus(jVal, "payload.status.", false))
     }
     statuses ::: status
   }
   def getPrescStatus(jVal: JValue, parentKey: String, ind: Boolean): getStatusSchema = {
    getStatusSchema.apply(
     getTimeStamp(getJSONValue[String](jVal, s"${parentKey}effectiveDateTime")),
     getJSONValue[String](jVal, s"${parentKey}value"),
     getJSONValue[String](jVal, s"${parentKey}effectiveDateTime"),
     if (getJSONValue[String](jVal, s"${parentKey}effectiveDateTime").eq(null)) ""
     else entityTimeZone,
     ind
    )
   }





 //Store_Key

 private def getStoreKeys: UserDefinedFunction = udf((jsonStr: String) => {
   val jObj = parseJson(jsonStr)
   val storeKeys = getStoreKeyList(jObj, "payload.")
   storeKeys

  })

  def getStoreKeyList(jVal: JValue, sourceStr: String = ""): List[getStoreKeySchema] = {
   val skeyName = s"$sourceStr" + "secondaryKeys"
   val skList = getJSONValue[List[JObject]](jVal, skeyName)
   val secStoreInd = skList != null
   val skCheck = if (secStoreInd) {
    getStoreKeyVal(skList, true)
   } else List(): List[getStoreKeySchema]
   skCheck
  }

 def getStoreKeyVal(list: List[JObject], secondary_store_ind: Boolean): List[getStoreKeySchema] = {
  val kc = List(("", ""))
  if (list == null) List(): List[getStoreKeySchema]
  else list.map(x => {
   (
    getJSONValue[String](x, "storeName"),
    getJSONValue[String](x, "keyName"),
    secondary_store_ind,
    if (getJSONValue[List[JObject]](x, "keyComponents").eq(null)) kc
    else (getJSONValue[List[JObject]](x, "keyComponents").
     map(x => (getJSONValue[String](x, "name"), getJSONValue[String](x, "value"))))
   )
  }).flatMap {
   case (b, c, d, e) => e.map(v => getStoreKeySchema.fromSeq(Seq(b, c, d, v._1, v._2)))
  }
 }




 override def doTransform(input: ConfigParser.DPFConfig, df: DataFrame, config: Config): DataFrame = {
  //val jsonCol = "kafka_value"

  val initDf = df

  //val outputColsOrder = input.custom.getJSONArray("columns_order").iterator.asScala.toList.map(_.toString)


  val tempDf = initDf.
   withColumn("prescribed_items", getPrescribedItems(col("kafka_value"))).
   withColumn("prescriber", getPrescriberSchema(col("kafka_value"))).
   withColumn("event", getEventSchema(col("kafka_value"))).
   withColumn("referral_documents", getReferral_documentsSchema(col("kafka_value"))).
   withColumn("for_patient", getForPatientSchema(col("kafka_value"))).
   withColumn("previous_for_patient", getPreviousForPatientSchema(col("kafka_value"))).
   withColumn("status", getStatusSchema(col("kafka_value"))).
   withColumn("receiving_pharmacy_store_key", getReceivingPharmacyStoreKeySchema(col("kafka_value"))).
   withColumn("fills", getFillsSchema(col("kafka_value"))).

   withColumn("scheduled_fill", getOthers(col("kafka_value"))).
   withColumn("store_key", getStoreKeys(col("kafka_value")))



  val transformedDf = tempDf.withColumn("data_lake_last_update_tms", current_timestamp)
   .withColumn("medical_education_nbr_status_effective_tms",
    to_timestamp(col("medical_education_nbr_status_effective_tms_txt")))
   .withColumn("medical_education_nbr_status_effective_timezone_txt", lit(entityTimeZone))

   .withColumn("medical_education_nbr_status_expiration_tms",
    to_timestamp(col("medical_education_nbr_status_expiration_tms_txt")))
   .withColumn("medical_education_nbr_status_expiration_timezone_txt", lit(entityTimeZone))

   .withColumn("military_id_status_effective_tms",
    to_timestamp(col("military_id_status_effective_tms_txt")))
   .withColumn("military_id_status_effective_timezone_txt", lit(entityTimeZone))

   .withColumn("military_id_status_expiration_tms",
    to_timestamp(col("military_id_status_expiration_tms_txt")))
   .withColumn("military_id_status_expiration_timezone_txt", lit(entityTimeZone))

   .withColumn("npi_status_effective_tms",
    to_timestamp(col("npi_status_effective_tms_txt")))
   .withColumn("npi_status_effective_timezone_txt", lit(entityTimeZone))

   .withColumn("npi_status_expiration_tms",
    to_timestamp(col("npi_status_expiration_tms_txt")))
   .withColumn("npi_status_expiration_timezone_txt", lit(entityTimeZone))

   .withColumn("spi_id_status_effective_tms",
    to_timestamp(col("spi_id_status_effective_tms_txt")))
   .withColumn("spi_id_status_effective_timezone_txt", lit(entityTimeZone))

   .withColumn("spi_id_status_expiration_tms",
    to_timestamp(col("spi_id_status_expiration_tms_txt")))
   .withColumn("spi_id_status_expiration_timezone_txt", lit(entityTimeZone))

   .withColumn("status_effective_tms",
    to_timestamp(col("status_effective_tms_txt")))
   .withColumn("status_effective_timezone_txt", lit(entityTimeZone))

   .withColumn("status_expiration_tms",
    to_timestamp(col("status_expiration_tms_txt")))
   .withColumn("status_expiration_timezone_txt", lit(entityTimeZone))
   .withColumn("tenant_id", col("tenant_id").cast("decimal(18)"))

  transformedDf.where("tenant_id == '1'")
 }

}
