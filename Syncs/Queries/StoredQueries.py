import sys

#Queries for Clinic.Adjustment
ClinicAdjustmentES = 'SELECT adjustment_type_id, description, impacts, IFNULL(central_id, 0) FROM adjustment_type'
ClinicAdjustmentINSERT = 'INSERT INTO "Clinic"."Adjustment" VALUES {0} ON CONFLICT (clinicid, adjustmentid) DO NOTHING'
ClinicAdjustmentTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."Adjustment"

EXCEPT

SELECT *
FROM "Clinic"."Adjustment")

INSERT INTO "Clinic"."Adjustment" AS ca
SELECT w.*
FROM CTE_Exception AS w
ON CONFLICT (clinicid, adjustmentid)
DO NOTHING
"""

#Queries for Clinic.AppointmentType
ClinicAppointmentTypeES = 'SELECT type_id, description, amount, appt_minutes FROM appt_types'
ClinicAppointmentTypeINSERT = 'INSERT INTO "Clinic"."AppointmentType" VALUES {0} ON CONFLICT (clinicid, typeid) DO NOTHING'
ClinicAppointmentTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."AppointmentType"

EXCEPT

SELECT *
FROM "Clinic"."AppointmentType")

INSERT INTO "Clinic"."AppointmentType" AS ca
SELECT w.*
FROM CTE_Exception AS w
ON CONFLICT (clinicid, typeid)
DO NOTHING
"""

#Queries for Clinic.EOD
ClinicEODES = 'SELECT eod_sequence, time_ran, start_tran_num, end_tran_num, user_id, eod_description FROM eod'
ClinicEODFilter = ' WHERE time_ran > GETDATE()-20 ORDER BY eod_sequence DESC'
ClinicEODINSERT = 'INSERT INTO "Clinic"."EOD" VALUES {0} ON CONFLICT (clinicid, eodsequence) DO NOTHING'
ClinicEODTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."EOD"

EXCEPT

SELECT *
FROM "Clinic"."EOD")

INSERT INTO "Clinic"."EOD" AS ca
SELECT w.*
FROM CTE_Exception AS w
ON CONFLICT (clinicid, eodsequence)
DO NOTHING
"""

#Queries for Clinic.Paytype
ClinicPaytypeES = """
SELECT 
	paytype_id, 
	sequence, 
	REPLACE(description,'''','') AS description,
	IFNULL(prompt,'',prompt), 
	display_on_payment_screen, 
	currency_type, 
	include_on_deposit_yn, 
	IFNULL(central_id,'0',central_id), 
	system_required 
FROM paytype
"""
ClinicPaytypeINSERT = 'INSERT INTO "Clinic"."Paytype" VALUES {0} ON CONFLICT (clinicid, paytypeid) DO NOTHING'
ClinicPaytypeTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."Paytype"

EXCEPT

SELECT *
FROM "Clinic"."Paytype")

INSERT INTO "Clinic"."Paytype" AS ca
SELECT w.*
FROM CTE_Exception AS w
ON CONFLICT (clinicid, paytypeid)
DO NOTHING
"""

#Queries for Clinic.ReferralType
ClinicReferralTypeES = 'SELECT other_referral_id, name, status FROM other_referral'
ClinicReferralTypeINSERT = 'INSERT INTO "Clinic"."ReferralType" VALUES {0} ON CONFLICT (clinicid, referralid) DO NOTHING'
ClinicReferralTypeTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."ReferralType"

EXCEPT

SELECT *
FROM "Clinic"."ReferralType")

INSERT INTO "Clinic"."ReferralType" AS ca
SELECT w.*
FROM CTE_Exception AS w
ON CONFLICT (clinicid, referralid)
DO NOTHING
"""

#Queries for Clinic.Services
ClinicServicesES = """
SELECT 
	service_code,
	IFNULL(ada_code,'',ada_code),
	REPLACE(description,'''','') AS description,
	CAST(IFNULL(service_type_id,'0',service_type_id) AS INT),
	impacted_area,
	REPLACE(IFNULL(smart_code1,'NULL',smart_code1),'''',''),
	REPLACE(IFNULL(smart_code2,'NULL',smart_code2),'''',''),
	REPLACE(IFNULL(smart_code3,'NULL',smart_code3),'''',''),
	REPLACE(IFNULL(smart_code4,'NULL',smart_code4),'''',''),
	REPLACE(IFNULL(smart_code5,'NULL',smart_code5),'''',''),
	CAST(ISNULL(sequence,0,sequence) AS INT),
	CAST(ISNULL(fee,0,fee) AS NUMERIC(16,2))
FROM services
"""
ClinicServicesINSERT = 'INSERT INTO "Clinic"."Services" VALUES {0} ON CONFLICT (clinicid, servicecode) DO NOTHING'
ClinicServicesTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."Services"

EXCEPT

SELECT *
FROM "Clinic"."Services")

INSERT INTO "Clinic"."Services" AS ca
SELECT w.*
FROM CTE_Exception AS w
ON CONFLICT (clinicid, servicecode)
DO NOTHING
"""

#Queries for Patient.Appointment
PatientAppointmentES = """
SELECT 
	appointment_id,
	start_time,
	end_time,
	patient_id,
	location_id,
	appointment_type_id,
	arrival_time,
	inchair_time,
	walkout_time
FROM appointment
WHERE start_time > '2000-01-01' AND end_time > '2000-01-01'
"""
PatientAppointmentINSERT = 'INSERT INTO "Patient"."Appointment" VALUES {0} ON CONFLICT (clinicid, appointmentid) DO NOTHING'
PatientAppointmentFilter = 'AND start_time > GETDATE()-1' #Limit the amount of results queried
PatientAppointmentTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."Appointment"

EXCEPT

SELECT *
FROM "Patient"."Appointment")

INSERT INTO "Patient"."Appointment" AS ca
SELECT w.*
FROM CTE_Exception AS w
LEFT JOIN "Patient"."Patient" pat
  ON w.clinicid = pat.clinicid AND w.patientid = pat.patientid
LEFT JOIN "Clinic"."AppointmentType" apt
  ON w.clinicid = apt.clinicid AND w.typeid = apt.typeid
WHERE pat.patientid IS NOT NULL
AND apt.typeid IS NOT NULL
ON CONFLICT (clinicid, appointmentid)
DO NOTHING
"""

#Queries for Patient.Employer
PatientEmployerES = """
SELECT
	employer_id,
	name,
	address_1,
	city,
	state,
	zipcode,
    CAST(REPLACE(REPLACE(phone1,' ', ''),'-','') AS BIGINT),
    CAST(REPLACE(REPLACE(fax,' ', ''),'-','') AS BIGINT),
	group_number,
	insurance_company_id,
	maximum_coverage,
	yearly_deductible,
	group_name
FROM employer
"""
PatientEmployerINSERT = 'INSERT INTO "Patient"."Employer" VALUES {0} ON CONFLICT (clinicid, employerid) DO NOTHING'
PatientEmployerTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."Employer"

EXCEPT

SELECT *
FROM "Patient"."Employer")

INSERT INTO "Patient"."Employer" AS ca
SELECT w.*
FROM CTE_Exception AS w
LEFT JOIN "Patient"."Insurance" i
  ON w.clinicid = i.clinicid AND w.insuranceid = i.insuranceid
WHERE i.insuranceid IS NOT NULL
ON CONFLICT (clinicid, employerid)
DO NOTHING
"""

#Queries for Patient.Insurance
PatientInsuranceES = """
SELECT
    insurance_company_id,
    REPLACE(name,'''',''),
    address_1,
    city,
    state,
    zipcode,
    CAST(REPLACE(REPLACE(phone1,' ', ''),'-','') AS BIGINT),
    CAST(REPLACE(REPLACE(fax,' ', ''),'-','') AS BIGINT),
    neic_payer_id,
    nea_payer_id
FROM insurance_company
"""
PatientInsuranceINSERT = 'INSERT INTO "Patient"."Insurance" VALUES {0} ON CONFLICT (clinicid, insuranceid) DO NOTHING'
PatientInsuranceTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."Insurance"

EXCEPT

SELECT *
FROM "Patient"."Insurance")

INSERT INTO "Patient"."Insurance" AS ca
SELECT w.*
FROM CTE_Exception AS w
ON CONFLICT (clinicid, insuranceid)
DO NOTHING
"""

#Queries for Patient.Operatory
PatientOperatoryES = """
SELECT
	note_id,
	patient_id,
	date_entered,
	user_id,
	note_type,
	REPLACE(description,'''','') AS description,
	locked_eod,
	status,
	claim_id,
	resp_party_id,
	tran_num
FROM operatory_notes
"""
PatientOperatoryINSERT = 'INSERT INTO "Patient"."Operatory" VALUES {0} ON CONFLICT (clinicid, noteid) DO NOTHING'
PatientOperatoryTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."Operatory"

EXCEPT

SELECT *
FROM "Patient"."Operatory")

INSERT INTO "Patient"."Operatory" AS ca
SELECT w.*
FROM CTE_Exception AS w
ON CONFLICT (clinicid, noteid)
DO NOTHING
"""

#Queries for Patient.Patient
PatientPatientES = """
SELECT
	patient_id,
	responsible_party,
	status,
	date_entered,
	prim_employer_id
FROM patient
"""
PatientPatientTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."Patient"

EXCEPT

SELECT *
FROM "Patient"."Patient")

INSERT INTO "Patient"."Patient" AS p
SELECT w.*
FROM CTE_Exception AS w
LEFT JOIN "Patient"."Employer" AS e
  ON w.clinicid = e.clinicid AND w.employerid = e.employerid
WHERE e.employerid IS NOT NULL
ON CONFLICT (clinicid, patientid)
DO NOTHING
"""
PatientPatientINSERT = 'INSERT INTO "Patient"."Patient" VALUES {0} ON CONFLICT (clinicid, patientid) DO NOTHING'
PatientPatientFilter = 'WHERE patient_id > (SELECT MAX(patient_id)-200 FROM patient)'

#Queries for Patient.Referral
PatientReferralES = """
SELECT
	referred_patient,
	other_referral_id,
	patient_id,
	provider_id,
	eod_sequence
FROM referred_by
"""
PatientReferralINSERT = 'INSERT INTO "Patient"."Referral" VALUES {0} ON CONFLICT (clinicid, patientid) DO NOTHING'
PatientReferralTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."Referral"

EXCEPT

SELECT *
FROM "Patient"."Referral")

INSERT INTO "Patient"."Referral" AS ca
SELECT w.*
FROM CTE_Exception AS w
LEFT JOIN "Clinic"."ReferralType" rt
  ON w.clinicid = rt.clinicid AND w.referralid = rt.referralid
LEFT JOIN "Patient"."Patient" p
  ON w.clinicid = p.clinicid AND w.patientid = p.patientid
WHERE rt.referralid IS NOT NULL
AND p.patientid IS NOT NULL
ON CONFLICT (clinicid, patientid, referralid)
DO NOTHING
"""

#Queries for Patient.TreatmentItems
PatientTreatmentItemsES = """
SELECT
	treatment_plan_id,
	line_number,
	patient_id,
	claim_id,
	sort_order
FROM treatment_plan_items
"""
PatientTreatmentItemsINSERT = 'INSERT INTO "Patient"."TreatmentItems" VALUES {0} ON CONFLICT (clinicid, treatmentid, lineid) DO NOTHING'
PatientTreatmentItemsTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."TreatmentItems"

EXCEPT

SELECT *
FROM "Patient"."TreatmentItems")

INSERT INTO "Patient"."TreatmentItems" AS ca
SELECT w.*
FROM CTE_Exception AS w
LEFT JOIN "Patient"."Patient" rt
  ON w.clinicid = rt.clinicid AND w.patientid = rt.patientid
LEFT JOIN "Patient"."Patient" p
  ON w.clinicid = p.clinicid AND w.patientid = p.patientid
LEFT JOIN "Patient"."TreatmentPlan" tp
  ON w.clinicid = tp.clinicid AND w.treatmentid = tp.treatmentid
WHERE rt.patientid IS NOT NULL
AND p.patientid IS NOT NULL
AND tp.treatmentid IS NOT NULL
ON CONFLICT (clinicid, treatmentid, lineid)
DO NOTHING
"""

#Queries for Patient.TreatmentPlan
PatientTreatmentPlanES = """
SELECT
	treatment_plan_id,
	patient_id,
	user_id,
	REPLACE(description,'''','') AS description,
	status,
	date_entered
FROM treatment_plans
"""
PatientTreatmentPlanINSERT = 'INSERT INTO "Patient"."TreatmentPlan" VALUES {0} ON CONFLICT (clinicid, treatmentid) DO NOTHING'
PatientTreatmentPlanTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."TreatmentPlan"

EXCEPT

SELECT *
FROM "Patient"."TreatmentPlan")

INSERT INTO "Patient"."TreatmentPlan" AS ca
SELECT w.*
FROM CTE_Exception AS w
LEFT JOIN "Patient"."Patient" p
  ON w.clinicid=p.clinicid AND w.patientid = p.patientid
LEFT JOIN "Provider"."Provider" pro
  ON w.clinicid=pro."ClinicID" AND w.userid = pro.providerid
WHERE p.patientid IS NOT NULL
AND pro.providerid IS NOT NULL
ON CONFLICT (clinicid, treatmentid)
DO NOTHING
"""

#Queries for Provider.Position
ProviderPositionES = 'SELECT position_id, description, security_profile FROM positions'
ProviderPositionINSERT = 'INSERT INTO "Provider"."Position" VALUES {0} ON CONFLICT (clinicid, positionid) DO NOTHING'
ProviderPositionTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."Position"

EXCEPT

SELECT *
FROM "Provider"."Position")

INSERT INTO "Provider"."Position" AS ca
SELECT w.*
FROM CTE_Exception AS w
ON CONFLICT (clinicid, positionid)
DO NOTHING
"""

#Queries for Provider.Provider
ProviderProviderES = """
SELECT 
    provider_id,
    first_name,
    last_name,
    hire_date,
    collections_go_to,
    provider_on_insurance,
    CAST(position_id AS INT),
    email,
    CAST(REPLACE(other_id_21,'-','') AS BIGINT),
    birth_date
FROM provider
"""
ProviderProviderINSERT = 'INSERT INTO "Provider"."Provider" VALUES {0} ON CONFLICT ("ClinicID", providerid) DO NOTHING'
ProviderProviderTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."Provider"

EXCEPT

SELECT *
FROM "Provider"."Provider")

INSERT INTO "Provider"."Provider" AS ca
SELECT w.*
FROM CTE_Exception AS w
LEFT JOIN "Provider"."Position" AS p
  ON w."ClinicID" = p.clinicid AND w.positionid=p.positionid
ON CONFLICT ("ClinicID",providerid)
DO NOTHING
"""

#Queries for Trans.InsuranceClaim
TransInsuranceClaimES = """
SELECT
	claim_id,
	statement_num,
	patient_id,
	date_created,
	provider_id,
	prim_employer_id,
	prim_insurance_company_id,
	prim_responsible_id,
	prim_relationship,
	claim_type
FROM insurance_claim
"""
TransInsuranceClaimINSERT = 'INSERT INTO "Trans"."InsuranceClaim" VALUES {0} ON CONFLICT ("clinicid", claimid) DO NOTHING'
TransInsuranceClaimTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."InsuranceClaim"

EXCEPT

SELECT *
FROM "Trans"."InsuranceClaim")

INSERT INTO "Trans"."InsuranceClaim" AS ca
SELECT w.*
FROM CTE_Exception AS w
LEFT JOIN "Patient"."Patient" p
  ON w.clinicid = p.clinicid AND w.patientid = p.patientid
LEFT JOIN "Provider"."Provider" pro
  ON w.clinicid = pro."ClinicID" AND w.providerid = pro.providerid
WHERE p.patientid IS NOT NULL
AND pro.providerid IS NOT NULL
ON CONFLICT (clinicid, claimid)
DO NOTHING
"""

#Queries for Trans.InsurancePaid
TransInsurancePaidES = """
SELECT
	claim_id,
	prim_submitted_total,
	prim_total_paid,
	sec_total_paid
FROM insurance_claim
"""
TransInsurancePaidINSERT = 'INSERT INTO "Trans"."InsurancePaid" VALUES {0} ON CONFLICT ("clinicid", claimid) DO NOTHING'
TransInsurancePaidTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."InsurancePaid"

EXCEPT

SELECT *
FROM "Trans"."InsurancePaid")

INSERT INTO "Trans"."InsurancePaid" AS ca
SELECT w.*
FROM CTE_Exception AS w
LEFT JOIN "Trans"."InsuranceClaim" ic
  ON w.clinicid = ic.clinicid AND w.claimid = ic.claimid
WHERE ic.claimid IS NOT NULL
ON CONFLICT (clinicid, claimid)
DO NOTHING
"""

#Queries for Trans.PlannedServices
TransPlannedServicesES = """
SELECT
	patient_id,
	line_number,
	service_code,
	sequence,
	provider_id,
	date_planned,
	status,
	REPLACE(description,'''',''),
	sort_order
FROM planned_services
"""
TransPlannedServicesINSERT = 'INSERT INTO "Trans"."PlannedServices" VALUES {0} ON CONFLICT ("clinicid", patientid, lineid) DO NOTHING'
TransPlannedServicesTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."PlannedServices"

EXCEPT

SELECT *
FROM "Trans"."PlannedServices")
INSERT INTO "Trans"."PlannedServices" AS ca
SELECT w.*
FROM CTE_Exception AS w
LEFT JOIN "Patient"."Patient" p
  ON w.clinicid = p.clinicid AND w.patientid = p.patientid
LEFT JOIN "Provider"."Provider" pro
  ON w.clinicid = pro."ClinicID" AND w.providerid = pro.providerid
WHERE p.patientid IS NOT NULL
AND pro.providerid IS NOT NULL
ON CONFLICT (clinicid, patientid, lineid)
DO NOTHING
"""

#Queries for Trans.TransactionDetail
TransTransactionDetailES = """
SELECT
	detail_id,
	tran_num,
	patient_id,
	user_id,
	provider_id,
	collections_go_to,
	date_entered,
	amount,
	applied_to
FROM transactions_detail
"""
TransTransactionDetailINSERT = 'INSERT INTO "Trans"."TransactionDetail" VALUES {0} ON CONFLICT ("clinicid", trannum, detailid) DO NOTHING'
TransTransactionDetailTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."TransactionDetail"

EXCEPT

SELECT *
FROM "Trans"."TransactionDetail")

INSERT INTO "Trans"."TransactionDetail" AS ca
SELECT w.*
FROM CTE_Exception AS w
LEFT JOIN "Trans"."TransactionHeader" th
  ON w.clinicid = th.clinicid AND w.trannum = th.trannum
LEFT JOIN "Patient"."Patient" pat
  ON w.clinicid = pat.clinicid AND w.patientid = pat.patientid
LEFT JOIN "Provider"."Provider" pro
  ON w.clinicid = pro."ClinicID" AND w.providerid = pro.providerid
WHERE th.trannum IS NOT NULL
AND pat.patientid IS NOT NULL
AND pro.providerid IS NOT NULL
ON CONFLICT (clinicid, trannum, detailid)
DO NOTHING
"""

#Queries for Trans.TransactionHeader
TransTransactionHeaderES = """
SELECT
	tran_num,
	user_id,
	resp_party_id,
	amount,
	tran_date,
	service_code,
	paytype_id,
	adjustment_type,
	statement_num,
	claim_id,
	impacts,
	type,
	sequence,
	REPLACE(description,'''','') AS description,
	status
FROM transactions_header
"""
TransTransactionHeaderINSERT = 'INSERT INTO "Trans"."TransactionHeader" VALUES {0} ON CONFLICT ("clinicid", trannum) DO NOTHING'
TransTransactionHeaderTempCompare = """
WITH CTE_Exception AS(
SELECT *
FROM "Temp"."TransactionHeader"

EXCEPT

SELECT *
FROM "Trans"."TransactionHeader")

INSERT INTO "Trans"."TransactionHeader" AS ca
SELECT w.*
FROM CTE_Exception AS w
LEFT JOIN "Provider"."Provider" pro
   ON w.clinicid = pro."ClinicID" AND w.userid = pro.providerid
LEFT JOIN "Patient"."Patient" pat
   ON w.clinicid = pat.clinicid AND w.responsibleparty = pat.patientid
LEFT JOIN "Clinic"."Services" serv
   ON w.clinicid = serv.clinicid AND w.servicecode = serv.servicecode
WHERE pro.providerid IS NOT NULL
AND pat.patientid IS NOT NULL
AND serv.servicecode IS NOT NULL
AND (w.paytypeid IS NULL
     OR EXISTS (SELECT pay.paytypeid
      FROM "Clinic"."Paytype" pay
      WHERE pay.clinicid = w.clinicid
      AND pay.paytypeid = w.paytypeid))
AND (w.adjustmentid IS NULL
     OR EXISTS(SELECT adj.adjustmentid
      FROM "Clinic"."Adjustment" adj
      WHERE adj.clinicid = w.clinicid
      AND adj.adjustmentid = w.adjustmentid))
ON CONFLICT (clinicid, trannum)
DO NOTHING
"""