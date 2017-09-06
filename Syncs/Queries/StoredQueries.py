import sys

#Queries for Clinic.Adjustment
ClinicAdjustmentES = 'SELECT adjustment_type_id, description, impacts, IFNULL(central_id, 0) FROM adjustment_type'
ClinicAdjustmentINSERT = 'INSERT INTO "Clinic"."Adjustment" VALUES {0} ON CONFLICT (clinicid, adjustmentid) DO NOTHING'

#Queries for Clinic.AppointmentType
ClinicAppointmentTypeES = 'SELECT type_id, description, amount, appt_minutes FROM appt_types'
ClinicAppointmentTypeINSERT = 'INSERT INTO "Clinic"."AppointmentType" VALUES {0} ON CONFLICT (clinicid, typeid) DO NOTHING'

#Queries for Clinic.EOD
ClinicEODES = 'SELECT eod_sequence, time_ran, start_tran_num, end_tran_num, user_id, eod_description FROM eod'
ClinicEODFilter = ' WHERE time_ran > GETDATE()-20 ORDER BY eod_sequence DESC'
ClinicEODINSERT = 'INSERT INTO "Clinic"."EOD" VALUES {0} ON CONFLICT (clinicid, eodsequence) DO NOTHING'

#Queries for Clinic.Paytype
ClinicPaytypeES = """
SELECT 
	paytype_id, 
	sequence, 
	description, 
	IFNULL(prompt,'',prompt), 
	display_on_payment_screen, 
	currency_type, 
	include_on_deposit_yn, 
	IFNULL(central_id,'0',central_id), 
	system_required 
FROM paytype
"""
ClinicPaytypeINSERT = 'INSERT INTO "Clinic"."Paytype" VALUES {0} ON CONFLICT (clinicid, paytypeid) DO NOTHING'

#Queries for Clinic.ReferralType
ClinicReferralTypeES = 'SELECT other_referral_id, name, status FROM other_referral'
ClinicReferralTypeINSERT = 'INSERT INTO "Clinic"."ReferralType" VALUES {0} ON CONFLICT (clinicid, referralid) DO NOTHING'

#Queries for Clinic.Services
ClinicServicesES = """
SELECT 
	service_code,
	IFNULL(ada_code,'',ada_code),
	description,
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

#Queries for Patient.Appointment
PatientAppointmentES = """
SELECT 
	appointment_id,
	start_time,
	end_time
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

#Queries for Patient.Operatory
PatientOperatoryES = """
SELECT
	note_id,
	patient_id,
	date_entered
	user_id,
	note_type,
	description,
	locked_eod,
	status,
	claim_id,
	resp_party_id,
	tran_num
FROM operatory_notes
"""
PatientOperatoryINSERT = 'INSERT INTO "Patient"."Operatory" VALUES {0} ON CONFLICT (clinicid, noteid) DO NOTHING'

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
PatientPatientINSERT = 'INSERT INTO "Patient"."Patient" VALUES {0} ON CONFLICT (clinicid, patientid) DO NOTHING'

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

#Queries for Patient.TreatmentPlan
PatientTreatmentPlanES = """
SELECT
	treatment_pland_id,
	patient_id,
	user_id,
	description,
	status,
	date_entered
FROM treatment_plans
"""
PatientTreatmentPlanINSERT = 'INSERT INTO "Patient"."TreatmentPlan" VALUES {0} ON CONFLICT (clinicid, treatmentid) DO NOTHING'

#Queries for Provider.Position
ProviderPositionES = 'SELECT position_id, description, security_profile FROM positions'
ProviderPositionINSERT = 'INSERT INTO "Provider"."Position" VALUES {0} ON CONFLICT (clinicid, positionid) DO NOTHING'

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
	description,
	sort_order
FROM planned_services
"""
TransPlannedServicesINSERT = 'INSERT INTO "Trans"."PlannedServices" VALUES {0} ON CONFLICT ("clinicid", patientid, lineid) DO NOTHING'

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
	status,
	sequence,
	description
FROM transaction_header
"""
TransTransactionHeaderINSERT = 'INSERT INTO "Trans"."TransactionHeader" VALUES {0} ON CONFLICT ("clinicid", trannum) DO NOTHING'