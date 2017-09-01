import sys

#Queries for Clinic.Adjustment
ClinicAdjustmentES = 'SELECT adjustment_type_id, description, impacts, IFNULL(central_id, 0) FROM adjustment_type'
ClinicAdjustmentINSERT = 'INSERT INTO "Clinic"."Adjustment" VALUES {0} ON CONFLICT (clinicid, adjustmentid) DO NOTHING'

#Queries for Clinic.AppointmentType
ClinicAppointmentTypeES = 'SELECT type_id, description, amount, appt_minutes FROM appt_types'
ClinicAppointmentTypeINSERT = 'INSERT INTO "Clinic"."AppointmentType" VALUES {0} ON CONFLICT (clinicid, typeid) DO NOTHING'

#Queries for Clinic.EOD
ClinicEODES = 'SELECT eod_sequence, time_ran, start_tran_num, end_tran_num, user_id, eod_description FROM eod'
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
#Queries for Patient.Employer
#Queries for Patient.Insurance
PatientInsuranceES = """
SELECT
    insurance_company_id,
    REPLACE(name,'''',''),
    IFNULL(address_1,'',address_1),
    IFNULL(city,'',city),
    IFNULL(state,'',state),
    IFNULL(zipcode,'',zipcode),
    CAST(IFNULL(phone1,0,phone1) AS BIGINT),
    CAST(IFNULL(fax,0,fax) AS BIGINT),
    IFNULL(neic_payer_id, 'No ID',neic_payer_id),
    IFNULL(nea_payer_id, 'No ID', nea_payer_id)
FROM insurance_company
"""
PatientInsuranceINSERT = 'INSERT INTO "Patient"."Insurance" VALUES {0} ON CONFLICT (clinicid, insuranceid) DO NOTHING'

#Queries for Patient.Operatory
#Queries for Patient.Patient
#Queries for Patient.Referral
#Queries for Patient.TreatmentItems
#Queries for Patient.TreatmentPlan
#Queries for Provider.GMBReview
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
#Queries for Trans.InsurancePaid
#Queries for Trans.PlannedServices
#Queries for Trans.TransactionDetail
#Queries for Trans.TransactionHeader