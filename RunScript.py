from DatabaseSyncs import DBFunctions as dbf #Import Functions
from DatabaseSyncs.Queries import StoredQueries as sq #Import queries

#Insert missing rows for Clinic.Adjustment
dbf.BuildInsertPG(sq.ClinicAdjustmentES, sq.ClinicAdjustmentINSERT)

#Insert missing rows for Clinic.AppointmentType
dbf.BuildInsertPG(sq.ClinicAppointmentTypeES, sq.ClinicAppointmentTypeINSERT)

#Insert missing rows for Clinic.Paytype
dbf.BuildInsertPG(sq.ClinicPaytypeES, sq.ClinicPaytypeINSERT)

#Insert missing rows for Clinic.ReferralType
dbf.BuildInsertPG(sq.ClinicReferralTypeES, sq.ClinicReferralTypeINSERT)

#Insert missing rows for Patient.Insurance
dbf.BuildInsertPG(sq.PatientInsuranceES, sq.PatientInsuranceINSERT)

#Insert missing rows for Clinic.Services
dbf.BuildInsertPG(sq.ClinicServicesES, sq.ClinicServicesINSERT)

#Insert missing rows for Provider.Position
dbf.BuildInsertPG(sq.ProviderPositionES, sq.ProviderPositionINSERT)

#Insert missing rows for Patient.Employer
dbf.BuildInsertPG(sq.PatientEmployerES, sq.PatientEmployerINSERT)

#Insert missing rows for Provider.Provider
dbf.BuildInsertPG(sq.ProviderProviderES, sq.ProviderProviderINSERT)

#Insert missing rows for Clinic.EOD
dbf.BuildInsertPG(sq.ClinicEODES, sq.ClinicEODINSERT)

#Insert missing rows for Patient.Patient
dbf.BuildInsertPG(sq.PatientPatientES, sq.PatientPatientINSERT)

#Insert missing rows for Patient.Appointment
dbf.BuildInsertPG(sq.PatientAppointmentES, sq.PatientAppointmentINSERT)

#Insert missing rows for Patient.Referral
dbf.BuildInsertPG(sq.PatientReferralES, sq.PatientReferralINSERT)

#Insert missing rows for Patient.TreatmentPlan
dbf.BuildInsertPG(sq.PatientTreatmentPlanES, sq.PatientTreatmentPlanINSERT)

#Insert missing rows for Trans.InsuranceClaim
dbf.BuildInsertPG(sq.TransInsuranceClaimES, sq.TransInsuranceClaimINSERT)

#Insert missing rows for Trans.PlannedServices
dbf.BuildInsertPG(sq.TransPlannedServicesES, sq.TransPlannedServicesINSERT)

#Insert missing rows for Patient.Operatory
dbf.BuildInsertPG(sq.PatientOperatoryES, sq.PatientEmployerINSERT)

#Insert missing rows for Patient.TreatmentItems
dbf.BuildInsertPG(sq.PatientTreatmentItemsES, sq.PatientTreatmentItemsINSERT)

#Insert missing rows for Trans.TransactionHeader
dbf.BuildInsertPG(sq.TransTransactionHeaderES, sq.TransTransactionHeaderINSERT)

#Insert missing rows for Trans.TransactionDetail
dbf.BuildInsertPG(sq.TransTransactionDetailES, sq.TransTransactionDetailINSERT)