"""Predefined recognizers package. Holds all the default recognizers."""

from presidio_analyzer.predefined_recognizers.transformers_recognizer import (
    TransformersRecognizer,
)
from .aba_routing_recognizer import AbaRoutingRecognizer
from .credit_card_recognizer import CreditCardRecognizer
from .crypto_recognizer import CryptoRecognizer
from .date_recognizer import DateRecognizer
from .email_recognizer import EmailRecognizer
from .es_nif_recognizer import EsNifRecognizer
from .iban_recognizer import IbanRecognizer
from .ip_recognizer import IpRecognizer
from .it_driver_license_recognizer import ItDriverLicenseRecognizer
from .it_fiscal_code_recognizer import ItFiscalCodeRecognizer
from .it_identity_card_recognizer import ItIdentityCardRecognizer
from .it_passport_recognizer import ItPassportRecognizer
from .it_vat_code import ItVatCodeRecognizer
from .medical_license_recognizer import MedicalLicenseRecognizer
from .phone_recognizer import PhoneRecognizer
from .sg_fin_recognizer import SgFinRecognizer
from .spacy_recognizer import SpacyRecognizer
from .stanza_recognizer import StanzaRecognizer
from .uk_nhs_recognizer import NhsRecognizer
from .url_recognizer import UrlRecognizer
from .us_bank_recognizer import UsBankRecognizer
from .us_driver_license_recognizer import UsLicenseRecognizer
from .us_itin_recognizer import UsItinRecognizer
from .us_passport_recognizer import UsPassportRecognizer
from .us_ssn_recognizer import UsSsnRecognizer
from .au_abn_recognizer import AuAbnRecognizer
from .au_acn_recognizer import AuAcnRecognizer
from .au_tfn_recognizer import AuTfnRecognizer
# from .au_medicare_recognizer import AuMedicareRecognizer
# from .in_pan_recognizer import InPanRecognizer
from .pl_pesel_recognizer import PlPeselRecognizer
from .azure_ai_language import AzureAILanguageRecognizer
from .in_aadhaar_recognizer import InAadhaarRecognizer
from .in_vehicle_registration_recognizer import InVehicleRegistrationRecognizer
from .medical_number_recognizer import MedicalNumberRecognizer
from .medical_record_number_recognizer import MedicalRecordNumberRecognizer
from .date_of_birth_recognizer import DateOfBirthRecognizer
from .encounter_number_recognizer import EncounterNumberRecognizer
from .word_list_recognizer import WordlistRecognizer
from .health_insurance_claim_number_recognizer import HICNRecognizer
from .australia_bank_account_numbers_recognizer import AustraliaBankAccountRecognizer
from .bank_state_branch_code_recognizer import BSBCodeRecognizer
# from .australiabusiness_and_companynumbers_recognizer import AustraliaBusinessCompanyNumberRecognizer
from .australia_medicare_card_numbers_recognizer import AustraliaMedicareCardRecognizer
from .au_bic_swift_numbers_reconizer import AustraliaBICRecognizer
from .canada_bic_swift_numbers_reconizer import CanadaBICRecognizer
from .canada_driving_license_recognizer import CanadaDriversLicenceRecognizer
from .canada_social_insurance_numbers_recognizer import CanadaSINRecognizer
from .canada_PIPEDA_recognizer import CanadaPIPEDARecognizer
from .france_national_identification_numbers_recognizer import FranceNationalIdRecognizer
from .france_driving_licence_numbers_recognizer import FranceDriversLicenceRecognizer
from .hipaa_and_hitech_drug_recognizer import DrugRecognizer
from .hipaa_and_hitech_ingredients_recognizer import IngredientRecognizer
from .hipaa_and_hitech_ICD9_recognizer import ICD9Recognizer
from .hipaa_and_hitech_ICD10_recognizer import ICD10Recognizer
from .us_pin_recognizer import PatientIDRecognizer
from .hipaa_and_hitech_low_threshold_recognizer import LowThresholdHIPAARecognizer
from .business_terminology_recognizer import BusinessTerminologyRecognizer
from .us_formatted_ssn import US_Formatted_SSN_Recognizer
from .us_privacy_et_SSNTIN_recognizer import SSNAndTINRecognizer
# from .hipaa_and_hitech_high import UnifiedRecognizer
from .us_privacy_formatted_and_unformatted_ssn import SSN_Formatted_Unformatted_Recognizer
from .germany_driving_licence_numbers_recognizer import GermanDriversLicenseRecognizer
from .germany_national_identification_number_recognizer import GermanIDCardRecognizer
from .germany_passport_numbers_recognizer import GermanPassportRecognizer
from .german_iban_number_recognizer import GermanIBANRecognizer
from .german_vat_number_recognizer import GermanVATRecognizer
from .us_et_ferpa import FerpaRecognizer
from .all_credit_card_number_recognizer import AllCreditCardNumberRecognizer
from .cc_numbers_Issuer_recognizer import CreditCardIssuerRecognizer
from .cc_track_data_attachments_recognizer import CCTrackDataRecognizer
from .us_driving_license_number_recognizer import USDriversLicenseRecognizer
# from .italy_BIC_swift_number_recognizer import ItalyBICSwiftRecognizer
from .us_PCI_DSS_recognizer import PCI_DSS_CreditCardAndTrackDataRecognizer
from .us_AZSB1338_recognizer import US_AZSB1338Recognizer
from .ca_CASB1386_recognizer import CASB1386Recognizer
from .us_COHB1119_recognizer import COHB1119Recognizer
from .us_CTSB650_recognizer import ConnecticutDLPRecognizer
from .us_DCCB16810_recognizer import ColumbiaDLPRecognizer
from .us_FLHB481_recognizer import FLHB481Recognizer
from .us_IDSB1374_recognizer import IdahoSB1374Recognizer
from .us_LASB205_recognizer import LouisianaRecognizer
from .us_MASS201_recognizer import MassachusettsDataRecognizer
from .us_HF2121_recognizer import MinnesotaRecognizer
from .us_NVSB347_recognizer import NevadaRecognizer
from .us_NJA4001_recognizer import NewJerseyDLPRecognizer
from .us_NHHB1660_recognizer import NewHampshirePolicyRecognizer
from .us_NYAB4254_recognizer import NewYorkDataRecognizer
from .us_OHHB104_recognizer import OhioDataRecognizer
from .us_OKHB2357_recognizer import OklahomaRecognizer
from .us_PASB712_recognizer import PennsylvaniaRecognizer
from .us_TXSB122_recognizer import TexasPolicyRecognizer
from .us_UTSB69_recognizer import UtahPolicyRecognizer
from .us_WASB6043_recognizer import WashingtonStateRecognizer
from .uk_driving_licence_numbers_recognizer import UKDriversLicenseRecognizer
from .uk_et_UKNINO_recognizer import UKNINORecognizer
from .uk_passport_recognizer import UKPassportRecognizer
from .uk_tax_identification_number_recognizer import UKTaxpayerRecognizer
from .italy_driving_licence_numbers_recognizer import ItalyDriversLicenseRecognizer
from .Italy_fiscal_code_recognizer import ItalyFiscalCodeRecognizer
from .italy_vat_numbers_recognizer import ItalyVATRecognizer
from .netherlands_passport_numbers_recognizer import NetherlandsPassportRecognizer
from .netherland_vat_numbers_recognizer import NetherlandsVATRecognizer
from .spain_iban_numbers_recognizer import SpainIBANRecognizer
from .spain_national_identification_numbers_recognizer import SpainDNIRecognizer
from .spain_passport_number_recognizer import SpainPassportRecognizer
from .spain_ssn_recognizer import SpainSSNRecognizer
from .spain_vat_numbers_recognizer import SpainVATRecognizer
from .eu_bic_numbers_recognizer import EUBICSwiftRecognizer
from .italy_iban_numbers_recognizer import ItalyIBANRecognizer
from .netherlands_iban_number_recognizer import NetherlandsIBANRecognizer
from .netherlands_driving_licence_numbers_recognizer import NetherlandsDriversLicenseRecognizer
from .netherlands_national_identification_numbers_recognizer import NetherlandsNationalIDRecognizer
from .newzealand_Inland_revenue_department_number_recognizer import NewZealandInlandRevenueDepartmentNumberRecognizer
from .newzealand_ministry_of_health_numbers_recognizer import NewZealandHealthNumberRecognizer
from .france_bic_swift_numbers_recognizer import FranceBICSwiftRecognizer
from .germany_bic_swift_numbers_recognizer import GermanyBICSwiftRecognizer
from .netherland_bic_swift_numbers_recognizer import NetherlandsBICSwiftRecognizer
from .spain_bic_swift_numbers_recognizer import SpainBICSwiftRecognizer
from .sweden_national_Identification_numbers_recognizer import SwedenNationalIDRecognizer
from .france_iban_numbers_recognizer import FranceIBANRecognizer
from .france_vat_numbers_recognizer import FranceVATRecognizer
from .us_custom_ssn_recognizer import USCustomSSNRecognizer
from .eu_credit_card_number_recognizer import EUDebitCardRecognizer
from .eu_iban_numbers_recognizer import EU_IBANRecognizer
from .eu_vat_number_recognizer import EUVATRecognizer
from .sweden_iban_numbers_recognizer import SwedenIBANRecognizer
from .sweden_swift_bic_number_recognizer import SwedenBICSwiftRecognizer
from .sweden_passport_numbers_recognizer import SwedenPassportRecognizer
from .sweden_vat_number_recognizer import SwedenVATRecognizer
from .custom_visa_credit_recognizer import VisaCreditCardRecognizer
from .credit_card_number_45_or_67_recognizer import CreditCardRecognizer
from .credit_debit_card_numbers_recognizer import DatotelCreditDebitCardRecognizer
from .bsfinvest_custom_account_numbers_recognizer import BFSInvestCustomAccountRecognizer
from .custom_BGFininc_wordlist_body_recognizer import BGFinincCustomWordlistRecognizer
from .hipaa_hitech_medium_threshold_recognizer import HIPAAHITECHMEDIUMRecognizer
from .dob_text_recognizer import DOBRecognizer
from .npi_recognizer import NPIRecognizer
from .hipaareg_recognizer import HipaaRegRecognizer
from .us_CORPFIN_recognizer import USCorporateFinancialRecognizer

NLP_RECOGNIZERS = {
    "spacy": SpacyRecognizer,
    "stanza": StanzaRecognizer,
    "transformers": TransformersRecognizer,
}

__all__ = [
    "AbaRoutingRecognizer",
    "CreditCardRecognizer",
    "CryptoRecognizer",
    "DateRecognizer",
    "EmailRecognizer",
    "IbanRecognizer",
    "IpRecognizer",
    "NhsRecognizer",
    "MedicalLicenseRecognizer",
    "PhoneRecognizer",
    "SgFinRecognizer",
    "UrlRecognizer",
    "UsBankRecognizer",
    "UsItinRecognizer",
    "UsLicenseRecognizer",
    "UsPassportRecognizer",
    "UsSsnRecognizer",
    "EsNifRecognizer",
    "SpacyRecognizer",
    "StanzaRecognizer",
    "NLP_RECOGNIZERS",
    "AuAbnRecognizer",
    "AuAcnRecognizer",
    "AuTfnRecognizer",
    #"AuMedicareRecognizer",
    "TransformersRecognizer",
    "ItDriverLicenseRecognizer",
    "ItFiscalCodeRecognizer",
    "ItVatCodeRecognizer",
    "ItIdentityCardRecognizer",
    "ItPassportRecognizer",
    # "InPanRecognizer",
    "PlPeselRecognizer",
    "AzureAILanguageRecognizer",
    "InAadhaarRecognizer",
    "InVehicleRegistrationRecognizer",
    "MedicalNumberRecognizer",
    "MedicalRecordNumberRecognizer",
    "DateOfBirthRecognizer",
    "EncounterNumberRecognizer",
    "WordlistRecognizer",
    "HICNRecognizer",
    "AustraliaBankAccountRecognizer",
    "BSBCodeRecognizer",
    # "AustraliaBusinessCompanyNumberRecognizer",
    "AustraliaMedicareCardRecognizer",
    "AustraliaBICRecognizer",
    "CanadaBICRecognizer",
    "CanadaDriversLicenceRecognizer",
    "CanadaSINRecognizer",
    "FranceNationalIdRecognizer",
    "FranceDriversLicenceRecognizer",
    "DrugRecognizer",
    "IngredientRecognizer",
    "ICD9Recognizer",
    "ICD10Recognizer",
    "PatientIDRecognizer",
    "LowThresholdHIPAARecognizer",
    "BusinessTerminologyRecognizer",
    "US_Formatted_SSN_Recognizer",
    # "UnifiedRecognizer",
    "SSNAndTINRecognizer",
    "SSN_Formatted_Unformatted_Recognizer",
    "GermanDriversLicenseRecognizer",
    "GermanIDCardRecognizer",
    "GermanPassportRecognizer",
    "GermanIBANRecognizer",
    "GermanVATRecognizer",
    "FerpaRecognizer",
    "AllCreditCardNumberRecognizer",
    "CreditCardIssuerRecognizer",
    "CCTrackDataRecognizer",
    "USDriversLicenseRecognizer",
    # "ItalyBICSwiftRecognizer",
    "PCI_DSS_CreditCardAndTrackDataRecognizer",
    "US_AZSB1338Recognizer",
    "CASB1386Recognizer",
    "COHB1119Recognizer",
    "ConnecticutDLPRecognizer",
    "ColumbiaDLPRecognizer",
    "FLHB481Recognizer",
    "IdahoSB1374Recognizer",
    "LouisianaRecognizer",
    "MassachusettsDataRecognizer",
    "MinnesotaRecognizer",
    "NevadaRecognizer",
    "NewJerseyDLPRecognizer",
    "NewHampshirePolicyRecognizer",
    "NewYorkDataRecognizer",
    "OhioDataRecognizer",
    "OklahomaRecognizer",
    "PennsylvaniaRecognizer",
    "TexasPolicyRecognizer",
    "UtahPolicyRecognizer",
    "WashingtonStateRecognizer",
    "UKDriversLicenseRecognizer",
    "UKNINORecognizer",
    "UKPassportRecognizer",
    "UKTaxpayerRecognizer",
    "ItalyDriversLicenseRecognizer",
    "ItalyFiscalCodeRecognizer",
    "ItalyVATRecognizer",
    "NetherlandsPassportRecognizer",
    "NetherlandsVATRecognizer",
    "SpainIBANRecognizer",
    "SpainDNIRecognizer",
    "SpainPassportRecognizer",
    "SpainSSNRecognizer",
    "SpainVATRecognizer",
    "EUBICSwiftRecognizer",
    "ItalyIBANRecognizer",
    "NetherlandsIBANRecognizer",
    "NetherlandsDriversLicenseRecognizer",
    "NetherlandsNationalIDRecognizer",
    "NewZealandInlandRevenueDepartmentNumberRecognizer",
    "NewZealandHealthNumberRecognizer",
    "FranceBICSwiftRecognizer",
    "GermanyBICSwiftRecognizer",
    "NetherlandsBICSwiftRecognizer",
    "SpainBICSwiftRecognizer",
    "SwedenNationalIDRecognizer",
    "FranceIBANRecognizer",
    "FranceVATRecognizer",
    "USCustomSSNRecognizer",
    "EUDebitCardRecognizer",
    "EU_IBANRecognizer",
    "EUVATRecognizer",
    "SwedenIBANRecognizer",
    "SwedenBICSwiftRecognizer",
    "SwedenPassportRecognizer",
    "SwedenVATRecognizer",
    "CanadaPIPEDARecognizer",
    "VisaCreditCardRecognizer",
    "CreditCardRecognizer",
    "DatotelCreditDebitCardRecognizer",
    "BFSInvestCustomAccountRecognizer",
    "BGFinincCustomWordlistRecognizer",
    "HIPAAHITECHMEDIUMRecognizer",
    "DOBRecognizer",
    "NPIRecognizer",
    "HipaaRegRecognizer",
    "USCorporateFinancialRecognizer",
    
]
