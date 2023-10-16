USE ML.PUBLIC;


create or replace view DISPUTE_PVC_MODEL_FEATURES_VIEW_ZZZ comment='Dispute PVC model features as per requirements at: https://github.com/1debit/ml_common/blob/35f6d68396e0e9a8feaf47dd5ceeaaa7ace2b97d/common/data/chimeml_featurelib.py'
 as
 WITH recent_disputes AS (
  SELECT
     id as dispute_claim_id,
     user_id,
     created_at as claim_created_at,
     date_transaction_cancelled,
     duplicate_charge_date,
     amount_supposed_to_have_been_charged,
     atm_amount_received,
     atm_amount_requested,
     last_authorized_tran_date_before_unauthorized_tran,
     dispute_type = 'ach_debit' as dispute_type_ach_debit,
     card_holder_possesses_card_after_unauthorized_transaction = true as card_holder_possesses_card_after_unauthorized_transaction_true,
     card_holder_possesses_card_after_unauthorized_transaction = false as card_holder_possesses_card_after_unauthorized_transaction_false
   FROM analytics.looker.user_dispute_claims
   WHERE created_at::date>='2019-12-01' AND  created_at::date<'2020-03-01'
   ORDER BY dispute_claim_id, claim_created_at DESC
 ),
 
 claim_transactions AS (
 SELECT
     user_dispute_claim_id,
     id as udct_id,
     created_at,
     settled_at,
     amount,
     atm_withdraw,
     transaction_type = 'merchant_debit' as transaction_type_merchant_debit,
     transaction_type = 'transfer' as transaction_type_transfer,
     currency_code = '156' as currency_code_156,
     currency_code = '484' as currency_code_484,
     network_code = 'S' as network_code_S,
     network_code = 'V' as network_code_V,
     issuer = 'Bancorp' as issuer_Bancorp,
     ROUND(CASE WHEN LEFT(transaction_id, 1) in (4,5,6,7,9)
        THEN substr(transaction_id, 2)
        ELSE transaction_id
        END,0)::varchar as auth_id,
     transaction_id as udct_trx_id
 FROM analytics.looker.user_dispute_claim_transactions
 ),
 
 user_attributes AS
 (SELECT
     id as user_id,
     first_fund_type = 'PMC2' as first_fund_type_PMC2,
     first_fund_type = 'PMDB' as first_fund_type_PMDB,
     first_fund_type = 'PMPB' as first_fund_type_PMPB,
     first_fund_type = 'PMTU' as first_fund_type_PMTU,
     source = 'referral' as source_referral,
     first_dd_quality_quartile,
     first_dd_at,
     first_deposit_at,
     first_cash_load,
     first_gov_benefit_dd,
     first_child_support_dd,
     enrollment
     FROM mysql_db.chime_prod.users a
     LEFT JOIN analytics.looker.users_by_attributes b
     ON a.id=b.user_id
 ),
 
 balances AS (
 SELECT
     a.user_id,
     claim_created_at,
     MAX(CASE WHEN balance_on_date<rd.claim_created_at THEN available_balance END) as last_balance,
     AVG(available_balance) AS avg_balance_l28d,
     MIN(available_balance) AS min_balance_l28d,
     (MAX(available_balance)-MIN(available_balance))/(1.0000000001+AVG(available_balance)) AS range_over_mean_balance_l28d,
     COUNT(available_balance) AS count_balance_l28d
 FROM analytics.looker.galileo_daily_balances a
 INNER JOIN recent_disputes rd
 ON a.user_id = rd.user_id
 WHERE balance_on_date between DATEADD(day, -28, rd.claim_created_at) and DATEADD(hour, -1, rd.claim_created_at)
 GROUP BY 1,2),
 
 transactions AS (
 SELECT
     gpt.user_id,
     gpt.transaction_timestamp,
     id as transaction_id,
     dispute_claim_id,
     post_date,
     amount,
     merchant_category_code = '0' as merchant_category_code_0,
     merchant_category_code = '3132' as merchant_category_code_3132,
     merchant_category_code = '3260' as merchant_category_code_3260,
     merchant_category_code = '4121' as merchant_category_code_4121,
     merchant_category_code = '4814' as merchant_category_code_4814,
     merchant_category_code = '4829' as merchant_category_code_4829,
     merchant_category_code = '5310' as merchant_category_code_5310,
     merchant_category_code = '5399' as merchant_category_code_5399,
     merchant_category_code = '5541' as merchant_category_code_5541,
     merchant_category_code = '5542' as merchant_category_code_5542,
     merchant_category_code = '5812' as merchant_category_code_5812,
     merchant_category_code = '5813' as merchant_category_code_5813,
     merchant_category_code = '5814' as merchant_category_code_5814,
     merchant_category_code = '5921' as merchant_category_code_5921,
     merchant_category_code = '5942' as merchant_category_code_5942,
     merchant_category_code = '5964' as merchant_category_code_5964,
     merchant_category_code = '5967' as merchant_category_code_5967,
     merchant_category_code = '5999' as merchant_category_code_5999,
     merchant_category_code = '6011' as merchant_category_code_6011,
     merchant_category_code = '6012' as merchant_category_code_6012,
     merchant_category_code = '6051' as merchant_category_code_6051,
     merchant_category_code = '6300' as merchant_category_code_6300,
     merchant_category_code = '7011' as merchant_category_code_7011,
     merchant_category_code = '7230' as merchant_category_code_7230,
     merchant_category_code = '7394' as merchant_category_code_7394,
     merchant_category_code = '7399' as merchant_category_code_7399,
     merchant_category_code = '8931' as merchant_category_code_8931,
     merchant_number = '160146000762203' as is_prime_trx,
     merchant_number IN ('844164925886','248748000103177','18777015') as is_google_pay_trx,
     (merchant_number IN ('445265732997','000445265732997')) OR (merchant_number = '445120538993' AND merchant_id = 1210)
         as is_doordash_trx,
     merchant_id=1303 as is_amzn_trx,
     merchant_id=1314 as is_hulu_trx,
     gpt.merchant_name like '%PAYPAL%' as is_paypal_trx,
     transaction_code = 'ADPF' as transaction_code_ADPF,
     transaction_code = 'ADbc' as transaction_code_ADbc,
     transaction_code = 'ISA' as transaction_code_ISA,
     transaction_code = 'PLW' as transaction_code_PLW,
     transaction_code = 'VSA' as transaction_code_VSA,
     transaction_code = 'VSC' as transaction_code_VSC,
     transaction_code = 'VSM' as transaction_code_VSM,
     interchange_fee_amount,
     authorization_code
 FROM mysql_db.galileo.galileo_posted_transactions gpt
 INNER JOIN claim_transactions ct
 INNER JOIN recent_disputes rd
 WHERE gpt.user_id = rd.user_id AND gpt.authorization_code = ct.auth_id
   AND ct.user_dispute_claim_id = rd.dispute_claim_id
 ),
 
 claim_updates AS (
     select user_dispute_claim_id, date_final_resolution_letter_sent, user_dispute_claim_transaction_id as udct_id_cu_table
     from analytics.looker.user_dispute_claim_updates
     where user_dispute_claim_id in (select dispute_claim_id from recent_disputes)
 ),
 
 recent_transactions AS (
 
     WITH
     purchase_filter as (
     transaction_code IN (
     'ISA', 'ISC', 'ISJ', 'ISL', 'ISM', 'ISR', 'ISZ', 'VSA', 'VSC', 'VSJ', 'VSL', 'VSM', 'VSR', 'VSZ',
     'SDA', 'SDC', 'SDL', 'SDM', 'SDR', 'SDV', 'SDZ', 'PLM', 'PLA', 'PRA')
     ),
     card_based_filter as (
     transaction_code like 'VS%' or transaction_code like 'IS%' or transaction_code like 'MP%' or
     transaction_code like 'PL%' or transaction_code like 'SD%' or transaction_code like 'PR%'
     ),
     cash_load_filter as (
     transaction_code IN ('PMGT', 'PMGO', 'PMVL', 'ADgd', 'ADGO')
     ),
     atm_filter as (
     transaction_code IN ('VSW', 'MPW', 'MPM', 'MPR', 'PLW', 'PLR', 'PRW', 'SDW')
     ),
     visa_load_filter as (
     transaction_code IN ('PMVT', 'ADy')
     ),
 
     bill_pay_filter as (transaction_code = 'ADZ'),
 
     mcc_top5_filter as (
     merchant_category_code IN ('7273','5967','5968','4816','5734')),
 
     mcc_top5to10_filter as (
     merchant_category_code IN ('8999','7011','5818','5816','5691')),
 
     merch_num_top5_filter as (
     merchant_number IN ('174030072998','112137000108778','248748000103177','160146000762203','18777015')
     ),
 
     merch_catid_top5_filter as (
     merchant_category_id IN (28,5,21,6,27)
     )
 
 select
 rd.dispute_claim_id as dcid,
 claim_created_at,
 a.user_id,
 count(*) as num_trans_l28d,
 SUM(CASE WHEN transaction_amount>0 THEN transaction_amount ELSE 0 END) as tot_pos_trans_amt_l28d,
 SUM(CASE WHEN transaction_amount<0 THEN transaction_amount ELSE 0 END) as tot_neg_trans_amt_l28d,
 SUM(transaction_amount) AS tot_trans_amt_128d,
 MAX(transaction_amount) as max_trans_amt_l28d,
 MIN(transaction_amount) as min_trans_amt_l28d,
 MAX(transaction_timestamp) as last_trans_time,
 sum(interchange_fee_amount) as interchange_l28d,
 -- transaction code bases features
 sum(case when purchase_filter then 1 else 0 end) as purchase_count_l28d,
 sum(case when card_based_filter then 1 else 0 end) as card_based_transactions_count_l28d,
 sum(case when cash_load_filter then 1 else 0 end) as cash_load_count_l28d,
 sum(case when atm_filter then 1 else 0 end) as atm_count_l28d,
 sum(case when visa_load_filter then 1 else 0 end) as visa_load_count_l28d,
 SUM(case when RIGHT(transaction_code,1)='M' then 1 else 0 end) as num_tc_manual_entry,
 SUM(case when transaction_code='ADbc' then 1 else 0 end) AS num_tc_adbc,
 SUM(case when transaction_code IN ('ADTU','ADTS') then 1 else 0 end) as num_c2c,
 SUM(case when transaction_code='VSL' THEN 1 ELSE 0 END) as num_visa_preauth,
 -- merchant country code based features
 SUM(case when merchant_country_code NOT IN ('840') THEN 1 ELSE 0 END) as num_non_us,
 -- mcc based features
 SUM(case when mcc='7273' THEN 1 ELSE 0 END) as num_mcc_7273,
 SUM(case when mcc='5967' THEN 1 ELSE 0 END) as num_mcc_5967,
 SUM(case when mcc='5968' THEN 1 ELSE 0 END) as num_mcc_5968,
 SUM(case when mcc='4816' THEN 1 ELSE 0 END) as num_mcc_4816,
 SUM(case when mcc='5734' THEN 1 ELSE 0 END) as num_mcc_5734,
 SUM(case when mcc='8999' THEN 1 ELSE 0 END) as num_mcc_8999,
 SUM(case when mcc='7011' THEN 1 ELSE 0 END) as num_mcc_7011,
 SUM(case when mcc='5818' THEN 1 ELSE 0 END) as num_mcc_5818,
 SUM(case when mcc='5816' THEN 1 ELSE 0 END) as num_mcc_5816,
 SUM(case when mcc='5691' THEN 1 ELSE 0 END) as num_mcc_5691,
 -- merchant number
 SUM(case when merchant_number='174030072998' THEN 1 ELSE 0 END) as num_merchnum_mixmerch,
 SUM(case when merchant_number IN ('844164925886','248748000103177','18777015') THEN 1 ELSE 0 END) as num_merchnum_googlepay,
 SUM(case when merchant_number='112137000108778' THEN 1 ELSE 0 END) as num_merchnum_appleitunes,
 SUM(case when merchant_number='160146000762203' THEN 1 ELSE 0 END) as num_merchnum_amzndigital,
 SUM(case when merchant_number='445301505993' THEN 1 ELSE 0 END) as num_merchnum_cashapp,
 -- merch id
 SUM(case when merchant_id=1451 THEN 1 ELSE 0 END) as num_merchid_lyft,
 SUM(case when merchant_id=1303 THEN 1 ELSE 0 END) as num_merchid_amzn,
 SUM(case when merchant_id=1314 THEN 1 ELSE 0 END) as num_merchid_hulu,
 SUM(case when merchant_id=1468 THEN 1 ELSE 0 END) as num_merchid_boostmobile,
 
 --merch cat id
 SUM(case when merchant_category_id = 28 THEN 1 ELSE 0 END) as num_merchcat_28,
 SUM(case when merchant_category_id = 5 THEN 1 ELSE 0 END) as num_merchcat_5,
 SUM(case when merchant_category_id = 21 THEN 1 ELSE 0 END) as num_merchcat_21,
 SUM(case when merchant_category_id = 6 THEN 1 ELSE 0 END) as num_merchcat_6,
 
 -- network code
 SUM(case when network_code = 'V' THEN 1 ELSE 0 END) as num_network_visa
 
 from mysql_db.galileo.galileo_posted_transactions a
 LEFT JOIN mysql_db.chime_prod.categories c ON a.merchant_category_code = c.mcc
 INNER JOIN recent_disputes rd
 ON a.user_id = rd.user_id
 WHERE transaction_timestamp between DATEADD(day, -28, rd.claim_created_at) and DATEADD(hour, -1, rd.claim_created_at)
 GROUP BY 1,2,3),
 
 
 final_dataset AS (
 SELECT DISTINCT
  --recent_disputes
  rd.dispute_claim_id,
  rd.user_id,
  rd.claim_created_at,
  amount_supposed_to_have_been_charged,
  atm_amount_received,
  atm_amount_requested,
  last_authorized_tran_date_before_unauthorized_tran,
  date_transaction_cancelled,
  duplicate_charge_date,
  dispute_type_ach_debit,
  card_holder_possesses_card_after_unauthorized_transaction_true,
  card_holder_possesses_card_after_unauthorized_transaction_false,
  --claim_transactions
  ct.user_dispute_claim_id,
  udct_id,
  created_at,
  settled_at,
  atm_withdraw,
  transaction_type_merchant_debit,
  transaction_type_transfer,
  currency_code_156,
  currency_code_484,
  network_code_s,
  network_code_v,
  issuer_bancorp,
  udct_trx_id,
  auth_id,
  --user_attributes
  first_fund_type_PMC2,
  first_fund_type_PMDB,
  first_fund_type_PMPB,
  first_fund_type_PMTU,
  source_referral,
  first_dd_quality_quartile,
  first_dd_at,
  first_deposit_at,
  first_cash_load,
  first_gov_benefit_dd,
  first_child_support_dd,
  enrollment,
  --balances
  date_final_resolution_letter_sent,
  last_balance,
  avg_balance_l28d,
  min_balance_l28d,
  range_over_mean_balance_l28d,
  count_balance_l28d,
  -- transactions
  num_trans_l28d,
  tot_pos_trans_amt_l28d,
  tot_neg_trans_amt_l28d,
  tot_trans_amt_128d,
  max_trans_amt_l28d,
  min_trans_amt_l28d,
  last_trans_time,
  purchase_count_l28d,
  card_based_transactions_count_l28d,
  atm_count_l28d,
  visa_load_count_l28d,
  cash_load_count_l28d,
  interchange_l28d,
  num_tc_manual_entry,
  num_tc_adbc,
  num_c2c,
  num_visa_preauth,
  num_non_us,
  num_mcc_7273,
  num_mcc_5967,
  num_mcc_5968,
  num_mcc_4816,
  num_mcc_5734,
  num_mcc_8999,
  num_mcc_7011,
  num_mcc_5818,
  num_mcc_5816,
  num_mcc_5691,
  num_merchnum_mixmerch,
  num_merchnum_googlepay,
  num_merchnum_appleitunes,
  num_merchnum_amzndigital,
  num_merchnum_cashapp,
  num_merchid_lyft,
  num_merchid_amzn,
  num_merchid_hulu,
  num_merchid_boostmobile,
  num_merchcat_28,
  num_merchcat_5,
  num_merchcat_21,
  num_merchcat_6,
  num_network_visa
  FROM recent_disputes rd
  LEFT JOIN claim_transactions ct ON ct.user_dispute_claim_id=rd.dispute_claim_id
  LEFT JOIN balances b ON rd.user_id=b.user_id AND b.claim_created_at=rd.claim_created_at
  LEFT JOIN user_attributes ua ON rd.user_id=ua.user_id
  LEFT JOIN claim_updates cu on rd.dispute_claim_id=cu.user_dispute_claim_id AND udct_id=cu.udct_id_cu_table
  LEFT JOIN recent_transactions rt on rt.dcid=rd.dispute_claim_id
 ) select fd.*,
  transaction_id,
  transaction_timestamp,
  amount,
  post_date,
  merchant_category_code_0,
  merchant_category_code_3132,
  merchant_category_code_3260,
  merchant_category_code_4121,
  merchant_category_code_4814,
  merchant_category_code_4829,
  merchant_category_code_5310,
  merchant_category_code_5399,
  merchant_category_code_5541,
  merchant_category_code_5542,
  merchant_category_code_5812,
  merchant_category_code_5813,
  merchant_category_code_5814,
  merchant_category_code_5921,
  merchant_category_code_5942,
  merchant_category_code_5964,
  merchant_category_code_5967,
  merchant_category_code_5999,
  merchant_category_code_6011,
  merchant_category_code_6012,
  merchant_category_code_6051,
  merchant_category_code_6300,
  merchant_category_code_7011,
  merchant_category_code_7230,
  merchant_category_code_7394,
  merchant_category_code_7399,
  merchant_category_code_8931,
  is_prime_trx,
  is_google_pay_trx,
  is_doordash_trx,
  is_amzn_trx,
  is_hulu_trx,
  is_paypal_trx,
  transaction_code_ADPF,
  transaction_code_ADbc,
  transaction_code_ISA,
  transaction_code_PLW,
  transaction_code_VSA,
  transaction_code_VSC,
  transaction_code_VSM,
  interchange_fee_amount
 from final_dataset fd
 join transactions t
 WHERE fd.user_id = t.user_id AND t.authorization_code = fd.auth_id
 AND fd.user_dispute_claim_id = t.dispute_claim_id
 ORDER BY user_dispute_claim_id, t.transaction_id, udct_id DESC;
