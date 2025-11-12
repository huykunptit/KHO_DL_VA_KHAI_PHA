SELECT
  AVG(age) AS avg_age,
  MIN(age) AS min_age,
  MAX(age) AS max_age,
  AVG(balance) AS avg_balance,
  MIN(balance) AS min_balance,
  MAX(balance) AS max_balance,
  AVG(campaign) AS avg_campaign,
  MIN(campaign) AS min_campaign,
  MAX(campaign) AS max_campaign
FROM bank_customers;

SELECT housing AS has_housing_loan, COUNT(*) AS cnt
FROM bank_customers
GROUP BY housing;

SELECT loan AS has_personal_loan, COUNT(*) AS cnt
FROM bank_customers
GROUP BY loan;

SELECT
  FLOOR(age/10)*10 AS age_group_start,
  COUNT(*) AS cnt,
  AVG(balance) AS avg_balance
FROM bank_customers
GROUP BY age_group_start
ORDER BY age_group_start;

SELECT housing, loan, COUNT(*) AS cnt, AVG(balance) AS avg_balance
FROM bank_customers
GROUP BY housing, loan;
