-- dim_payment_method.sql
-- Payment method dimension (static seed)
-- Since generator V2 doesn't store payment method as a field,
-- we derive from journal_id or create a static reference

{{ config(materialized='table') }}

SELECT * FROM (
    VALUES
        (1, 'cod',          'Cash on Delivery',     'cash'),
        (2, 'bank_transfer','Bank Transfer',         'bank'),
        (3, 'momo',         'MoMo Wallet',           'e_wallet'),
        (4, 'zalopay',      'ZaloPay',               'e_wallet'),
        (5, 'vnpay',        'VNPay',                 'e_wallet'),
        (6, 'credit_card',  'Credit Card',           'card'),
        (7, 'shopee_pay',   'ShopeePay',             'e_wallet'),
        (8, 'installment',  'Installment',           'installment')
) AS t(method_id, method_key, method_name, method_group)
