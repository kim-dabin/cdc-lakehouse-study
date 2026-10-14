-- The same refund was corrected, not a second refund.
UPDATE refunds SET amount = 30000
WHERE refund_id = :'namespace' || '-r1';
