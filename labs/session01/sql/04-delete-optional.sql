-- Session 01 core: observe op=d in Kafka and the retained Bronze history.
DELETE FROM refunds WHERE refund_id = :'namespace' || '-r1';
