CREATE OR REPLACE PROCEDURE update_itemprice() AS $$
DECLARE
    cur_timestamp timestamp := NOW();
BEGIN
    INSERT INTO itemprice
    SELECT DISTINCT ON (pricing_line_id, pricing_tier_id)
        pricing_line_id, pricing_tier_id, price, cur_timestamp
    FROM (SELECT p.pricing_line_id, pt.pricing_tier_id, ip.price, COUNT(*) AS occurrence
         FROM product p
         INNER JOIN "ItemPricingOnSKUinPOS" ip ON p.product_id = ip.product_id
         INNER JOIN postier pt ON ip.pos_id = pt.pos_id
         GROUP BY p.pricing_line_id, pt.pricing_tier_id, ip.price) ss
    ORDER BY pricing_line_id, pricing_tier_id, occurrence DESC, price DESC;
END;
$$
LANGUAGE plpgsql;
