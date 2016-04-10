SELECT salesdate || ' - ' || product AS date_product, * FROM sales_new
WHERE salesdate >= '2015-04-01'
AND salesdate <= '2015-04-02'