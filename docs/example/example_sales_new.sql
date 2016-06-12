SELECT salesdate || ' - ' || product AS date_product, * FROM sales_new
WHERE salesdate BETWEEN  ? AND ?
