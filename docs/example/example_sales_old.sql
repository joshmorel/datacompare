SELECT salesdate || ' - ' || product AS date_product, * FROM sales_old
WHERE salesdate BETWEEN  ? AND ?
