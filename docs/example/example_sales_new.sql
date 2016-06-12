SELECT salesdate || ' - ' || product AS date_product, sales_quantity, sales_amount FROM sales_new
WHERE salesdate BETWEEN  ? AND ?
