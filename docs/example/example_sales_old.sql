SELECT salesdate || ' - ' || product AS date_product, sales_quantity, sales_amount FROM sales_old
WHERE salesdate BETWEEN  ? AND ?
