# DataCompare

## Background
We work in Data Warehousing with SSIS for ETL and the options for easy-to-build and interact with data-comparison for testing and exploration purposes are limited.

We don't have Informatica or anything super fancy and our stand-by of Excel copy/paste/vlookup is not sufficient and too manual. So I'm creating something with Python, ideally 
used in spyder, for interactive yet largely automated comparison of data.

## Features
- Compare data from SQL (via pyodbc), txt or DataFrame
- Show rows in one set not in the other and vice versa
- Show values different in every column for matched rows, with difference made clearly available

This is very much small scale, organizational use. However, if anyone stumbles upon finds this useful please fork it but it's not in an installable package at this point.