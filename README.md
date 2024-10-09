# **Top 10 Largest Banks by Market Capitalization**

## **Project Overview**

Welcome to the Top 10 Largest Banks project! In this project, I've taken on the challenge of building an automated system to track the world’s largest banks based on their market capitalization in billion USD. The twist? I also transform this data into GBP, EUR, and INR using exchange rates provided in a CSV file.

## **What It Does**

- **Automation at Its Best:** This system is set up to run every financial quarter, making it easy to keep tabs on the latest rankings.
- **Web Scraping:** I’ve used web scraping techniques to pull data directly from a webpage, focusing on the 'By market capitalization' section.
- **Data Transformation:** The project includes a transformation step where I convert the market capitalization figures into multiple currencies, rounding them to two decimal places.
- **CSV and Database:** Once transformed, the data is saved in a local CSV file and also loaded into an SQL database for easy querying.
- **Progress Logging:** To keep track of everything, I’ve implemented a logging system that records progress at various stages in a file.

## **Tasks Completed**

1. **Logging Progress:** Set up a `log_progress()` function to keep tabs on code execution in `code_log.txt`.
2. **Data Extraction:** Created an `extract()` function to scrape the necessary tabular data and save it into a Pandas DataFrame.
3. **Data Transformation:** Developed a `transform()` function to add currency conversion columns to the DataFrame.
4. **Loading to CSV:** Wrote a `load_to_csv()` function to save the transformed data as a CSV file.
5. **Loading to Database:** Built a `load_to_db()` function to upload the DataFrame to an SQL database.
6. **Database Queries:** Created queries to extract insights from the database table.
7. **Progress Check:** Finally, I made sure all logging entries were recorded by checking the contents of `code_log.txt`.

## **Conclusion**

This project is a great example of using web scraping, data manipulation with Pandas, and database management with SQLite3. It’s been a fun journey creating a system that not only gathers and transforms data but also makes it easy to generate quarterly reports on the banking sector.
