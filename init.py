# Databricks notebook source
# MAGIC  %md
# MAGIC #### AI for Data Engineering

# COMMAND ----------

def first_n_primes(n):
    primes = []
    num = 2
    while len(primes) < n:
        for p in primes:
            if num % p == 0:
                break
        else:
            primes.append(num)
        num += 1
    return primes

# Example usage
n = 10
example_primes = first_n_primes(n)
display(example_primes)

# COMMAND ----------

from typing import List

def first_n_primes(n: int) -> List[int]:
    """
    Generate a list of the first n prime numbers.

    Parameters:
    n (int): The number of prime numbers to generate.

    Returns:
    List[int]: A list containing the first n prime numbers.
    """
    primes = []
    num = 2
    while len(primes) < n:
        for p in primes:
            if num % p == 0:
                break
        else:
            primes.append(num)
        num += 1
    return primes

# Example usage
n = 10
example_primes = first_n_primes(n)
display(example_primes)

# COMMAND ----------

from typing import List

def first_n_primes(n: int) -> List[int]:
    """
    Generate a list of the first n prime numbers.

    Parameters:
    n (int): The number of prime numbers to generate.

    Returns:
    List[int]: A list containing the first n prime numbers.
    """
    primes = []
    num = 2
    while len(primes) < n:
        for p in primes:
            if num % p == 0:
                break
        else:
            primes.append(num)
        num += 1
    return primes

# Unit tests
def test_first_n_primes():
    assert first_n_primes(0) == [], "Test failed for n=0"
    assert first_n_primes(1) == [2], "Test failed for n=1"
    assert first_n_primes(5) == [2, 3, 5, 7, 11], "Test failed for n=5"
    assert first_n_primes(10) == [2, 3, 5, 7, 11, 13, 17, 19, 23, 29], "Test failed for n=10"
    print("All tests passed.")

# Run tests
test_first_n_primes()

# COMMAND ----------

# Create table if it doesn't exist
spark.sql("""
CREATE TABLE IF NOT EXISTS pavan_naidu.demo.people (
    id INT,
    name STRING,
    dob DATE,
    gender STRING,
    city STRING
)
""")

# Truncate the table
spark.sql("TRUNCATE TABLE pavan_naidu.demo.people")

# Insert sample records
spark.sql("""
INSERT INTO pavan_naidu.demo.people (id, name, dob, gender, city) VALUES
(1, 'John Doe', '1990-01-01', 'Male', 'New York'),
(2, 'Jane Smith', '1985-05-15', 'Female', 'Los Angeles'),
(3, 'Alice Johnson', '1992-07-21', 'Female', 'Chicago'),
(4, 'Bob Brown', '1988-11-30', 'Male', 'Houston'),
(5, 'Charlie Davis', '1995-03-10', 'Male', 'Phoenix')
""")

# Display the table
people_df = spark.sql("SELECT * FROM pavan_naidu.demo.people")
display(people_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS pavan_naidu.demo.millennial
# MAGIC LIKE pavan_naidu.demo.people

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO pavan_naidu.demo.millennial AS target
# MAGIC USING (
# MAGIC     SELECT * 
# MAGIC     FROM pavan_naidu.demo.people 
# MAGIC     WHERE dob BETWEEN DATE '1990-01-01' AND DATE '1996-12-31'
# MAGIC ) AS source
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pavan_naidu.demo.millennial

# COMMAND ----------

# Create the table if it doesn't exist
spark.sql("""
CREATE TABLE IF NOT EXISTS pavan_naidu.demo.millennial_stats (
    gender STRING,
    count INT
)
""")

# Insert overwrite with aggregate data
spark.sql("""
INSERT OVERWRITE TABLE pavan_naidu.demo.millennial_stats
SELECT gender, COUNT(*) as count
FROM pavan_naidu.demo.millennial
GROUP BY gender
""")

# Display the output
result_df = spark.sql("SELECT * FROM pavan_naidu.demo.millennial_stats")
display(result_df)
