import sys
from lib import dataManipulation, dataReader, utils
from pyspark.sql.functions import *
from lib.logger import Log4j

if __name__=='__main__':
    
    if len(sys.argv)<2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env=sys.argv[1]

    print("Creating Spark Session...")

    spark=utils.get_spark_session(job_run_env)

    logger=Log4j(spark)

    logger.warn("Spark Session Created Successfully!")

    orders_df=dataReader.read_orders(spark=spark,env=job_run_env)
    orders_filtered=dataManipulation.filter_closed_orders(orders_df=orders_df)
    customers_df=dataReader.read_customers(spark=spark,env=job_run_env)
    
    joined_df=dataManipulation.join_orders_customers(orders_df=orders_filtered, customers_df=customers_df)

    aggregated_result=dataManipulation.count_orders_state(joined_df=joined_df)

    aggregated_result.show(truncate=False)

    # print(orders_df.count())
    # print(customers_df.count())
    # print(orders_filtered.count())

    logger.warn("End of main")



