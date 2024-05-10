
from pyspark.sql.functions import *

def filter_closed_orders(orders_df):
    return orders_df.filter("order_status=='CLOSED'")

def join_orders_customers(orders_df, customers_df):
    return orders_df.join(customers_df, orders_df.customer_id==customers_df.customer_id,"inner")

def count_orders_state(joined_df):
    return joined_df.groupBy('state').count()

def filter_order_generic(order_df,status):
    return order_df.filter("order_status=='{}'".format(status))