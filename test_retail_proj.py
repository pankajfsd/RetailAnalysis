import pytest
from lib.dataReader import read_customers, read_orders
from lib.dataManipulation import filter_closed_orders, count_orders_state, filter_order_generic
from lib.configReader import get_app_config


@pytest.mark.action()
def test_read_customers_df(spark):
    customers_count=read_customers(spark, "LOCAL").count()

    assert customers_count==12435

@pytest.mark.action()
def test_read_orders_df(spark):
    orders_count=read_orders(spark,"LOCAL").count()

    assert orders_count==68884

@pytest.mark.transformation()
def test_filter_closed_orders(spark):
    orders_df=read_orders(spark,"LOCAL")
    filtererd_count=filter_closed_orders(orders_df=orders_df).count()

    assert filtererd_count==7556

@pytest.mark.skip("work in progress")
def test_read_app_config():
    config=get_app_config("LOCAL")

    assert config['orders.file.path']=='data/orders.csv'

@pytest.mark.transformation()
def test_count_orders_state(spark, expected_results):
    customers_df=read_customers(spark,"LOCAL")

    actual_results=count_orders_state(customers_df)

    # collect will convert the df into python list

    assert actual_results.collect()==expected_results.collect()

@pytest.mark.skip("Not generic")
def test_check_closed_count(spark):
    orders_df=read_orders(spark,'LOCAL')
    filtered_count=filter_order_generic(orders_df,'CLOSED').count()
    
    assert filtered_count==7556

# parameterized generic test cases

@pytest.mark.parametrize(
        "status, count",
        [
            ("CLOSED",7556),
            ("PENDING_PAYMENT",15030),
            ("COMPLETE",22900)
        ]
)
def test_check_count(spark, status, count):
    orders_df=read_orders(spark,'LOCAL')
    filtered_count=filter_order_generic(orders_df,status).count()
    
    assert filtered_count==count




