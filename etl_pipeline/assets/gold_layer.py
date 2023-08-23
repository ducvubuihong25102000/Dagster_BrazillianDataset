import pandas as pd
from dagster import Output, Definitions, AssetIn, multi_asset, AssetOut, SourceAsset, AssetKey

#___ Table 1

@multi_asset(
    ins={
        "dim_products": AssetIn(
            key_prefix=["silver", "ecom"],
        ),
        "fact_sales": AssetIn(
            key_prefix=["silver", "ecom"],
        ),
    },
    outs={
        "sales_values_by_category": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["gold", "ecom"],
            metadata={
                "primary_keys": [
                    "monthly",
                    "category",
                ],
                "columns": [
                    "monthly",
                    "category",
                    "total_sales",
                    "total_bills",
                    "values_per_bill",
                ],
            },
        )
    },
    group_name="gold"
)
def sales_values_by_category(dim_products, fact_sales) -> Output[pd.DataFrame]:
    df_daily_sales_products = fact_sales[fact_sales["order_status"] == 'delivered'].groupby(by=
        [pd.to_datetime(fact_sales["order_purchase_timestamp"]),"product_id"], as_index = False).agg({
                "order_id" : "count",
                "payment_value" : "sum",
                "order_purchase_timestamp" : 'first',
                "customer_id" : 'first',
                "order_status" : 'first',
                "product_id": 'first'
            }).rename(
                columns = {
                    "order_purchase_timestamp" : "daily",
                    "product_id" : "product_id",
                    "order_id" : 'bills',
                    "payment_value" : 'sales',
                    "customer_id" : "customer_id",
                    "order_status" : "order_status"
                },
            )
            
    df_daily_sales_categories = pd.merge(df_daily_sales_products, dim_products, on="product_id")
    df_daily_sales_categories = df_daily_sales_categories.assign(
        monthly = pd.to_datetime(df_daily_sales_categories["daily"]).dt.strftime('%Y-%m'),
        category = df_daily_sales_categories["product_category_name_english"], 
        values_per_bill = df_daily_sales_categories["sales"] / df_daily_sales_categories["bills"])
    
    
    df_daily_sales_categories = df_daily_sales_categories[["daily", "monthly", "category", "sales", "bills", "values_per_bill"]]
    
    
    df_gold_layer_result = df_daily_sales_categories.groupby(
            by= ["monthly", "category"], 
            as_index = True).agg(
                {
                    "sales" : "sum",
                    "bills" : "sum",
                    "monthly" : "first",
                    "category" : "first",
                    "values_per_bill" : "first"
                }
            )
    res = df_gold_layer_result[['monthly', 'category', 'sales', 'bills', 'values_per_bill']]
    # print(res)
    # print('-------------------------------------------------------')
    # print(df_gold_layer_result.columns.values.tolist())

    
    return Output(
        res,
        metadata={
            "schema": "public",
            "table": "sales_values_by_category",
            "records counts": len(res),
        },
    )

