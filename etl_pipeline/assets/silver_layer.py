import pandas as pd
from dagster import asset, Output, AssetIn, multi_asset, AssetOut, SourceAsset, AssetKey


#__ Table dim_products
@multi_asset(
    ins={
        "bronze_olist_products_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
        "bronze_product_category_name_translation": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
    },
    outs={
        "dim_products": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["silver", "ecom"],
            metadata={
                "primary_keys": [
                    "product_id",
                ],
                "columns": [
                    "product_id",
                    "product_category_name_english",
                ],
            },
        )
    },
    compute_kind="MinIO",
    group_name="silver"
)


def dim_products(bronze_olist_products_dataset, bronze_product_category_name_translation) -> Output[pd.DataFrame]:
    # df_merge = pd.merge(bronze_olist_products_dataset,bronze_product_category_name_translation, on="product_category_name")
    # df_result = df_merge[["product_id", "product_category_name_english"]]

    df_result = pd.merge(left=bronze_olist_products_dataset[['product_category_name','product_id']],
                         right= bronze_product_category_name_translation[['product_category_name_english','product_category_name']], 
                         how='left',
                         on=['product_category_name','product_category_name']).drop(columns=['product_category_name'],
                                                                                    axis=1)
    return Output(
        df_result,
        metadata={
            "schema": "public",
            "table": "dim_products",
            "product data set": str(type(bronze_olist_products_dataset)),
            "records counts": len(bronze_olist_products_dataset),
            "first column": str(list(bronze_olist_products_dataset.columns.values)[0])
        },
    )

#__ Table fact_sales

@multi_asset(
    ins={
        "bronze_olist_order_payments_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
        "bronze_olist_order_items_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
        "bronze_olist_orders_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
    },
    outs={
        "fact_sales": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["silver", "ecom"],
            metadata={
                "primary_keys": [
                    "order_id",
                ],
                "columns": [
                    "order_id",
                    "customer_id",
                    "order_purchase_timestamp",
                    "product_id",
                    "payment_value",
                    "order_status",
                ],
            },
        )
    },
    compute_kind="MinIO",
    group_name="silver"
)



def fact_sales(bronze_olist_order_payments_dataset, bronze_olist_order_items_dataset, bronze_olist_orders_dataset) -> Output[pd.DataFrame]:
    df_merge = pd.merge(pd.merge(bronze_olist_order_payments_dataset, bronze_olist_order_items_dataset, on="order_id"), bronze_olist_orders_dataset, on="order_id")

    df_result = df_merge[["order_id", "customer_id", "order_purchase_timestamp", "product_id", "payment_value", "order_status"]]

    return Output(
        df_result,
        metadata={
            "schema": "public",
            "table": "fact_sales",
            "records counts": len(df_result),
        },
    )

